use std::collections::HashMap;
use std::env;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::{Value, json};
use tokio::net::TcpListener;

use crate::{
    BatchResult, Lqs, LqsError, QueueOptions, QueueType, QueueUpdate, ReceivedMessage,
    RedrivePolicy, SendRequest, SendResult,
};

#[path = "batch_http.rs"]
mod batch_http;

#[path = "attributes_http.rs"]
mod attributes_http;

#[path = "management_http.rs"]
mod management_http;

#[path = "security_http.rs"]
mod security_http;
pub use security_http::{AuthorizationHook, HttpAuthorizationRequest};

#[cfg(test)]
#[path = "polling_http_tests.rs"]
mod polling_http_tests;

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:9324";
const DEFAULT_DATABASE_PATH: &str = "lqs.sqlite";
// A 1 MiB decoded body can expand up to 6x in JSON, or 3x in Query encoding.
const MAX_REQUEST_BYTES: usize = 8 * 1_048_576;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub trust_principal_header: bool,
    pub bind_addr: SocketAddr,
    pub database_path: PathBuf,
    pub public_base_url: String,
}

impl ServerConfig {
    pub fn from_env() -> Result<Self, ServerConfigError> {
        let bind_addr: SocketAddr = env::var("LQS_BIND_ADDR")
            .unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_owned())
            .parse::<SocketAddr>()
            .map_err(|error| ServerConfigError::InvalidBindAddress(error.to_string()))?;
        let database_path = env::var_os("LQS_DATABASE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATABASE_PATH));
        let public_base_url = env::var("LQS_BASE_URL")
            .unwrap_or_else(|_| format!("http://{bind_addr}"))
            .trim_end_matches('/')
            .to_owned();
        if !(public_base_url.starts_with("http://") || public_base_url.starts_with("https://")) {
            return Err(ServerConfigError::InvalidBaseUrl(public_base_url));
        }
        Ok(Self {
            trust_principal_header: match env::var("LQS_TRUST_PRINCIPAL_HEADER").as_deref() {
                Ok("true") => true,
                Ok("false") | Err(env::VarError::NotPresent) => false,
                _ => return Err(ServerConfigError::InvalidAuthorizationMode),
            },
            bind_addr,
            database_path,
            public_base_url,
        })
    }
}

#[derive(Debug)]
pub enum ServerConfigError {
    InvalidAuthorizationMode,
    InvalidBindAddress(String),
    InvalidBaseUrl(String),
}

impl fmt::Display for ServerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAuthorizationMode => write!(
                formatter,
                "LQS_TRUST_PRINCIPAL_HEADER must be true or false"
            ),
            Self::InvalidBindAddress(message) => {
                write!(formatter, "invalid LQS_BIND_ADDR: {message}")
            }
            Self::InvalidBaseUrl(url) => write!(formatter, "invalid LQS_BASE_URL: {url}"),
        }
    }
}

impl std::error::Error for ServerConfigError {}

#[derive(Debug)]
pub enum ServerError {
    Database(LqsError),
    Io(std::io::Error),
}

impl fmt::Display for ServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => error.fmt(formatter),
            Self::Io(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ServerError {}

impl From<LqsError> for ServerError {
    fn from(error: LqsError) -> Self {
        Self::Database(error)
    }
}

impl From<std::io::Error> for ServerError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Clone)]
struct AppState {
    authorization_hook: Arc<AuthorizationHook>,
    access: Option<security_http::AccessContext>,
    lqs: Arc<Mutex<Lqs>>,
    public_base_url: Arc<str>,
    request_sequence: Arc<AtomicU64>,
}

impl AppState {
    fn next_request_id(&self) -> String {
        let sequence = self.request_sequence.fetch_add(1, Ordering::Relaxed);
        format!("lqs-{sequence:016x}")
    }
}

pub async fn serve(config: ServerConfig) -> Result<(), ServerError> {
    let lqs = Lqs::open(&config.database_path)?;
    let listener = TcpListener::bind(config.bind_addr).await?;
    axum::serve(
        listener,
        router_with_authorization(
            lqs,
            config.public_base_url,
            security_http::local_hook(config.trust_principal_header),
        ),
    )
    .await
    .map_err(ServerError::Io)
}

pub async fn serve_with_listener(
    listener: TcpListener,
    lqs: Lqs,
    public_base_url: impl Into<String>,
) -> Result<(), ServerError> {
    axum::serve(listener, router(lqs, public_base_url))
        .await
        .map_err(ServerError::Io)
}

pub fn router(lqs: Lqs, public_base_url: impl Into<String>) -> Router {
    router_with_authorization(lqs, public_base_url, security_http::local_hook(false))
}

pub fn router_with_authorization(
    lqs: Lqs,
    public_base_url: impl Into<String>,
    authorization_hook: Arc<AuthorizationHook>,
) -> Router {
    let state = AppState {
        authorization_hook,
        access: None,
        lqs: Arc::new(Mutex::new(lqs)),
        public_base_url: Arc::from(public_base_url.into().trim_end_matches('/').to_owned()),
        request_sequence: Arc::new(AtomicU64::new(1)),
    };
    Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .fallback(handle_request)
        .with_state(state)
}

async fn handle_request(State(mut state): State<AppState>, request: Request) -> Response {
    let request_id = state.next_request_id();
    if request.method() != Method::POST {
        return ApiError::new(
            StatusCode::METHOD_NOT_ALLOWED,
            "InvalidAction",
            "only POST requests are supported",
        )
        .into_response(Protocol::Query, &request_id);
    }

    let path = request.uri().path().to_owned();
    let uri = request.uri().clone();
    let headers = request.headers().clone();
    let body = match to_bytes(request.into_body(), MAX_REQUEST_BYTES).await {
        Ok(body) => body,
        Err(_) => {
            return ApiError::new(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                "request body is too large",
            )
            .into_response(protocol_from_headers(&headers), &request_id);
        }
    };
    let wire_request = match WireRequest::parse(&headers, body.clone()) {
        Ok(request) => request,
        Err(error) => {
            return error.into_response(protocol_from_headers(&headers), &request_id);
        }
    };
    let protocol = wire_request.protocol;
    match security_http::access_context(&state, &path, &wire_request, headers, uri, body) {
        Ok(access) => state.access = Some(access),
        Err(error) => return error.into_response(protocol, &request_id),
    }
    let result = if wire_request.action == "ReceiveMessage" {
        receive_message(&state, &path, &wire_request).await
    } else {
        dispatch(&state, &path, wire_request)
    };
    match result {
        Ok(success) => success.into_response(protocol, &request_id),
        Err(error) => error.into_response(protocol, &request_id),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Protocol {
    Json,
    Query,
}

fn protocol_from_headers(headers: &HeaderMap) -> Protocol {
    if headers.contains_key("x-amz-target") {
        Protocol::Json
    } else {
        Protocol::Query
    }
}

struct WireRequest {
    protocol: Protocol,
    action: String,
    json: Option<Value>,
    query: HashMap<String, String>,
}

impl WireRequest {
    fn parse(headers: &HeaderMap, body: Bytes) -> Result<Self, ApiError> {
        let protocol = protocol_from_headers(headers);
        match protocol {
            Protocol::Json => {
                let target = headers
                    .get("x-amz-target")
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(|| ApiError::missing("X-Amz-Target"))?;
                let action = target.rsplit('.').next().unwrap_or(target).to_owned();
                let json = serde_json::from_slice(&body).map_err(|error| {
                    ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("invalid JSON request: {error}"),
                    )
                })?;
                Ok(Self {
                    protocol,
                    action,
                    json: Some(json),
                    query: HashMap::new(),
                })
            }
            Protocol::Query => {
                let query: HashMap<String, String> =
                    serde_urlencoded::from_bytes(&body).map_err(|error| {
                        ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterValue",
                            format!("invalid query request: {error}"),
                        )
                    })?;
                let action = query
                    .get("Action")
                    .cloned()
                    .ok_or_else(|| ApiError::missing("Action"))?;
                Ok(Self {
                    protocol,
                    action,
                    json: None,
                    query,
                })
            }
        }
    }

    fn string(&self, name: &str) -> Option<&str> {
        self.json
            .as_ref()
            .and_then(|value| value.get(name))
            .and_then(Value::as_str)
            .or_else(|| self.query.get(name).map(String::as_str))
    }

    fn required_string(&self, name: &str) -> Result<&str, ApiError> {
        self.string(name)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ApiError::missing(name))
    }

    fn optional_string(&self, name: &str) -> Result<Option<&str>, ApiError> {
        if let Some(value) = self.json.as_ref().and_then(|json| json.get(name)) {
            return value
                .as_str()
                .map(Some)
                .ok_or_else(|| ApiError::invalid_parameter(name, "must be a string"));
        }
        Ok(self.string(name))
    }

    fn unsigned(&self, name: &str) -> Result<Option<u64>, ApiError> {
        if let Some(value) = self.json.as_ref().and_then(|json| json.get(name)) {
            return value.as_u64().map(Some).ok_or_else(|| {
                ApiError::invalid_parameter(name, "must be a non-negative integer")
            });
        }
        self.string(name)
            .map(|value| {
                value.parse().map_err(|_| {
                    ApiError::invalid_parameter(name, "must be a non-negative integer")
                })
            })
            .transpose()
    }

    fn attributes(&self) -> Result<HashMap<String, String>, ApiError> {
        if let Some(attributes) = self.json.as_ref().and_then(|value| value.get("Attributes")) {
            let attributes = attributes
                .as_object()
                .ok_or_else(|| ApiError::invalid_parameter("Attributes", "must be a string map"))?;
            return attributes
                .iter()
                .map(|(name, value)| {
                    value
                        .as_str()
                        .map(|value| (name.clone(), value.to_owned()))
                        .ok_or_else(|| {
                            ApiError::invalid_parameter(name, "attribute value must be a string")
                        })
                })
                .collect();
        }

        let mut entries =
            std::collections::BTreeMap::<String, (Option<String>, Option<String>)>::new();
        for (key, value) in &self.query {
            let Some(rest) = key.strip_prefix("Attribute.") else {
                continue;
            };
            let (index, field) = if matches!(rest, "Name" | "Value") {
                ("1", rest)
            } else {
                rest.split_once('.')
                    .ok_or_else(|| ApiError::invalid_parameter("Attributes", "invalid entry"))?
            };
            if index
                .parse::<usize>()
                .ok()
                .filter(|n| *n > 0 && n.to_string() == index)
                .is_none()
                || !matches!(field, "Name" | "Value")
            {
                return Err(ApiError::invalid_parameter("Attributes", "invalid entry"));
            }
            let entry = entries.entry(index.into()).or_default();
            let slot = if field == "Name" {
                &mut entry.0
            } else {
                &mut entry.1
            };
            if slot.replace(value.clone()).is_some() {
                return Err(ApiError::invalid_parameter("Attributes", "duplicate field"));
            }
        }
        let mut attributes = HashMap::new();
        for (_, (name, value)) in entries {
            let name = name.ok_or_else(|| ApiError::missing("Attribute.Name"))?;
            let value = value.ok_or_else(|| ApiError::missing("Attribute.Value"))?;
            if attributes.insert(name, value).is_some() {
                return Err(ApiError::invalid_parameter(
                    "Attributes",
                    "duplicate attribute",
                ));
            }
        }
        Ok(attributes)
    }
}

enum ApiSuccess {
    Management {
        action: &'static str,
        json: Value,
        xml: String,
    },
    Batch {
        action: &'static str,
        result: BatchResult<Value, ApiError>,
    },
    CreateQueue {
        queue_url: String,
    },
    SendMessage {
        result: SendResult,
        body: String,
        sequence_number: Option<String>,
    },
    ReceiveMessage {
        messages: Vec<ReceivedMessage>,
    },
    DeleteMessage,
    ChangeMessageVisibility,
    SetQueueAttributes,
    GetQueueAttributes {
        attributes: HashMap<String, String>,
    },
    ListDeadLetterSourceQueues {
        queue_urls: Vec<String>,
        next_cursor: Option<String>,
    },
}

impl ApiSuccess {
    fn into_response(self, protocol: Protocol, request_id: &str) -> Response {
        let body = match protocol {
            Protocol::Json => self.json_body(),
            Protocol::Query => self.xml_body(request_id),
        };
        response(StatusCode::OK, protocol, request_id, Body::from(body), None)
    }

    fn json_body(&self) -> String {
        let value = match self {
            Self::Management { json, .. } => json.clone(),
            Self::Batch { result, .. } => batch_http::json_result(result),
            Self::CreateQueue { queue_url } => json!({ "QueueUrl": queue_url }),
            Self::SendMessage {
                result,
                body,
                sequence_number,
            } => {
                let mut value = json!({
                    "MD5OfMessageBody": md5_hex(body),
                    "MessageId": result.message_id,
                });
                if let Some(sequence_number) = sequence_number {
                    value["SequenceNumber"] = Value::String(sequence_number.clone());
                }
                if let Some(digest) = &result.md5_of_message_attributes {
                    value["MD5OfMessageAttributes"] = json!(digest);
                }
                value
            }
            Self::ReceiveMessage { messages } => json!({
                "Messages": messages.iter().map(message_json).collect::<Vec<_>>()
            }),
            Self::DeleteMessage | Self::ChangeMessageVisibility | Self::SetQueueAttributes => {
                json!({})
            }
            Self::GetQueueAttributes { attributes } => json!({ "Attributes": attributes }),
            Self::ListDeadLetterSourceQueues {
                queue_urls,
                next_cursor,
            } => {
                let mut value = json!({ "queueUrls": queue_urls });
                if let Some(token) = next_cursor {
                    value["NextToken"] = json!(token);
                }
                value
            }
        };
        value.to_string()
    }

    fn xml_body(&self, request_id: &str) -> String {
        let (action, result) = match self {
            Self::Management { action, xml, .. } => (*action, xml.clone()),
            Self::Batch { action, result } => (*action, batch_http::xml_result(action, result)),
            Self::CreateQueue { queue_url } => (
                "CreateQueue",
                format!("<QueueUrl>{}</QueueUrl>", xml_escape(queue_url)),
            ),
            Self::SendMessage {
                result,
                body,
                sequence_number,
            } => {
                let sequence_number = sequence_number
                    .as_ref()
                    .map(|value| format!("<SequenceNumber>{}</SequenceNumber>", xml_escape(value)))
                    .unwrap_or_default();
                let attributes_digest = result
                    .md5_of_message_attributes
                    .as_ref()
                    .map(|digest| {
                        format!("<MD5OfMessageAttributes>{digest}</MD5OfMessageAttributes>")
                    })
                    .unwrap_or_default();
                (
                    "SendMessage",
                    format!(
                        "<MD5OfMessageBody>{}</MD5OfMessageBody><MessageId>{}</MessageId>{sequence_number}{attributes_digest}",
                        md5_hex(body),
                        xml_escape(&result.message_id),
                    ),
                )
            }
            Self::ReceiveMessage { messages } => (
                "ReceiveMessage",
                messages.iter().map(message_xml).collect::<String>(),
            ),
            Self::DeleteMessage => ("DeleteMessage", String::new()),
            Self::ChangeMessageVisibility => ("ChangeMessageVisibility", String::new()),
            Self::SetQueueAttributes => ("SetQueueAttributes", String::new()),
            Self::GetQueueAttributes { attributes } => (
                "GetQueueAttributes",
                attributes
                    .iter()
                    .map(|(name, value)| {
                        format!(
                            "<Attribute><Name>{}</Name><Value>{}</Value></Attribute>",
                            xml_escape(name),
                            xml_escape(value)
                        )
                    })
                    .collect::<String>(),
            ),
            Self::ListDeadLetterSourceQueues {
                queue_urls,
                next_cursor,
            } => {
                let mut result = queue_urls
                    .iter()
                    .map(|url| format!("<QueueUrl>{}</QueueUrl>", xml_escape(url)))
                    .collect::<String>();
                if let Some(token) = next_cursor {
                    result.push_str(&format!("<NextToken>{}</NextToken>", xml_escape(token)));
                }
                ("ListDeadLetterSourceQueues", result)
            }
        };
        format!(
            "<?xml version=\"1.0\"?><{action}Response xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\"><{action}Result>{result}</{action}Result><ResponseMetadata><RequestId>{}</RequestId></ResponseMetadata></{action}Response>",
            xml_escape(request_id)
        )
    }
}

fn message_json(message: &ReceivedMessage) -> Value {
    let mut value = json!({
        "MessageId": message.message_id,
        "ReceiptHandle": message.receipt_handle,
        "MD5OfBody": md5_hex(&message.body),
        "Body": message.body,
    });
    attributes_http::add_json_metadata(&mut value, message);
    value
}

fn message_xml(message: &ReceivedMessage) -> String {
    format!(
        "<Message><MessageId>{}</MessageId><ReceiptHandle>{}</ReceiptHandle><MD5OfBody>{}</MD5OfBody><Body>{}</Body>{}</Message>",
        xml_escape(&message.message_id),
        xml_escape(&message.receipt_handle),
        md5_hex(&message.body),
        xml_escape(&message.body),
        attributes_http::xml_metadata(message)
    )
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl From<LqsError> for ApiError {
    fn from(error: LqsError) -> Self {
        map_lqs_error(error)
    }
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    fn missing(parameter: &str) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "MissingParameter",
            format!("The request must contain the parameter {parameter}."),
        )
    }

    fn invalid_parameter(parameter: &str, reason: &str) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            format!("Invalid value for {parameter}: {reason}."),
        )
    }

    fn into_response(self, protocol: Protocol, request_id: &str) -> Response {
        let body = match protocol {
            Protocol::Json => json!({
                "__type": format!("com.amazonaws.sqs#{}", self.code),
                "message": self.message,
            })
            .to_string(),
            Protocol::Query => format!(
                "<?xml version=\"1.0\"?><ErrorResponse><Error><Type>Sender</Type><Code>{}</Code><Message>{}</Message><Detail/></Error><RequestId>{}</RequestId></ErrorResponse>",
                self.code,
                xml_escape(&self.message),
                xml_escape(request_id)
            ),
        };
        response(
            self.status,
            protocol,
            request_id,
            Body::from(body),
            Some(self.code),
        )
    }
}

fn dispatch(state: &AppState, path: &str, request: WireRequest) -> Result<ApiSuccess, ApiError> {
    match request.action.as_str() {
        "AddPermission" | "RemovePermission" => security_http::permission(state, path, &request),
        "ListQueues" | "GetQueueUrl" | "DeleteQueue" | "PurgeQueue" | "TagQueue" | "UntagQueue"
        | "ListQueueTags" => management_http::execute(state, path, &request),
        "SendMessageBatch" | "DeleteMessageBatch" | "ChangeMessageVisibilityBatch" => {
            batch_http::execute(state, path, &request)
        }
        "CreateQueue" => create_queue(state, &request),
        "SendMessage" => send_message(state, path, &request),
        "DeleteMessage" => delete_message(state, path, &request),
        "ChangeMessageVisibility" => change_message_visibility(state, path, &request),
        "SetQueueAttributes" => set_queue_attributes(state, path, &request),
        "GetQueueAttributes" => get_queue_attributes(state, path, &request),
        "ListDeadLetterSourceQueues" => list_dead_letter_source_queues(state, path, &request),
        _ => Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "InvalidAction",
            format!("Unsupported action: {}", request.action),
        )),
    }
}

fn create_queue(state: &AppState, request: &WireRequest) -> Result<ApiSuccess, ApiError> {
    let name = request.required_string("QueueName")?;
    validate_queue_name(name)?;
    let attributes = request.attributes()?;
    management_http::validate_attribute_names(&attributes, true)?;
    let tags = management_http::parse_tags(request)?;
    let fifo = parse_bool_attribute(&attributes, "FifoQueue")?.unwrap_or(false);
    let content_based_deduplication =
        parse_bool_attribute(&attributes, "ContentBasedDeduplication")?.unwrap_or(false);
    if content_based_deduplication && !fifo {
        return Err(ApiError::invalid_parameter(
            "ContentBasedDeduplication",
            "is only valid for FIFO queues",
        ));
    }
    let visibility_timeout_ms = match attributes.get("VisibilityTimeout") {
        Some(value) => parse_seconds("VisibilityTimeout", value, 0, 43_200)?
            .checked_mul(1_000)
            .ok_or_else(|| ApiError::invalid_parameter("VisibilityTimeout", "is too large"))?,
        None => QueueOptions::default().visibility_timeout_ms,
    };
    let queue_type = if fifo {
        QueueType::Fifo
    } else {
        QueueType::Standard
    };
    let delivery = delivery_attributes(&attributes)?;
    if !fifo
        && [
            "DeduplicationScope",
            "FifoThroughputLimit",
            "ContentBasedDeduplication",
        ]
        .iter()
        .any(|key| attributes.contains_key(*key))
    {
        return Err(ApiError::invalid_parameter(
            "Attributes",
            "FIFO attributes require a FIFO queue",
        ));
    }
    let options = QueueOptions {
        security: crate::QueueSecurity::default().updated(&delivery.security)?,
        deduplication_scope: delivery.deduplication_scope.unwrap_or_default(),
        fifo_throughput_limit: delivery.fifo_throughput_limit.unwrap_or_default(),
        visibility_timeout_ms,
        content_based_deduplication,
        redrive_policy: delivery.redrive_policy.flatten(),
        delay_ms: delivery.delay_ms.unwrap_or(0),
        message_retention_ms: delivery
            .message_retention_ms
            .unwrap_or(QueueOptions::default().message_retention_ms),
        maximum_message_size: delivery
            .maximum_message_size
            .unwrap_or(QueueOptions::default().maximum_message_size),
        receive_wait_time_ms: delivery.receive_wait_time_ms.unwrap_or(0),
        max_in_flight: delivery
            .max_in_flight
            .unwrap_or(QueueOptions::default().max_in_flight),
        ..QueueOptions::default()
    };
    let mut lqs = lock_lqs(state)?;
    if let Err(error) =
        lqs.create_queue_with_tags_at(name, queue_type, options.clone(), tags, unix_time_ms())
    {
        let same_configuration = matches!(&error, LqsError::QueueAlreadyExists(_))
            && lqs
                .queue_config(name)
                .map(|existing| {
                    existing.queue_type == queue_type
                        && existing.visibility_timeout_ms == options.visibility_timeout_ms
                        && existing.content_based_deduplication
                            == options.content_based_deduplication
                        && existing.deduplication_window_ms == options.deduplication_window_ms
                        && existing.redrive_policy == options.redrive_policy
                        && existing.delay_ms == options.delay_ms
                        && existing.message_retention_ms == options.message_retention_ms
                        && existing.maximum_message_size == options.maximum_message_size
                        && existing.receive_wait_time_ms == options.receive_wait_time_ms
                        && existing.max_in_flight == options.max_in_flight
                        && existing.deduplication_scope == options.deduplication_scope
                        && existing.fifo_throughput_limit == options.fifo_throughput_limit
                        && existing.security == options.security
                })
                .unwrap_or(false);
        if !same_configuration {
            return Err(map_lqs_error(error));
        }
    }
    Ok(ApiSuccess::CreateQueue {
        queue_url: format!("{}/000000000000/{name}", state.public_base_url),
    })
}

// LQS is a single-account, single-region local service.
const QUEUE_ARN_PREFIX: &str = "arn:aws:sqs:us-east-1:000000000000:";

fn parse_redrive_policy(value: &str) -> Result<Option<RedrivePolicy>, ApiError> {
    if value.is_empty() {
        return Ok(None);
    }
    let invalid = || {
        ApiError::invalid_parameter(
            "RedrivePolicy",
            "requires a local deadLetterTargetArn and integer maxReceiveCount from 1 to 1000",
        )
    };
    let value: Value = serde_json::from_str(value).map_err(|_| invalid())?;
    let target = value
        .get("deadLetterTargetArn")
        .and_then(Value::as_str)
        .and_then(|arn| arn.strip_prefix(QUEUE_ARN_PREFIX))
        .ok_or_else(invalid)?;
    validate_queue_name(target).map_err(|_| invalid())?;
    let count = value
        .get("maxReceiveCount")
        .and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_str().and_then(|s| s.parse::<u64>().ok()))
        })
        .filter(|count| (1..=1000).contains(count))
        .ok_or_else(invalid)?;
    Ok(Some(RedrivePolicy {
        dead_letter_queue: target.to_owned(),
        max_receive_count: count as u32,
    }))
}

fn delivery_attributes(attributes: &HashMap<String, String>) -> Result<QueueUpdate, ApiError> {
    Ok(QueueUpdate {
        security: security_http::parse_update(attributes)?,
        visibility_timeout_ms: attributes
            .get("VisibilityTimeout")
            .map(|value| {
                parse_seconds("VisibilityTimeout", value, 0, 43_200).map(|seconds| seconds * 1000)
            })
            .transpose()?,
        content_based_deduplication: parse_bool_attribute(attributes, "ContentBasedDeduplication")?,
        deduplication_scope: attributes
            .get("DeduplicationScope")
            .map(|value| value.parse())
            .transpose()?,
        fifo_throughput_limit: attributes
            .get("FifoThroughputLimit")
            .map(|value| value.parse())
            .transpose()?,
        receive_wait_time_ms: attributes
            .get("ReceiveMessageWaitTimeSeconds")
            .map(|value| {
                parse_seconds("ReceiveMessageWaitTimeSeconds", value, 0, 20)
                    .map(|seconds| seconds * 1000)
            })
            .transpose()?,
        max_in_flight: attributes
            .get("LqsMaxInFlightMessages")
            .map(|value| {
                parse_seconds(
                    "LqsMaxInFlightMessages",
                    value,
                    1,
                    crate::DEFAULT_MAX_IN_FLIGHT as u64,
                )
                .map(|count| count as usize)
            })
            .transpose()?,
        delay_ms: attributes
            .get("DelaySeconds")
            .map(|value| parse_seconds("DelaySeconds", value, 0, 900).map(|seconds| seconds * 1000))
            .transpose()?,
        message_retention_ms: attributes
            .get("MessageRetentionPeriod")
            .map(|value| {
                parse_seconds("MessageRetentionPeriod", value, 60, 1_209_600)
                    .map(|seconds| seconds * 1000)
            })
            .transpose()?,
        maximum_message_size: attributes
            .get("MaximumMessageSize")
            .map(|value| {
                parse_seconds("MaximumMessageSize", value, 1024, 1_048_576)
                    .map(|bytes| bytes as usize)
            })
            .transpose()?,
        redrive_policy: attributes
            .get("RedrivePolicy")
            .map(|value| parse_redrive_policy(value))
            .transpose()?,
    })
}

fn set_queue_attributes(
    state: &AppState,
    path: &str,
    request: &WireRequest,
) -> Result<ApiSuccess, ApiError> {
    let name = queue_name(path, request)?;
    let attributes = request.attributes()?;
    management_http::validate_attribute_names(&attributes, false)?;
    if attributes.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "InvalidAttributeName",
            "unsupported queue attribute",
        ));
    }
    let update = delivery_attributes(&attributes)?;
    lock_lqs(state)?
        .set_queue_attributes(&name, update, unix_time_ms())
        .map_err(map_lqs_error)?;
    Ok(ApiSuccess::SetQueueAttributes)
}

fn get_queue_attributes(
    state: &AppState,
    path: &str,
    request: &WireRequest,
) -> Result<ApiSuccess, ApiError> {
    let name = queue_name(path, request)?;
    let lqs = lock_lqs(state)?;
    let config = lqs.queue_config(&name).map_err(map_lqs_error)?;
    let metrics = lqs.queue_metrics(&name, unix_time_ms())?;
    let names: Vec<&str> = if let Some(json) = &request.json {
        match json.get("AttributeNames") {
            None => Vec::new(),
            Some(Value::Array(values)) => values
                .iter()
                .map(|value| {
                    value.as_str().ok_or_else(|| {
                        ApiError::invalid_parameter("AttributeNames", "must be an array of strings")
                    })
                })
                .collect::<Result<_, _>>()?,
            _ => {
                return Err(ApiError::invalid_parameter(
                    "AttributeNames",
                    "must be an array of strings",
                ));
            }
        }
    } else {
        request
            .query
            .iter()
            .filter(|(key, _)| key.as_str() == "AttributeName" || key.starts_with("AttributeName."))
            .map(|(_, value)| value.as_str())
            .collect()
    };
    let mut attributes = HashMap::from([
        ("QueueArn".to_owned(), format!("{QUEUE_ARN_PREFIX}{name}")),
        (
            "FifoQueue".to_owned(),
            (config.queue_type == QueueType::Fifo).to_string(),
        ),
        (
            "ApproximateNumberOfMessages".to_owned(),
            metrics.visible.to_string(),
        ),
        (
            "ApproximateNumberOfMessagesDelayed".to_owned(),
            metrics.delayed.to_string(),
        ),
        (
            "CreatedTimestamp".to_owned(),
            (metrics.created_at_ms / 1000).to_string(),
        ),
        (
            "LastModifiedTimestamp".to_owned(),
            (metrics.modified_at_ms / 1000).to_string(),
        ),
        (
            "ReceiveMessageWaitTimeSeconds".to_owned(),
            (config.receive_wait_time_ms / 1000).to_string(),
        ),
        (
            "LqsMaxInFlightMessages".to_owned(),
            config.max_in_flight.to_string(),
        ),
        (
            "ApproximateNumberOfMessagesNotVisible".to_owned(),
            metrics.not_visible.to_string(),
        ),
        (
            "DelaySeconds".to_owned(),
            (config.delay_ms / 1000).to_string(),
        ),
        (
            "MessageRetentionPeriod".to_owned(),
            (config.message_retention_ms / 1000).to_string(),
        ),
        (
            "MaximumMessageSize".to_owned(),
            config.maximum_message_size.to_string(),
        ),
        (
            "VisibilityTimeout".to_owned(),
            (config.visibility_timeout_ms / 1000).to_string(),
        ),
    ]);
    security_http::add_attributes(&mut attributes, &config.security);
    if config.queue_type == QueueType::Fifo {
        attributes.insert("FifoQueue".into(), "true".into());
        attributes.insert(
            "DeduplicationScope".into(),
            config.deduplication_scope.as_str().into(),
        );
        attributes.insert(
            "FifoThroughputLimit".into(),
            config.fifo_throughput_limit.as_str().into(),
        );
        attributes.insert(
            "ContentBasedDeduplication".into(),
            config.content_based_deduplication.to_string(),
        );
    }
    if let Some(policy) = config.redrive_policy {
        attributes.insert(
            "RedrivePolicy".into(),
            json!({
                "deadLetterTargetArn": format!("{QUEUE_ARN_PREFIX}{}", policy.dead_letter_queue),
                "maxReceiveCount": policy.max_receive_count,
            })
            .to_string(),
        );
    }
    for name in &names {
        if ![
            "All",
            "QueueArn",
            "VisibilityTimeout",
            "FifoQueue",
            "DeduplicationScope",
            "FifoThroughputLimit",
            "ContentBasedDeduplication",
            "RedrivePolicy",
            "DelaySeconds",
            "MessageRetentionPeriod",
            "MaximumMessageSize",
            "ReceiveMessageWaitTimeSeconds",
            "LqsMaxInFlightMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesDelayed",
            "CreatedTimestamp",
            "LastModifiedTimestamp",
            "Policy",
            "SqsManagedSseEnabled",
            "KmsMasterKeyId",
            "KmsDataKeyReusePeriodSeconds",
        ]
        .contains(name)
        {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "InvalidAttributeName",
                format!("unsupported attribute: {name}"),
            ));
        }
    }
    if !names.contains(&"All") {
        attributes.retain(|key, _| names.contains(&key.as_str()));
    }
    Ok(ApiSuccess::GetQueueAttributes { attributes })
}

fn list_dead_letter_source_queues(
    state: &AppState,
    path: &str,
    request: &WireRequest,
) -> Result<ApiSuccess, ApiError> {
    let name = queue_name(path, request)?;
    let requested_max = request.unsigned("MaxResults")?;
    let max = requested_max.unwrap_or(1000);
    if !(1..=1000).contains(&max) {
        return Err(ApiError::invalid_parameter(
            "MaxResults",
            "must be between 1 and 1000",
        ));
    }
    let prefix = format!("lqs-dlq:{name}:");
    let cursor = request
        .string("NextToken")
        .map(|token| {
            token
                .strip_prefix(&prefix)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| {
                    ApiError::invalid_parameter("NextToken", "invalid token for this DLQ")
                })
        })
        .transpose()?;
    let mut sources = lock_lqs(state)?
        .list_dead_letter_source_queues(&name)
        .map_err(map_lqs_error)?;
    if let Some(cursor) = cursor {
        sources.retain(|source| source.as_str() > cursor);
    }
    let next_cursor = if sources.len() > max as usize {
        sources.truncate(max as usize);
        requested_max.map(|_| format!("{prefix}{}", sources.last().unwrap()))
    } else {
        None
    };
    let queue_urls = sources
        .iter()
        .map(|source| format!("{}/000000000000/{source}", state.public_base_url))
        .collect();
    Ok(ApiSuccess::ListDeadLetterSourceQueues {
        queue_urls,
        next_cursor,
    })
}

fn send_message(
    state: &AppState,
    path: &str,
    request: &WireRequest,
) -> Result<ApiSuccess, ApiError> {
    let queue_name = queue_name(path, request)?;
    let send_request = parse_send_request(request)?;
    let body = send_request.body.clone();
    let mut lqs = lock_lqs(state)?;
    let is_fifo_message = lqs.queue_config(&queue_name)?.queue_type == QueueType::Fifo;
    let result = lqs.send(&queue_name, send_request, unix_time_ms())?;
    let sequence_number = is_fifo_message.then(|| sequence_number(&result.message_id));
    Ok(ApiSuccess::SendMessage {
        result,
        body,
        sequence_number,
    })
}

fn parse_send_request(request: &WireRequest) -> Result<SendRequest, ApiError> {
    let body = request
        .string("MessageBody")
        .ok_or_else(|| ApiError::missing("MessageBody"))?
        .to_owned();
    let message_group_id = request
        .optional_string("MessageGroupId")?
        .map(str::to_owned);
    Ok(SendRequest {
        body,
        message_attributes: attributes_http::parse_attributes(request)?,
        message_group_id,
        deduplication_id: request
            .optional_string("MessageDeduplicationId")?
            .map(str::to_owned),
        delay_ms: request
            .unsigned("DelaySeconds")?
            .map(|seconds| {
                seconds.checked_mul(1000).ok_or_else(|| {
                    ApiError::invalid_parameter("DelaySeconds", "must be between 0 and 900")
                })
            })
            .transpose()?,
    })
}

async fn receive_message(
    state: &AppState,
    path: &str,
    request: &WireRequest,
) -> Result<ApiSuccess, ApiError> {
    let queue_name = queue_name(path, request)?;
    let max_messages = request.unsigned("MaxNumberOfMessages")?.unwrap_or(1);
    let selection = attributes_http::Selection::parse(request)?;
    let visibility = request.unsigned("VisibilityTimeout")?;
    if visibility.is_some_and(|seconds| seconds > 43_200) {
        return Err(ApiError::invalid_parameter(
            "VisibilityTimeout",
            "must be between 0 and 43200",
        ));
    }
    let options = crate::ReceiveOptions {
        receive_request_attempt_id: request
            .optional_string("ReceiveRequestAttemptId")?
            .map(str::to_owned),
        visibility_timeout_ms: visibility.map(|seconds| seconds * 1000),
    };
    if !(1..=10).contains(&max_messages) {
        return Err(ApiError::invalid_parameter(
            "MaxNumberOfMessages",
            "must be between 1 and 10",
        ));
    }
    let requested_wait = request.unsigned("WaitTimeSeconds")?;
    if requested_wait.is_some_and(|seconds| seconds > 20) {
        return Err(ApiError::invalid_parameter(
            "WaitTimeSeconds",
            "must be between 0 and 20",
        ));
    }
    let wait_ms = requested_wait.map(|seconds| seconds * 1000).unwrap_or(
        lock_lqs(state)?
            .queue_config(&queue_name)?
            .receive_wait_time_ms,
    );
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(wait_ms);
    loop {
        // Neither the Mutex nor the SQLite transaction lives across the await below.
        let result = {
            lock_lqs(state)?.receive_page(
                &queue_name,
                max_messages as usize,
                &options,
                unix_time_ms(),
                tokio::time::Instant::now() >= deadline,
            )
        };
        match result {
            Ok((mut messages, true)) => {
                for message in &mut messages {
                    selection.apply(message);
                }
                return Ok(ApiSuccess::ReceiveMessage { messages });
            }
            Ok((_, false)) => {}
            Err(LqsError::OverLimit) if wait_ms > 0 => {
                if tokio::time::Instant::now() >= deadline {
                    return Ok(ApiSuccess::ReceiveMessage {
                        messages: Vec::new(),
                    });
                }
            }
            Err(error) => return Err(error.into()),
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            continue;
        }
        // Also detects time-driven availability and writes from other DB connections.
        tokio::time::sleep_until(deadline.min(now + std::time::Duration::from_millis(100))).await;
    }
}

fn delete_message(
    state: &AppState,
    path: &str,
    request: &WireRequest,
) -> Result<ApiSuccess, ApiError> {
    let queue_name = queue_name(path, request)?;
    let receipt_handle = request.required_string("ReceiptHandle")?;
    lock_lqs(state)?
        .delete(&queue_name, receipt_handle)
        .map_err(map_lqs_error)?;
    Ok(ApiSuccess::DeleteMessage)
}

fn change_message_visibility(
    state: &AppState,
    path: &str,
    request: &WireRequest,
) -> Result<ApiSuccess, ApiError> {
    let queue_name = queue_name(path, request)?;
    let receipt_handle = request.required_string("ReceiptHandle")?;
    let timeout = request
        .unsigned("VisibilityTimeout")?
        .ok_or_else(|| ApiError::missing("VisibilityTimeout"))?;
    if timeout > 43_200 {
        return Err(ApiError::invalid_parameter(
            "VisibilityTimeout",
            "must be between 0 and 43200",
        ));
    }
    lock_lqs(state)?
        .change_visibility(
            &queue_name,
            receipt_handle,
            timeout.saturating_mul(1_000),
            unix_time_ms(),
        )
        .map_err(map_lqs_error)?;
    Ok(ApiSuccess::ChangeMessageVisibility)
}

fn queue_name(path: &str, request: &WireRequest) -> Result<String, ApiError> {
    let source = request.string("QueueUrl").unwrap_or(path);
    source
        .split('?')
        .next()
        .and_then(|value| value.trim_end_matches('/').rsplit('/').next())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| ApiError::missing("QueueUrl"))
}

fn validate_queue_name(name: &str) -> Result<(), ApiError> {
    let stem = name.strip_suffix(".fifo").unwrap_or(name);
    if !(1..=80).contains(&name.len())
        || stem.is_empty()
        || !stem
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(ApiError::invalid_parameter(
            "QueueName",
            "must be 1-80 characters containing letters, digits, hyphens, or underscores, with an optional .fifo suffix",
        ));
    }
    Ok(())
}

fn parse_bool_attribute(
    attributes: &HashMap<String, String>,
    name: &str,
) -> Result<Option<bool>, ApiError> {
    attributes
        .get(name)
        .map(|value| match value.as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(ApiError::invalid_parameter(name, "must be true or false")),
        })
        .transpose()
}

fn parse_seconds(name: &str, value: &str, minimum: u64, maximum: u64) -> Result<u64, ApiError> {
    let seconds = u64::from_str(value)
        .map_err(|_| ApiError::invalid_parameter(name, "must be an integer"))?;
    if !(minimum..=maximum).contains(&seconds) {
        return Err(ApiError::invalid_parameter(
            name,
            &format!("must be between {minimum} and {maximum}"),
        ));
    }
    Ok(seconds)
}

fn lock_lqs(state: &AppState) -> Result<std::sync::MutexGuard<'_, Lqs>, ApiError> {
    let guard = state.lqs.lock().map_err(|_| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "queue database lock is poisoned",
        )
    })?;
    if let Some(access) = &state.access {
        security_http::authorize(&guard, access)?;
    }
    Ok(guard)
}

fn map_lqs_error(error: LqsError) -> ApiError {
    let (status, code) = match error {
        LqsError::AccessDenied => (StatusCode::FORBIDDEN, "AccessDenied"),
        LqsError::PurgeQueueInProgress => (
            StatusCode::BAD_REQUEST,
            "AWS.SimpleQueueService.PurgeQueueInProgress",
        ),
        LqsError::QueueDeletedRecently => (
            StatusCode::BAD_REQUEST,
            "AWS.SimpleQueueService.QueueDeletedRecently",
        ),
        LqsError::QueueAlreadyExists(_) => (StatusCode::BAD_REQUEST, "QueueNameExists"),
        LqsError::QueueNotFound(_) => (
            StatusCode::BAD_REQUEST,
            "AWS.SimpleQueueService.NonExistentQueue",
        ),
        LqsError::InvalidReceiptHandle(_) => (StatusCode::BAD_REQUEST, "ReceiptHandleIsInvalid"),
        LqsError::Database(_) => (StatusCode::INTERNAL_SERVER_ERROR, "InternalError"),
        LqsError::InvalidBatch(code) => (StatusCode::BAD_REQUEST, code),
        LqsError::OverLimit => (StatusCode::BAD_REQUEST, "OverLimit"),
        LqsError::MessageNotInflight => (StatusCode::BAD_REQUEST, "MessageNotInflight"),
        _ => (StatusCode::BAD_REQUEST, "InvalidParameterValue"),
    };
    ApiError::new(status, code, error.to_string())
}

fn response(
    status: StatusCode,
    protocol: Protocol,
    request_id: &str,
    body: Body,
    error_code: Option<&str>,
) -> Response {
    let mut response = (status, body).into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(match protocol {
            Protocol::Json => "application/x-amz-json-1.0",
            Protocol::Query => "text/xml; charset=utf-8",
        }),
    );
    if let Ok(value) = HeaderValue::from_str(request_id) {
        headers.insert("x-amzn-requestid", value);
    }
    if let Some(error_code) = error_code {
        if let Ok(value) = HeaderValue::from_str(error_code) {
            headers.insert("x-amzn-errortype", value);
        }
        if protocol == Protocol::Json {
            let query_error = format!("{error_code};Sender");
            if let Ok(value) = HeaderValue::from_str(&query_error) {
                headers.insert("x-amzn-query-error", value);
            }
        }
    }
    response
}

fn md5_hex(body: &str) -> String {
    format!("{:x}", md5::compute(body.as_bytes()))
}

fn sequence_number(message_id: &str) -> String {
    message_id
        .strip_prefix("msg-")
        .and_then(|value| u64::from_str_radix(value, 16).ok())
        .unwrap_or_default()
        .to_string()
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;
    use tower::ServiceExt;

    #[test]
    fn parses_query_attributes() {
        let request = WireRequest::parse(
            &HeaderMap::new(),
            Bytes::from_static(
                b"Action=CreateQueue&QueueName=orders.fifo&Attribute.1.Name=FifoQueue&Attribute.1.Value=true",
            ),
        )
        .unwrap();

        assert_eq!(request.protocol, Protocol::Query);
        assert_eq!(request.action, "CreateQueue");
        assert_eq!(request.required_string("QueueName").unwrap(), "orders.fifo");
        assert_eq!(
            request.attributes().unwrap().get("FifoQueue").unwrap(),
            "true"
        );
    }

    #[test]
    fn escapes_xml_values() {
        assert_eq!(xml_escape("<&>\"'"), "&lt;&amp;&gt;&quot;&apos;");
    }

    #[tokio::test]
    async fn json_errors_have_sqs_codes_and_request_ids() {
        let app = router(Lqs::new(), "http://localhost:9324");
        let request = Request::builder()
            .method(Method::POST)
            .uri("/")
            .header("x-amz-target", "AmazonSQS.CreateQueue")
            .header(header::CONTENT_TYPE, "application/x-amz-json-1.0")
            .body(Body::from(r#"{"QueueName":"invalid.fifo"}"#))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(response.headers().contains_key("x-amzn-requestid"));
        assert_eq!(
            response
                .headers()
                .get("x-amzn-query-error")
                .unwrap()
                .to_str()
                .unwrap(),
            "InvalidParameterValue;Sender"
        );
        let body = to_bytes(response.into_body(), MAX_REQUEST_BYTES)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["__type"], "com.amazonaws.sqs#InvalidParameterValue");
    }

    #[tokio::test]
    async fn query_create_queue_returns_an_xml_queue_url() {
        let app = router(Lqs::new(), "http://localhost:9324");
        let request = Request::builder()
            .method(Method::POST)
            .uri("/")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(
                "Action=CreateQueue&Version=2012-11-05&QueueName=events",
            ))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/xml; charset=utf-8"
        );
        let body = to_bytes(response.into_body(), MAX_REQUEST_BYTES)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("<CreateQueueResponse"));
        assert!(body.contains("<QueueUrl>http://localhost:9324/000000000000/events</QueueUrl>"));
    }
}
