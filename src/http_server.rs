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
    Lqs, LqsError, QueueOptions, QueueType, ReceivedMessage, RedrivePolicy, SendRequest, SendResult,
};

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:9324";
const DEFAULT_DATABASE_PATH: &str = "lqs.sqlite";
const MAX_REQUEST_BYTES: usize = 1_048_576;

#[derive(Debug, Clone)]
pub struct ServerConfig {
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
            bind_addr,
            database_path,
            public_base_url,
        })
    }
}

#[derive(Debug)]
pub enum ServerConfigError {
    InvalidBindAddress(String),
    InvalidBaseUrl(String),
}

impl fmt::Display for ServerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
    serve_with_listener(listener, lqs, config.public_base_url).await
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
    let state = AppState {
        lqs: Arc::new(Mutex::new(lqs)),
        public_base_url: Arc::from(public_base_url.into().trim_end_matches('/').to_owned()),
        request_sequence: Arc::new(AtomicU64::new(1)),
    };
    Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .fallback(handle_request)
        .with_state(state)
}

async fn handle_request(State(state): State<AppState>, request: Request) -> Response {
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
    let wire_request = match WireRequest::parse(&headers, body) {
        Ok(request) => request,
        Err(error) => {
            return error.into_response(protocol_from_headers(&headers), &request_id);
        }
    };
    let protocol = wire_request.protocol;
    match dispatch(&state, &path, wire_request) {
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

        let mut attributes = HashMap::new();
        for (key, name) in &self.query {
            let Some(index) = key
                .strip_prefix("Attribute.")
                .and_then(|value| value.strip_suffix(".Name"))
            else {
                continue;
            };
            if let Some(value) = self.query.get(&format!("Attribute.{index}.Value")) {
                attributes.insert(name.clone(), value.clone());
            }
        }
        Ok(attributes)
    }
}

enum ApiSuccess {
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
                (
                    "SendMessage",
                    format!(
                        "<MD5OfMessageBody>{}</MD5OfMessageBody><MessageId>{}</MessageId>{sequence_number}",
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
    json!({
        "MessageId": message.message_id,
        "ReceiptHandle": message.receipt_handle,
        "MD5OfBody": md5_hex(&message.body),
        "Body": message.body,
    })
}

fn message_xml(message: &ReceivedMessage) -> String {
    format!(
        "<Message><MessageId>{}</MessageId><ReceiptHandle>{}</ReceiptHandle><MD5OfBody>{}</MD5OfBody><Body>{}</Body></Message>",
        xml_escape(&message.message_id),
        xml_escape(&message.receipt_handle),
        md5_hex(&message.body),
        xml_escape(&message.body)
    )
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
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
        "CreateQueue" => create_queue(state, &request),
        "SendMessage" => send_message(state, path, &request),
        "ReceiveMessage" => receive_message(state, path, &request),
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
    if visibility_timeout_ms == 0 {
        return Err(ApiError::invalid_parameter(
            "VisibilityTimeout",
            "must be greater than zero in LQS",
        ));
    }
    let queue_type = if fifo {
        QueueType::Fifo
    } else {
        QueueType::Standard
    };
    let options = QueueOptions {
        visibility_timeout_ms,
        content_based_deduplication,
        redrive_policy: attributes
            .get("RedrivePolicy")
            .map(|value| parse_redrive_policy(value))
            .transpose()?
            .flatten(),
        ..QueueOptions::default()
    };
    let mut lqs = lock_lqs(state)?;
    if let Err(error) = lqs.create_queue(name, queue_type, options.clone()) {
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

fn set_queue_attributes(
    state: &AppState,
    path: &str,
    request: &WireRequest,
) -> Result<ApiSuccess, ApiError> {
    let name = queue_name(path, request)?;
    let attributes = request.attributes()?;
    if attributes.len() != 1 || !attributes.contains_key("RedrivePolicy") {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "InvalidAttributeName",
            "only RedrivePolicy can currently be set",
        ));
    }
    let policy = parse_redrive_policy(&attributes["RedrivePolicy"])?;
    lock_lqs(state)?
        .set_redrive_policy(&name, policy)
        .map_err(map_lqs_error)?;
    Ok(ApiSuccess::SetQueueAttributes)
}

fn get_queue_attributes(
    state: &AppState,
    path: &str,
    request: &WireRequest,
) -> Result<ApiSuccess, ApiError> {
    let name = queue_name(path, request)?;
    let config = lock_lqs(state)?
        .queue_config(&name)
        .map_err(map_lqs_error)?;
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
            .filter(|(key, _)| key.starts_with("AttributeName."))
            .map(|(_, value)| value.as_str())
            .collect()
    };
    let mut attributes = HashMap::from([
        ("QueueArn".to_owned(), format!("{QUEUE_ARN_PREFIX}{name}")),
        (
            "VisibilityTimeout".to_owned(),
            (config.visibility_timeout_ms / 1000).to_string(),
        ),
    ]);
    if config.queue_type == QueueType::Fifo {
        attributes.insert("FifoQueue".into(), "true".into());
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
            "ContentBasedDeduplication",
            "RedrivePolicy",
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
    let body = request.required_string("MessageBody")?.to_owned();
    let message_group_id = request.string("MessageGroupId").map(str::to_owned);
    let is_fifo_message = message_group_id.is_some();
    let send_request = SendRequest {
        body: body.clone(),
        message_group_id,
        deduplication_id: request.string("MessageDeduplicationId").map(str::to_owned),
    };
    let result = lock_lqs(state)?
        .send(&queue_name, send_request, unix_time_ms())
        .map_err(map_lqs_error)?;
    let sequence_number = is_fifo_message.then(|| sequence_number(&result.message_id));
    Ok(ApiSuccess::SendMessage {
        result,
        body,
        sequence_number,
    })
}

fn receive_message(
    state: &AppState,
    path: &str,
    request: &WireRequest,
) -> Result<ApiSuccess, ApiError> {
    let queue_name = queue_name(path, request)?;
    let max_messages = request.unsigned("MaxNumberOfMessages")?.unwrap_or(1);
    if !(1..=10).contains(&max_messages) {
        return Err(ApiError::invalid_parameter(
            "MaxNumberOfMessages",
            "must be between 1 and 10",
        ));
    }
    let messages = lock_lqs(state)?
        .receive(&queue_name, max_messages as usize, unix_time_ms())
        .map_err(map_lqs_error)?;
    Ok(ApiSuccess::ReceiveMessage { messages })
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
    state.lqs.lock().map_err(|_| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "queue database lock is poisoned",
        )
    })
}

fn map_lqs_error(error: LqsError) -> ApiError {
    let (status, code) = match error {
        LqsError::QueueAlreadyExists(_) => (StatusCode::BAD_REQUEST, "QueueNameExists"),
        LqsError::QueueNotFound(_) => (
            StatusCode::BAD_REQUEST,
            "AWS.SimpleQueueService.NonExistentQueue",
        ),
        LqsError::InvalidReceiptHandle(_) => (StatusCode::BAD_REQUEST, "ReceiptHandleIsInvalid"),
        LqsError::Database(_) => (StatusCode::INTERNAL_SERVER_ERROR, "InternalError"),
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
