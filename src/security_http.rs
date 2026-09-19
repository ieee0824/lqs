use super::*;
use crate::{QueueSecurity, RequestIdentity, SecurityUpdate};

#[cfg(test)]
#[path = "security_http_tests.rs"]
mod tests;

/// A synchronous, nonblocking identity/authentication hook. It may also deny
/// global actions (CreateQueue/ListQueues). Queue policies are checked afterwards.
pub type AuthorizationHook =
    dyn Fn(&HttpAuthorizationRequest) -> Result<RequestIdentity, LqsError> + Send + Sync;

#[derive(Clone, Debug)]
pub struct HttpAuthorizationRequest {
    pub action: String,
    pub queue_name: Option<String>,
    pub headers: HeaderMap,
    pub method: Method,
    pub uri: axum::http::Uri,
    pub body: Bytes,
}

#[derive(Clone)]
pub(super) struct AccessContext {
    identity: RequestIdentity,
    action: String,
    queue: Option<String>,
}

pub(super) fn local_hook(trust_header: bool) -> Arc<AuthorizationHook> {
    Arc::new(move |request| {
        let values: Vec<_> = request.headers.get_all("x-lqs-principal").iter().collect();
        if values.is_empty() {
            return Ok(RequestIdentity::Anonymous);
        }
        if !trust_header || values.len() != 1 {
            return Err(LqsError::AccessDenied);
        }
        RequestIdentity::aws(values[0].to_str().map_err(|_| LqsError::AccessDenied)?)
    })
}

pub(super) fn access_context(
    state: &AppState,
    path: &str,
    request: &WireRequest,
    headers: HeaderMap,
    uri: axum::http::Uri,
    body: Bytes,
) -> Result<AccessContext, ApiError> {
    let queue = match request.action.as_str() {
        "ListQueues" => None,
        "CreateQueue" | "GetQueueUrl" => Some(request.required_string("QueueName")?.to_owned()),
        "SendMessage"
        | "ReceiveMessage"
        | "DeleteMessage"
        | "ChangeMessageVisibility"
        | "SendMessageBatch"
        | "DeleteMessageBatch"
        | "ChangeMessageVisibilityBatch"
        | "GetQueueAttributes"
        | "SetQueueAttributes"
        | "ListDeadLetterSourceQueues"
        | "DeleteQueue"
        | "PurgeQueue"
        | "TagQueue"
        | "UntagQueue"
        | "ListQueueTags"
        | "AddPermission"
        | "RemovePermission" => Some(queue_name(path, request)?),
        _ => None,
    };
    let identity = (state.authorization_hook)(&HttpAuthorizationRequest {
        action: request.action.clone(),
        queue_name: queue.clone(),
        headers,
        method: Method::POST,
        uri,
        body,
    })?;
    Ok(AccessContext {
        identity,
        queue,
        action: request.action.clone(),
    })
}

// Called while the shared Lqs mutex is held, immediately before each operation.
// Long polling calls this again on every pass (including replayed attempts).
pub(super) fn authorize(lqs: &Lqs, access: &AccessContext) -> Result<(), ApiError> {
    if let Some(queue) = &access.queue {
        if access.action == "CreateQueue" && !lqs.queue_exists(queue)? {
            return Ok(());
        }
        lqs.authorize_queue(queue, &access.identity, &access.action)?;
    }
    Ok(())
}

pub(super) fn parse_update(
    attributes: &HashMap<String, String>,
) -> Result<SecurityUpdate, ApiError> {
    Ok(SecurityUpdate {
        policy: attributes
            .get("Policy")
            .map(|value| (!value.is_empty()).then(|| value.clone())),
        sqs_managed_sse_enabled: parse_bool_attribute(attributes, "SqsManagedSseEnabled")?,
        kms_master_key_id: attributes.get("KmsMasterKeyId").cloned(),
        kms_data_key_reuse_period_seconds: attributes
            .get("KmsDataKeyReusePeriodSeconds")
            .map(|value| parse_seconds("KmsDataKeyReusePeriodSeconds", value, 60, 86_400))
            .transpose()?,
    })
}

pub(super) fn add_attributes(attributes: &mut HashMap<String, String>, security: &QueueSecurity) {
    attributes.insert(
        "SqsManagedSseEnabled".into(),
        security.sqs_managed_sse_enabled.to_string(),
    );
    if let Some(policy) = &security.policy {
        attributes.insert("Policy".into(), policy.clone());
    }
    if let Some(key) = &security.kms_master_key_id {
        attributes.insert("KmsMasterKeyId".into(), key.clone());
    }
    attributes.insert(
        "KmsDataKeyReusePeriodSeconds".into(),
        security.kms_data_key_reuse_period_seconds.to_string(),
    );
}

fn list(request: &WireRequest, json_name: &str, query_name: &str) -> Result<Vec<String>, ApiError> {
    if let Some(json) = &request.json {
        return json
            .get(json_name)
            .ok_or_else(|| ApiError::missing(json_name))?
            .as_array()
            .ok_or_else(|| ApiError::invalid_parameter(json_name, "must be a string list"))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| ApiError::invalid_parameter(json_name, "must be a string list"))
            })
            .collect();
    }
    let mut values = std::collections::BTreeMap::new();
    for (key, value) in &request.query {
        let index = if key == query_name {
            Some("1")
        } else {
            key.strip_prefix(&format!("{query_name}."))
        };
        if let Some(index) = index {
            let index = index
                .parse::<usize>()
                .ok()
                .filter(|n| *n > 0 && n.to_string() == index)
                .ok_or_else(|| ApiError::invalid_parameter(json_name, "invalid index"))?;
            if values.insert(index, value.clone()).is_some() {
                return Err(ApiError::invalid_parameter(json_name, "duplicate entry"));
            }
        }
    }
    Ok(values.into_values().collect())
}

pub(super) fn permission(
    state: &AppState,
    path: &str,
    request: &WireRequest,
) -> Result<ApiSuccess, ApiError> {
    let queue = queue_name(path, request)?;
    let label = request.required_string("Label")?;
    let action = if request.action == "AddPermission" {
        let accounts = list(request, "AWSAccountIds", "AWSAccountId")?;
        let actions = list(request, "Actions", "ActionName")?;
        lock_lqs(state)?.add_permission(&queue, label, &accounts, &actions, unix_time_ms())?;
        "AddPermission"
    } else {
        lock_lqs(state)?.remove_permission(&queue, label, unix_time_ms())?;
        "RemovePermission"
    };
    Ok(ApiSuccess::Management {
        action,
        json: json!({}),
        xml: String::new(),
    })
}
