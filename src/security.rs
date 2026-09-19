use crate::LqsError;
use serde_json::{Value, json};

pub const LOCAL_QUEUE_ARN_PREFIX: &str = "arn:aws:sqs:us-east-1:000000000000:";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum RequestIdentity {
    #[default]
    Anonymous,
    Aws(String),
}

impl RequestIdentity {
    pub fn aws(value: impl Into<String>) -> Result<Self, LqsError> {
        let value = value.into();
        if !valid_principal(&value) || value == "*" {
            return Err(LqsError::AccessDenied);
        }
        Ok(Self::Aws(value))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueSecurity {
    pub policy: Option<String>,
    /// Configuration simulation only; no payload encryption is performed.
    pub sqs_managed_sse_enabled: bool,
    pub kms_master_key_id: Option<String>,
    pub kms_data_key_reuse_period_seconds: u64,
}
impl Default for QueueSecurity {
    fn default() -> Self {
        Self {
            policy: None,
            sqs_managed_sse_enabled: false,
            kms_master_key_id: None,
            kms_data_key_reuse_period_seconds: 300,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SecurityUpdate {
    /// Some(None) removes the policy; None preserves it.
    pub policy: Option<Option<String>>,
    pub sqs_managed_sse_enabled: Option<bool>,
    /// Empty string disables KMS configuration.
    pub kms_master_key_id: Option<String>,
    pub kms_data_key_reuse_period_seconds: Option<u64>,
}

fn invalid(reason: &str) -> LqsError {
    LqsError::InvalidSecuritySettings(reason.into())
}

impl QueueSecurity {
    pub(crate) fn updated(&self, update: &SecurityUpdate) -> Result<Self, LqsError> {
        let mut result = self.clone();
        if let Some(policy) = &update.policy {
            result.policy = policy.clone();
        }
        if let Some(reuse) = update.kms_data_key_reuse_period_seconds {
            result.kms_data_key_reuse_period_seconds = reuse;
        }
        if let Some(key) = &update.kms_master_key_id {
            result.kms_master_key_id = (!key.is_empty()).then(|| key.clone());
            if !key.is_empty() {
                result.sqs_managed_sse_enabled = false;
            }
        }
        if let Some(enabled) = update.sqs_managed_sse_enabled {
            if enabled
                && update
                    .kms_master_key_id
                    .as_ref()
                    .is_some_and(|key| !key.is_empty())
            {
                return Err(invalid("SSE-SQS and KMS cannot both be enabled"));
            }
            result.sqs_managed_sse_enabled = enabled;
            if enabled {
                result.kms_master_key_id = None;
            }
        }
        result.validate()?;
        Ok(result)
    }

    pub(crate) fn validate(&self) -> Result<(), LqsError> {
        if !(60..=86_400).contains(&self.kms_data_key_reuse_period_seconds) {
            return Err(invalid("KmsDataKeyReusePeriodSeconds must be 60-86400"));
        }
        if let Some(key) = &self.kms_master_key_id {
            if key.is_empty() || key.len() > 2048 || !key.bytes().all(|b| b.is_ascii_graphic()) {
                return Err(invalid(
                    "KmsMasterKeyId must be 1-2048 printable ASCII bytes",
                ));
            }
            if self.sqs_managed_sse_enabled {
                return Err(invalid("SSE-SQS and KMS cannot both be enabled"));
            }
        }
        if let Some(policy) = &self.policy {
            parse_policy(policy)?;
        }
        Ok(())
    }

    /// No policy preserves the historical local-open mode. A present policy
    /// uses default deny, with any matching explicit deny overriding allow.
    pub fn authorize(
        &self,
        identity: &RequestIdentity,
        action: &str,
        resource: &str,
    ) -> Result<(), LqsError> {
        let Some(policy) = &self.policy else {
            return Ok(());
        };
        let policy = parse_policy(policy)?;
        let action = format!("sqs:{}", permission_action(action)).to_ascii_lowercase();
        let mut allowed = false;
        for statement in statements(&policy)? {
            let principal = &statement["Principal"];
            let principal = principal.get("AWS").unwrap_or(principal);
            if !strings(principal)?
                .iter()
                .any(|value| principal_matches(value, identity))
                || !strings(&statement["Action"])?
                    .iter()
                    .any(|pattern| glob(&pattern.to_ascii_lowercase(), &action))
                || !strings(&statement["Resource"])?
                    .iter()
                    .any(|pattern| glob(pattern, resource))
            {
                continue;
            }
            if statement["Effect"] == "Deny" {
                return Err(LqsError::AccessDenied);
            }
            allowed = true;
        }
        if allowed {
            Ok(())
        } else {
            Err(LqsError::AccessDenied)
        }
    }
}

pub(crate) fn permission_action(action: &str) -> &str {
    match action {
        "SendMessageBatch" => "SendMessage",
        "DeleteMessageBatch" => "DeleteMessage",
        "ChangeMessageVisibilityBatch" => "ChangeMessageVisibility",
        _ => action,
    }
}

const ACTIONS: &[&str] = &[
    "AddPermission",
    "RemovePermission",
    "CreateQueue",
    "DeleteQueue",
    "GetQueueAttributes",
    "GetQueueUrl",
    "ListDeadLetterSourceQueues",
    "ListQueues",
    "ListQueueTags",
    "PurgeQueue",
    "ReceiveMessage",
    "SendMessage",
    "DeleteMessage",
    "ChangeMessageVisibility",
    "SetQueueAttributes",
    "TagQueue",
    "UntagQueue",
    "StartMessageMoveTask",
    "CancelMessageMoveTask",
    "ListMessageMoveTasks",
];

fn valid_principal(value: &str) -> bool {
    if value == "*" || account(value) {
        return true;
    }
    let parts: Vec<_> = value.splitn(6, ':').collect();
    parts.len() == 6
        && parts[0] == "arn"
        && parts[1] == "aws"
        && parts[2] == "iam"
        && parts[3].is_empty()
        && account(parts[4])
        && (parts[5] == "root"
            || parts[5].starts_with("user/") && parts[5].len() > 5
            || parts[5].starts_with("role/") && parts[5].len() > 5)
        && value.len() <= 2048
        && value
            .bytes()
            .all(|b| b.is_ascii_graphic() && b != b'*' && b != b'?')
}
fn account(value: &str) -> bool {
    value.len() == 12 && value.bytes().all(|b| b.is_ascii_digit())
}

fn principal_matches(principal: &str, identity: &RequestIdentity) -> bool {
    if principal == "*" {
        return true;
    }
    let RequestIdentity::Aws(identity) = identity else {
        return false;
    };
    if principal == identity {
        return true;
    }
    let identity_account = if account(identity) {
        identity.as_str()
    } else {
        identity.split(':').nth(4).unwrap_or("")
    };
    account(principal) && principal == identity_account
        || principal == format!("arn:aws:iam::{identity_account}:root")
}

// Bounded wildcard matching; never interpret policy strings as regexes.
fn glob(pattern: &str, value: &str) -> bool {
    let (p, v) = (pattern.as_bytes(), value.as_bytes());
    let (mut i, mut j, mut star, mut retry) = (0, 0, None, 0);
    while j < v.len() {
        if i < p.len() && (p[i] == b'?' || p[i] == v[j]) {
            i += 1;
            j += 1;
        } else if i < p.len() && p[i] == b'*' {
            star = Some(i);
            i += 1;
            retry = j;
        } else if let Some(start) = star {
            retry += 1;
            j = retry;
            i = start + 1;
        } else {
            return false;
        }
    }
    while i < p.len() && p[i] == b'*' {
        i += 1;
    }
    i == p.len()
}

fn strings(value: &Value) -> Result<Vec<&str>, LqsError> {
    match value {
        Value::String(value) if !value.is_empty() => Ok(vec![value]),
        Value::Array(values) if !values.is_empty() => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| invalid("policy lists must contain nonempty strings"))
            })
            .collect(),
        _ => Err(invalid(
            "policy fields must be a nonempty string or string list",
        )),
    }
}
fn statements(policy: &Value) -> Result<Vec<&Value>, LqsError> {
    match &policy["Statement"] {
        Value::Array(values) => Ok(values.iter().collect()),
        Value::Object(_) => Ok(vec![&policy["Statement"]]),
        _ => Err(invalid("Policy.Statement must be an object or array")),
    }
}

pub(crate) fn parse_policy(encoded: &str) -> Result<Value, LqsError> {
    if encoded.len() > 8192 {
        return Err(invalid("Policy is limited to 8192 bytes"));
    }
    let policy: Value =
        serde_json::from_str(encoded).map_err(|_| invalid("Policy must be JSON"))?;
    let root = policy
        .as_object()
        .ok_or_else(|| invalid("Policy must be an object"))?;
    if root
        .keys()
        .any(|key| !["Version", "Id", "Statement"].contains(&key.as_str()))
        || root
            .get("Version")
            .is_some_and(|v| v != "2012-10-17" && v != "2008-10-17")
        || root.get("Id").is_some_and(|v| !v.is_string())
    {
        return Err(invalid("unsupported policy field or version"));
    }
    let items = statements(&policy)?;
    if items.len() > 20 {
        return Err(invalid("Policy is limited to 20 statements"));
    }
    let mut ids = std::collections::HashSet::new();
    let mut principals = 0;
    for item in items {
        let item = item
            .as_object()
            .ok_or_else(|| invalid("statements must be objects"))?;
        if item.keys().any(|key| {
            !["Sid", "Effect", "Principal", "Action", "Resource"].contains(&key.as_str())
        }) {
            return Err(invalid(
                "Condition, NotAction, NotResource, NotPrincipal and other unsupported fields are rejected",
            ));
        }
        if let Some(sid) = item.get("Sid") {
            let sid = sid
                .as_str()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| invalid("Sid must be a nonempty string"))?;
            if !ids.insert(sid) {
                return Err(invalid("statement Sids must be unique"));
            }
        }
        if !matches!(
            item.get("Effect").and_then(Value::as_str),
            Some("Allow" | "Deny")
        ) {
            return Err(invalid("Effect must be Allow or Deny"));
        }
        let principal = item
            .get("Principal")
            .ok_or_else(|| invalid("Principal is required"))?;
        let principal = if let Some(object) = principal.as_object() {
            if object.len() != 1 || !object.contains_key("AWS") {
                return Err(invalid("only AWS principals or wildcard are supported"));
            }
            &object["AWS"]
        } else {
            if principal != "*" {
                return Err(invalid("use Principal.AWS for account or IAM identities"));
            }
            principal
        };
        let values = strings(principal)?;
        principals += values.len();
        if principals > 50 || values.iter().any(|value| !valid_principal(value)) {
            return Err(invalid("invalid or too many AWS principals"));
        }
        let actions = strings(item.get("Action").unwrap_or(&Value::Null))?;
        if actions.len() > 7
            || actions.iter().any(|pattern| {
                !ACTIONS.iter().any(|action| {
                    glob(
                        &pattern.to_ascii_lowercase(),
                        &format!("sqs:{action}").to_ascii_lowercase(),
                    )
                })
            })
        {
            return Err(invalid(
                "Action must match supported sqs actions (maximum seven per statement)",
            ));
        }
        for resource in strings(item.get("Resource").unwrap_or(&Value::Null))? {
            if resource != "*"
                && (!resource.starts_with("arn:aws:sqs:")
                    || resource.split(':').count() != 6
                    || resource.contains("${")
                    || !resource.bytes().all(|b| b.is_ascii_graphic()))
            {
                return Err(invalid("Resource must be an SQS ARN pattern or wildcard"));
            }
        }
    }
    Ok(policy)
}

pub(crate) fn add_permission(
    existing: Option<&str>,
    queue: &str,
    label: &str,
    accounts: &[String],
    actions: &[String],
) -> Result<String, LqsError> {
    validate_label(label)?;
    if accounts.is_empty()
        || accounts.iter().any(|id| !account(id))
        || actions.is_empty()
        || actions.len() > 7
        || actions
            .iter()
            .any(|a| a != "*" && !ACTIONS.contains(&a.as_str()))
    {
        return Err(invalid(
            "AddPermission requires account IDs and 1-7 supported action names",
        ));
    }
    let mut policy = match existing {
        Some(encoded) => parse_policy(encoded)?,
        None => json!({"Version":"2012-10-17", "Statement":[]}),
    };
    let mut items: Vec<_> = statements(&policy)?.into_iter().cloned().collect();
    if items.iter().any(|item| item["Sid"] == label) {
        return Err(invalid("permission Label already exists"));
    }
    items.push(json!({"Sid":label, "Effect":"Allow", "Principal":{"AWS":accounts}, "Action":actions.iter().map(|a| format!("sqs:{a}")).collect::<Vec<_>>(), "Resource":format!("{LOCAL_QUEUE_ARN_PREFIX}{queue}")}));
    policy["Statement"] = json!(items);
    let encoded = policy.to_string();
    parse_policy(&encoded)?;
    Ok(encoded)
}
pub(crate) fn remove_permission(
    existing: Option<&str>,
    label: &str,
) -> Result<Option<String>, LqsError> {
    validate_label(label)?;
    let Some(existing) = existing else {
        return Ok(None);
    };
    let mut policy = parse_policy(existing)?;
    let items: Vec<_> = statements(&policy)?
        .into_iter()
        .filter(|item| item["Sid"] != label)
        .cloned()
        .collect();
    policy["Statement"] = json!(items);
    Ok(Some(policy.to_string()))
}
fn validate_label(label: &str) -> Result<(), LqsError> {
    if !(1..=80).contains(&label.len())
        || !label
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        return Err(invalid(
            "Label must be 1-80 ASCII alphanumeric, hyphen or underscore characters",
        ));
    }
    Ok(())
}
