use super::*;
use crate::{MessageAttribute, MessageAttributeValue, MessageAttributes, message_attributes_md5};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::collections::BTreeMap;

pub(super) fn parse_attributes(request: &WireRequest) -> Result<MessageAttributes, ApiError> {
    let invalid = |reason: &str| ApiError::invalid_parameter("MessageAttributes", reason);
    let entries = if let Some(json) = &request.json {
        match json.get("MessageAttributes") {
            None => return Ok(MessageAttributes::new()),
            Some(Value::Object(entries)) => entries.clone(),
            _ => return Err(invalid("must be a map")),
        }
    } else {
        let mut indexed = BTreeMap::<usize, BTreeMap<String, String>>::new();
        for (key, value) in &request.query {
            let Some(rest) = key.strip_prefix("MessageAttribute.") else {
                continue;
            };
            let (index, field) = rest
                .split_once('.')
                .ok_or_else(|| invalid("invalid attribute index"))?;
            let number = index
                .parse::<usize>()
                .map_err(|_| invalid("invalid attribute index"))?;
            if number == 0 || index != number.to_string() {
                return Err(invalid("invalid attribute index"));
            }
            indexed
                .entry(number)
                .or_default()
                .insert(field.to_owned(), value.clone());
        }
        let mut entries = serde_json::Map::new();
        for fields in indexed.into_values() {
            let name = fields
                .get("Name")
                .ok_or_else(|| invalid("missing attribute name"))?;
            let mut value = serde_json::Map::new();
            for (field, content) in &fields {
                if field == "Name" {
                    continue;
                }
                let field = field
                    .strip_prefix("Value.")
                    .ok_or_else(|| invalid("invalid attribute field"))?;
                if !["DataType", "StringValue", "BinaryValue"].contains(&field) {
                    return Err(invalid("unsupported attribute field"));
                }
                value.insert(field.into(), json!(content));
            }
            if entries.insert(name.clone(), Value::Object(value)).is_some() {
                return Err(invalid("duplicate attribute name"));
            }
        }
        entries
    };
    let mut attributes = MessageAttributes::new();
    for (name, value) in entries {
        let object = value
            .as_object()
            .ok_or_else(|| invalid("attribute must be an object"))?;
        for field in ["StringListValues", "BinaryListValues"] {
            if let Some(value) = object.get(field)
                && value.as_array().is_none_or(|values| !values.is_empty())
            {
                return Err(invalid("list values are not supported"));
            }
        }
        let data_type = object
            .get("DataType")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("DataType must be a string"))?
            .to_owned();
        let value = match (object.get("StringValue"), object.get("BinaryValue")) {
            (Some(Value::String(text)), None) => MessageAttributeValue::String(text.clone()),
            (None, Some(Value::String(encoded))) => MessageAttributeValue::Binary(
                STANDARD
                    .decode(encoded)
                    .map_err(|_| invalid("BinaryValue must be valid base64"))?,
            ),
            _ => return Err(invalid("specify exactly one StringValue or BinaryValue")),
        };
        attributes.insert(name, MessageAttribute { data_type, value });
    }
    Ok(attributes)
}

fn string_list(
    request: &WireRequest,
    json_name: &str,
    query_name: &str,
) -> Result<Vec<String>, ApiError> {
    if let Some(json) = &request.json {
        return match json.get(json_name) {
            None => Ok(Vec::new()),
            Some(Value::Array(values)) => values
                .iter()
                .map(|value| {
                    value.as_str().map(str::to_owned).ok_or_else(|| {
                        ApiError::invalid_parameter(json_name, "must contain strings")
                    })
                })
                .collect(),
            _ => Err(ApiError::invalid_parameter(json_name, "must be an array")),
        };
    }
    let prefix = format!("{query_name}.");
    Ok(request
        .query
        .iter()
        .filter(|(key, _)| *key == query_name || key.starts_with(&prefix))
        .map(|(_, value)| value.clone())
        .collect())
}

pub(super) struct Selection {
    message: Vec<String>,
    system: Vec<String>,
}

impl Selection {
    pub fn parse(request: &WireRequest) -> Result<Self, ApiError> {
        let message = string_list(request, "MessageAttributeNames", "MessageAttributeName")?;
        let mut system = string_list(
            request,
            "MessageSystemAttributeNames",
            "MessageSystemAttributeName",
        )?;
        system.extend(string_list(request, "AttributeNames", "AttributeName")?);
        for name in &system {
            if ![
                "All",
                "SenderId",
                "SentTimestamp",
                "ApproximateFirstReceiveTimestamp",
                "ApproximateReceiveCount",
                "SequenceNumber",
                "MessageGroupId",
                "MessageDeduplicationId",
                "DeadLetterQueueSourceArn",
                "SqsManagedSseEnabled",
                "AWSTraceHeader",
            ]
            .contains(&name.as_str())
            {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "InvalidAttributeName",
                    format!("unsupported system attribute: {name}"),
                ));
            }
        }
        Ok(Self { message, system })
    }

    pub fn apply(&self, message: &mut ReceivedMessage) {
        message.message_attributes.retain(|name, _| {
            self.message.iter().any(|selector| {
                selector == "All"
                    || selector == ".*"
                    || selector == name
                    || selector
                        .strip_suffix('*')
                        .is_some_and(|prefix| name.starts_with(prefix))
            })
        });
        message.system_attributes.retain(|name, _| {
            self.system
                .iter()
                .any(|selector| selector == "All" || selector == name)
        });
    }
}

pub(super) fn add_json_metadata(value: &mut Value, message: &ReceivedMessage) {
    if !message.system_attributes.is_empty() {
        value["Attributes"] = json!(message.system_attributes);
    }
    if let Some(digest) = message_attributes_md5(&message.message_attributes) {
        value["MD5OfMessageAttributes"] = json!(digest);
        let attributes: serde_json::Map<String, Value> = message
            .message_attributes
            .iter()
            .map(|(name, attribute)| {
                let mut value = json!({ "DataType": attribute.data_type });
                match &attribute.value {
                    MessageAttributeValue::String(text) => value["StringValue"] = json!(text),
                    MessageAttributeValue::Binary(bytes) => {
                        value["BinaryValue"] = json!(STANDARD.encode(bytes))
                    }
                }
                (name.clone(), value)
            })
            .collect();
        value["MessageAttributes"] = Value::Object(attributes);
    }
}

pub(super) fn xml_metadata(message: &ReceivedMessage) -> String {
    let mut xml = String::new();
    for (name, value) in &message.system_attributes {
        xml.push_str(&format!(
            "<Attribute><Name>{}</Name><Value>{}</Value></Attribute>",
            xml_escape(name),
            xml_escape(value)
        ));
    }
    if let Some(digest) = message_attributes_md5(&message.message_attributes) {
        xml.push_str(&format!(
            "<MD5OfMessageAttributes>{digest}</MD5OfMessageAttributes>"
        ));
        for (name, attribute) in &message.message_attributes {
            let value = match &attribute.value {
                MessageAttributeValue::String(text) => {
                    format!("<StringValue>{}</StringValue>", xml_escape(text))
                }
                MessageAttributeValue::Binary(bytes) => {
                    format!("<BinaryValue>{}</BinaryValue>", STANDARD.encode(bytes))
                }
            };
            xml.push_str(&format!("<MessageAttribute><Name>{}</Name><Value><DataType>{}</DataType>{value}</Value></MessageAttribute>", xml_escape(name), xml_escape(&attribute.data_type)));
        }
    }
    xml
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    fn wire(attributes: Value) -> WireRequest {
        WireRequest {
            protocol: Protocol::Json,
            action: "SendMessage".into(),
            json: Some(json!({"MessageAttributes":attributes})),
            query: HashMap::new(),
        }
    }

    #[test]
    fn malformed_wire_attributes_are_not_silently_dropped() {
        for attributes in [
            json!([]),
            json!({"a":null}),
            json!({"a":{"DataType":42,"StringValue":"v"}}),
            json!({"a":{"DataType":"Binary","BinaryValue":"invalid!"}}),
            json!({"a":{"DataType":"String","StringValue":"v","BinaryValue":"AA=="}}),
            json!({"a":{"DataType":"String","StringValue":false}}),
            json!({"a":{"DataType":"String","StringListValues":["v"]}}),
        ] {
            assert!(parse_attributes(&wire(attributes)).is_err());
        }
        let query = "Action=SendMessage&MessageAttribute.1.Name=same&MessageAttribute.1.Value.DataType=String&MessageAttribute.1.Value.StringValue=a&MessageAttribute.2.Name=same&MessageAttribute.2.Value.DataType=String&MessageAttribute.2.Value.StringValue=b";
        let request = WireRequest::parse(&HeaderMap::new(), Bytes::from(query)).unwrap();
        assert!(parse_attributes(&request).is_err());
    }

    #[tokio::test]
    async fn malformed_selection_fails_before_receiving_and_batch_reports_attribute_errors() {
        let mut lqs = Lqs::new();
        lqs.create_queue("q", QueueType::Standard, QueueOptions::default())
            .unwrap();
        lqs.send("q", SendRequest::standard("keep"), unix_time_ms())
            .unwrap();
        let app = router(lqs, "http://localhost");
        for input in [
            json!({"MessageAttributeNames":[1]}),
            json!({"MessageSystemAttributeNames":"All"}),
            json!({"MessageSystemAttributeNames":["Unknown"]}),
        ] {
            let mut input = input;
            input["QueueUrl"] = json!("/q");
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .method("POST")
                        .header("x-amz-target", "AmazonSQS.ReceiveMessage")
                        .body(Body::from(input.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .header("x-amz-target", "AmazonSQS.ReceiveMessage")
                    .body(Body::from(json!({"QueueUrl":"/q"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(
            &to_bytes(response.into_body(), MAX_REQUEST_BYTES)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(value["Messages"][0]["Body"], "keep");
        let response = app.oneshot(axum::http::Request::builder().method("POST").header("x-amz-target","AmazonSQS.SendMessageBatch").body(Body::from(json!({"QueueUrl":"/q","Entries":[
            {"Id":"good","MessageBody":"body","MessageAttributes":{"a":{"DataType":"String","StringValue":"v"}}},
            {"Id":"bad","MessageBody":"body","MessageAttributes":{"b":{"DataType":"Binary","BinaryValue":"!!!"}}}
        ]}).to_string())).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value: Value = serde_json::from_slice(
            &to_bytes(response.into_body(), MAX_REQUEST_BYTES)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(value["Successful"][0]["Id"], "good");
        assert!(value["Successful"][0]["MD5OfMessageAttributes"].is_string());
        assert_eq!(value["Failed"][0]["Id"], "bad");
    }
}
