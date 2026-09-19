use super::*;
use crate::QueueTags;
use std::collections::BTreeMap;

fn success(action: &'static str, json: Value, xml: String) -> ApiSuccess {
    ApiSuccess::Management { action, json, xml }
}

pub(super) fn validate_attribute_names(
    attributes: &HashMap<String, String>,
    creating: bool,
) -> Result<(), ApiError> {
    for name in attributes.keys() {
        if !([
            "VisibilityTimeout",
            "DelaySeconds",
            "MessageRetentionPeriod",
            "MaximumMessageSize",
            "ReceiveMessageWaitTimeSeconds",
            "LqsMaxInFlightMessages",
            "RedrivePolicy",
            "ContentBasedDeduplication",
            "DeduplicationScope",
            "FifoThroughputLimit",
        ]
        .contains(&name.as_str())
            || creating && name == "FifoQueue")
        {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "InvalidAttributeName",
                format!("unsupported attribute: {name}"),
            ));
        }
    }
    Ok(())
}

pub(super) fn parse_tags(request: &WireRequest) -> Result<QueueTags, ApiError> {
    if let Some(json) = &request.json {
        let Some(value) = json.get("Tags").or_else(|| json.get("tags")) else {
            return Ok(QueueTags::new());
        };
        let values = value
            .as_object()
            .ok_or_else(|| ApiError::invalid_parameter("Tags", "must be a string map"))?;
        return values
            .iter()
            .map(|(key, value)| {
                let value = if value.is_null() {
                    ""
                } else {
                    value.as_str().ok_or_else(|| {
                        ApiError::invalid_parameter("Tags", "values must be strings")
                    })?
                };
                Ok((key.clone(), value.into()))
            })
            .collect();
    }
    let mut entries: BTreeMap<String, (Option<String>, Option<String>)> = BTreeMap::new();
    for (key, value) in &request.query {
        let Some(rest) = key.strip_prefix("Tag.") else {
            continue;
        };
        let (index, field) = if matches!(rest, "Key" | "Value") {
            ("1", rest)
        } else {
            rest.split_once('.')
                .ok_or_else(|| ApiError::invalid_parameter("Tags", "invalid tag entry"))?
        };
        if index
            .parse::<usize>()
            .ok()
            .filter(|n| *n > 0 && n.to_string() == index)
            .is_none()
            || !matches!(field, "Key" | "Value")
        {
            return Err(ApiError::invalid_parameter("Tags", "invalid tag entry"));
        }
        let entry = entries.entry(index.into()).or_default();
        let slot = if field == "Key" {
            &mut entry.0
        } else {
            &mut entry.1
        };
        if slot.replace(value.clone()).is_some() {
            return Err(ApiError::invalid_parameter("Tags", "duplicate tag field"));
        }
    }
    let mut tags = QueueTags::new();
    for (_, (key, value)) in entries {
        let key = key.ok_or_else(|| ApiError::missing("Tag.Key"))?;
        if tags.insert(key, value.unwrap_or_default()).is_some() {
            return Err(ApiError::invalid_parameter("Tags", "duplicate tag key"));
        }
    }
    Ok(tags)
}

fn tag_keys(request: &WireRequest) -> Result<Vec<String>, ApiError> {
    if let Some(json) = &request.json {
        return json
            .get("TagKeys")
            .ok_or_else(|| ApiError::missing("TagKeys"))?
            .as_array()
            .ok_or_else(|| ApiError::invalid_parameter("TagKeys", "must be a string list"))?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| ApiError::invalid_parameter("TagKeys", "must be a string list"))
            })
            .collect();
    }
    let mut keys = Vec::new();
    for (key, value) in &request.query {
        if key == "TagKey" {
            keys.push(value.clone());
        } else if let Some(index) = key.strip_prefix("TagKey.") {
            if index
                .parse::<usize>()
                .ok()
                .filter(|n| *n > 0 && n.to_string() == index)
                .is_none()
            {
                return Err(ApiError::invalid_parameter("TagKeys", "invalid index"));
            }
            keys.push(value.clone());
        }
    }
    Ok(keys)
}

fn destructive_queue(
    state: &AppState,
    path: &str,
    request: &WireRequest,
) -> Result<String, ApiError> {
    let source = request
        .optional_string("QueueUrl")?
        .unwrap_or(path)
        .trim_end_matches('/');
    let name = queue_name(path, request)?;
    validate_queue_name(&name)?;
    let canonical_path = format!("/000000000000/{name}");
    let canonical_url = format!("{}{canonical_path}", state.public_base_url);
    if source != canonical_url && source != canonical_path {
        return Err(ApiError::invalid_parameter(
            "QueueUrl",
            "destructive actions require this server's exact queue URL or account-qualified path",
        ));
    }
    Ok(name)
}

pub(super) fn execute(
    state: &AppState,
    path: &str,
    request: &WireRequest,
) -> Result<ApiSuccess, ApiError> {
    match request.action.as_str() {
        "ListQueues" => {
            let prefix = request.optional_string("QueueNamePrefix")?.unwrap_or("");
            let maximum = request.unsigned("MaxResults")?;
            if maximum.is_some_and(|value| !(1..=1000).contains(&value)) {
                return Err(ApiError::invalid_parameter("MaxResults", "must be 1-1000"));
            }
            let page = lock_lqs(state)?.list_queues(
                prefix,
                maximum.map(|value| value as usize),
                request.optional_string("NextToken")?,
            )?;
            let urls: Vec<_> = page
                .queue_names
                .iter()
                .map(|name| format!("{}/000000000000/{name}", state.public_base_url))
                .collect();
            let mut json = json!({"QueueUrls":urls});
            let mut xml = urls
                .iter()
                .map(|url| format!("<QueueUrl>{}</QueueUrl>", xml_escape(url)))
                .collect::<String>();
            if let Some(cursor) = page.next_cursor {
                json["NextToken"] = json!(cursor);
                xml.push_str(&format!("<NextToken>{}</NextToken>", xml_escape(&cursor)));
            }
            Ok(success("ListQueues", json, xml))
        }
        "GetQueueUrl" => {
            let name = request.required_string("QueueName")?;
            validate_queue_name(name)?;
            let owner = request.optional_string("QueueOwnerAWSAccountId")?;
            if owner.is_some_and(|id| id != "000000000000")
                || !lock_lqs(state)?.queue_exists(name)?
            {
                return Err(LqsError::QueueNotFound(name.into()).into());
            }
            let url = format!("{}/000000000000/{name}", state.public_base_url);
            Ok(success(
                "GetQueueUrl",
                json!({"QueueUrl":url}),
                format!("<QueueUrl>{}</QueueUrl>", xml_escape(&url)),
            ))
        }
        "DeleteQueue" | "PurgeQueue" => {
            let name = destructive_queue(state, path, request)?;
            let action = if request.action == "DeleteQueue" {
                lock_lqs(state)?.delete_queue(&name, unix_time_ms())?;
                "DeleteQueue"
            } else {
                lock_lqs(state)?.purge_queue(&name, unix_time_ms())?;
                "PurgeQueue"
            };
            Ok(success(action, json!({}), String::new()))
        }
        "TagQueue" => {
            let tags = parse_tags(request)?;
            lock_lqs(state)?.tag_queue(&queue_name(path, request)?, tags)?;
            Ok(success("TagQueue", json!({}), String::new()))
        }
        "UntagQueue" => {
            lock_lqs(state)?.untag_queue(&queue_name(path, request)?, &tag_keys(request)?)?;
            Ok(success("UntagQueue", json!({}), String::new()))
        }
        "ListQueueTags" => {
            let tags = lock_lqs(state)?.list_queue_tags(&queue_name(path, request)?)?;
            let xml = tags
                .iter()
                .map(|(key, value)| {
                    format!(
                        "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                        xml_escape(key),
                        xml_escape(value)
                    )
                })
                .collect();
            Ok(success("ListQueueTags", json!({"Tags":tags}), xml))
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    async fn call(app: Router, action: &str, value: Value) -> (StatusCode, Value) {
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/")
                    .header("x-amz-target", format!("AmazonSQS.{action}"))
                    .body(Body::from(value.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), MAX_REQUEST_BYTES)
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    #[tokio::test]
    async fn malformed_management_requests_cannot_partially_mutate() {
        let app = router(Lqs::new(), "http://localhost");
        for request in [
            json!({"QueueName":"bad", "Tags":{"valid":"v", "bad":3}}),
            json!({"QueueName":"bad", "Tags":{"aws:reserved":"v"}}),
            json!({"QueueName":"bad", "Attributes":{"Policy":"ignored before"}}),
            json!({"QueueName":"bad", "Attributes":{"CreatedTimestamp":"123"}}),
        ] {
            assert_eq!(
                call(app.clone(), "CreateQueue", request).await.0,
                StatusCode::BAD_REQUEST
            );
            assert_eq!(
                call(app.clone(), "GetQueueUrl", json!({"QueueName":"bad"}))
                    .await
                    .0,
                StatusCode::BAD_REQUEST
            );
        }
        assert_eq!(
            call(
                app.clone(),
                "CreateQueue",
                json!({"QueueName":"q", "Tags":{"keep":"v"}})
            )
            .await
            .0,
            StatusCode::OK
        );
        let url = "http://localhost/000000000000/q";
        for (action, request) in [
            ("TagQueue", json!({"QueueUrl":url, "Tags":null})),
            ("TagQueue", json!({"QueueUrl":url, "Tags":{}})),
            ("UntagQueue", json!({"QueueUrl":url, "TagKeys":["keep", 3]})),
            (
                "SetQueueAttributes",
                json!({"QueueUrl":url, "Attributes":{"VisibilityTimeout":"0", "FifoQueue":"true"}}),
            ),
            ("ListQueues", json!({"MaxResults":0})),
            ("ListQueues", json!({"NextToken":3})),
        ] {
            assert_eq!(
                call(app.clone(), action, request).await.0,
                StatusCode::BAD_REQUEST
            );
        }
        for action in ["DeleteQueue", "PurgeQueue"] {
            for invalid in [
                "q",
                "/q",
                "http://foreign/000000000000/q",
                "http://localhost/111111111111/q",
                "http://localhost/000000000000/q?extra=1",
            ] {
                assert_eq!(
                    call(app.clone(), action, json!({"QueueUrl":invalid}))
                        .await
                        .0,
                    StatusCode::BAD_REQUEST
                );
            }
        }
        let tags = call(app.clone(), "ListQueueTags", json!({"QueueUrl":url})).await;
        assert_eq!(tags.1["Tags"], json!({"keep":"v"}));
        let attributes = call(
            app.clone(),
            "GetQueueAttributes",
            json!({"QueueUrl":url, "AttributeNames":["VisibilityTimeout"]}),
        )
        .await;
        assert_eq!(
            attributes.1["Attributes"],
            json!({"VisibilityTimeout":"30"})
        );
    }

    #[tokio::test]
    async fn deleting_a_queue_wakes_its_pending_receiver() {
        let app = router(Lqs::new(), "http://localhost");
        call(app.clone(), "CreateQueue", json!({"QueueName":"q"})).await;
        let url = "http://localhost/000000000000/q";
        let waiting = tokio::spawn(call(
            app.clone(),
            "ReceiveMessage",
            json!({"QueueUrl":url, "WaitTimeSeconds":20}),
        ));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!waiting.is_finished());
        assert_eq!(
            call(app, "DeleteQueue", json!({"QueueUrl":url})).await.0,
            StatusCode::OK
        );
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.0, StatusCode::BAD_REQUEST);
        assert_eq!(
            result.1["__type"],
            "com.amazonaws.sqs#AWS.SimpleQueueService.NonExistentQueue"
        );
    }
}
