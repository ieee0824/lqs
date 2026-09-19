use std::collections::BTreeMap;

use super::*;
use crate::BatchEntry;
use crate::batch::validate_batch_size;

fn entries(request: &WireRequest) -> Result<Vec<BatchEntry<WireRequest>>, ApiError> {
    let requests = if let Some(json) = &request.json {
        json.get("Entries")
            .ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "EmptyBatchRequest",
                    "batch is empty",
                )
            })?
            .as_array()
            .ok_or_else(|| ApiError::invalid_parameter("Entries", "must be an array"))?
            .iter()
            .map(|value| WireRequest {
                protocol: Protocol::Json,
                action: String::new(),
                json: Some(value.clone()),
                query: HashMap::new(),
            })
            .collect::<Vec<_>>()
    } else {
        let prefix = format!("{}RequestEntry.", request.action);
        let mut grouped = BTreeMap::<usize, HashMap<String, String>>::new();
        for (key, value) in &request.query {
            let Some(rest) = key.strip_prefix(&prefix) else {
                continue;
            };
            let (index, field) = rest
                .split_once('.')
                .ok_or_else(|| ApiError::invalid_parameter("Entries", "invalid entry index"))?;
            let number: usize = index
                .parse()
                .map_err(|_| ApiError::invalid_parameter("Entries", "invalid entry index"))?;
            if number == 0 || index != number.to_string() || field.is_empty() {
                return Err(ApiError::invalid_parameter(
                    "Entries",
                    "invalid entry index",
                ));
            }
            grouped
                .entry(number)
                .or_default()
                .insert(field.to_owned(), value.clone());
        }
        grouped
            .into_values()
            .map(|query| WireRequest {
                protocol: Protocol::Query,
                action: String::new(),
                json: None,
                query,
            })
            .collect()
    };
    Ok(requests
        .into_iter()
        .map(|request| BatchEntry {
            id: request.string("Id").unwrap_or_default().to_owned(),
            value: request,
        })
        .collect())
}

pub(super) fn execute(
    state: &AppState,
    path: &str,
    request: &WireRequest,
) -> Result<ApiSuccess, ApiError> {
    let queue = queue_name(path, request)?;
    let entries = entries(request)?;
    let action = match request.action.as_str() {
        "SendMessageBatch" => "SendMessageBatch",
        "DeleteMessageBatch" => "DeleteMessageBatch",
        _ => "ChangeMessageVisibilityBatch",
    };
    if action == "SendMessageBatch" {
        validate_batch_size(entries.iter().map(|entry| {
            let body = entry.value.string("MessageBody").unwrap_or_default();
            attributes_http::parse_attributes(&entry.value)
                .map(|attributes| crate::message_attributes::payload_size(body, &attributes))
                .unwrap_or(body.len())
        }))?;
    }
    let now = unix_time_ms();
    let mut lqs = lock_lqs(state)?;
    let result = lqs.run_batch(&queue, entries, |lqs, entry| match action {
        "SendMessageBatch" => {
            let request = parse_send_request(&entry)?;
            let digest = md5_hex(&request.body);
            let fifo = lqs.queue_config(&queue)?.queue_type == QueueType::Fifo;
            let sent = lqs.send(&queue, request, now)?;
            let mut value = json!({ "MessageId": sent.message_id, "MD5OfMessageBody": digest });
            if let Some(digest) = sent.md5_of_message_attributes {
                value["MD5OfMessageAttributes"] = json!(digest);
            }
            if fifo {
                value["SequenceNumber"] = json!(sequence_number(&sent.message_id));
            }
            Ok(value)
        }
        "DeleteMessageBatch" => {
            lqs.delete(&queue, entry.required_string("ReceiptHandle")?)?;
            Ok(json!({}))
        }
        _ => {
            let handle = entry.required_string("ReceiptHandle")?;
            let seconds = entry
                .unsigned("VisibilityTimeout")?
                .ok_or_else(|| ApiError::missing("VisibilityTimeout"))?;
            if seconds > 43_200 {
                return Err(ApiError::invalid_parameter(
                    "VisibilityTimeout",
                    "must be between 0 and 43200",
                ));
            }
            lqs.change_visibility(&queue, handle, seconds * 1000, now)?;
            Ok(json!({}))
        }
    })?;
    Ok(ApiSuccess::Batch { action, result })
}

pub(super) fn json_result(result: &BatchResult<Value, ApiError>) -> Value {
    json!({
        "Successful": result.successful.iter().map(|entry| {
            let mut value = entry.value.clone();
            value["Id"] = json!(entry.id);
            value
        }).collect::<Vec<_>>(),
        "Failed": result.failed.iter().map(|entry| json!({
            "Id": entry.id, "Code": entry.value.code, "Message": entry.value.message,
            "SenderFault": entry.value.status.is_client_error(),
        })).collect::<Vec<_>>()
    })
}

pub(super) fn xml_result(action: &str, result: &BatchResult<Value, ApiError>) -> String {
    let value = json_result(result);
    let mut xml = String::new();
    for (name, tag) in [
        ("Successful", format!("{action}ResultEntry")),
        ("Failed", "BatchResultErrorEntry".to_owned()),
    ] {
        for entry in value[name].as_array().expect("batch result array") {
            xml.push_str(&format!("<{tag}>"));
            for (key, value) in entry.as_object().expect("batch entry object") {
                let text = value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string());
                xml.push_str(&format!("<{key}>{}</{key}>", xml_escape(&text)));
            }
            xml.push_str(&format!("</{tag}>"));
        }
    }
    xml
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    async fn post(app: &Router, action: &str, entries: Value) -> (StatusCode, Value) {
        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/")
            .header("x-amz-target", format!("AmazonSQS.{action}"))
            .body(Body::from(
                json!({"QueueUrl":"http://localhost/q", "Entries":entries}).to_string(),
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), MAX_REQUEST_BYTES)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn malformed_entry_parameters_are_individual_failures() {
        let mut lqs = Lqs::new();
        lqs.create_queue("q", QueueType::Standard, QueueOptions::default())
            .unwrap();
        let app = router(lqs, "http://localhost");
        let (status, value) = post(
            &app,
            "SendMessageBatch",
            json!([
                {"Id":"ok", "MessageBody":"body"},
                {"Id":"missing"},
                {"Id":"negative", "MessageBody":"bad", "DelaySeconds":-1},
                {"Id":"fraction", "MessageBody":"bad", "DelaySeconds":0.5},
            {"Id":"overflow", "MessageBody":"bad", "DelaySeconds":u64::MAX},
            {"Id":"group", "MessageBody":"bad", "MessageGroupId":123},
            {"Id":"key", "MessageBody":"bad", "MessageDeduplicationId":false}
            ]),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["Successful"].as_array().unwrap().len(), 1);
        assert_eq!(value["Failed"].as_array().unwrap().len(), 6);
        assert_eq!(value["Failed"][0]["Code"], "MissingParameter");
        for failure in value["Failed"].as_array().unwrap() {
            assert_eq!(failure["SenderFault"], true);
        }
        for (action, entries) in [
            (
                "DeleteMessageBatch",
                json!([{"Id":"missing"}, {"Id":"bad", "ReceiptHandle":false}]),
            ),
            (
                "ChangeMessageVisibilityBatch",
                json!([{"Id":"missing", "ReceiptHandle":"h"}, {"Id":"fraction", "ReceiptHandle":"h", "VisibilityTimeout":0.5}]),
            ),
        ] {
            let (status, value) = post(&app, action, entries).await;
            assert_eq!(status, StatusCode::OK);
            assert!(value["Successful"].as_array().unwrap().is_empty());
            assert_eq!(value["Failed"].as_array().unwrap().len(), 2);
        }
    }

    #[tokio::test]
    async fn malformed_envelopes_cannot_mutate_valid_entries() {
        let mut lqs = Lqs::new();
        lqs.create_queue("q", QueueType::Standard, QueueOptions::default())
            .unwrap();
        lqs.send("q", SendRequest::standard("keep"), unix_time_ms())
            .unwrap();
        let handle = lqs.receive("q", 1, unix_time_ms()).unwrap()[0]
            .receipt_handle
            .clone();
        let app = router(lqs, "http://localhost");
        for action in [
            "SendMessageBatch",
            "DeleteMessageBatch",
            "ChangeMessageVisibilityBatch",
        ] {
            for invalid in [
                json!({"Id":"bad id"}),
                json!({"Id":"ok"}),
                json!({"Id":123}),
                json!({}),
                json!(null),
            ] {
                let (status, _) = post(&app, action, json!([
                    {"Id":"ok", "MessageBody":"unexpected", "ReceiptHandle":handle, "VisibilityTimeout":0}, invalid
                ])).await;
                assert_eq!(status, StatusCode::BAD_REQUEST);
            }
            let (status, _) = post(&app, action, json!({})).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
        }
        let (status, received) = post(&app, "ReceiveMessage", json!([])).await;
        assert_eq!(status, StatusCode::OK);
        assert!(received["Messages"].as_array().unwrap().is_empty());
        let (status, deleted) = post(
            &app,
            "DeleteMessageBatch",
            json!([{"Id":"still-present", "ReceiptHandle":handle}]),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(deleted["Successful"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn query_envelopes_and_server_faults_are_encoded_correctly() {
        for query in [
            "Action=SendMessageBatch&SendMessageBatchRequestEntry.zero.Id=a",
            "Action=SendMessageBatch&SendMessageBatchRequestEntry.01.Id=a",
            "Action=SendMessageBatch&SendMessageBatchRequestEntry.0.Id=a",
        ] {
            let request = WireRequest::parse(&HeaderMap::new(), Bytes::from(query)).unwrap();
            assert!(entries(&request).is_err());
        }
        let error = ApiError::from(LqsError::Database("storage <failure>".into()));
        let result = BatchResult {
            successful: vec![],
            failed: vec![BatchEntry {
                id: "bad".into(),
                value: error,
            }],
        };
        assert_eq!(json_result(&result)["Failed"][0]["SenderFault"], false);
        assert_eq!(json_result(&result)["Failed"][0]["Code"], "InternalError");
        let xml = xml_result("SendMessageBatch", &result);
        assert!(xml.contains("<SenderFault>false</SenderFault>"));
        assert!(xml.contains("storage &lt;failure&gt;"));
    }
}
