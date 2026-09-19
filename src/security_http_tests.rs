use super::*;
use tower::ServiceExt;

fn policy(consumer: bool) -> String {
    let mut statements = vec![
        json!({"Sid":"Admin", "Effect":"Allow", "Principal":{"AWS":"111111111111"}, "Action":"sqs:*", "Resource":"*"}),
        json!({"Sid":"Producer", "Effect":"Allow", "Principal":{"AWS":"222222222222"}, "Action":"sqs:SendMessage", "Resource":"*"}),
    ];
    if consumer {
        statements.push(json!({"Sid":"Consumer", "Effect":"Allow", "Principal":{"AWS":"333333333333"}, "Action":"sqs:ReceiveMessage", "Resource":"*"}));
    }
    json!({"Version":"2012-10-17", "Statement":statements}).to_string()
}

fn app(fifo: bool) -> Router {
    let mut lqs = Lqs::new();
    lqs.create_queue(
        if fifo { "q.fifo" } else { "q" },
        if fifo {
            QueueType::Fifo
        } else {
            QueueType::Standard
        },
        QueueOptions {
            content_based_deduplication: fifo,
            security: QueueSecurity {
                policy: Some(policy(true)),
                ..QueueSecurity::default()
            },
            ..QueueOptions::default()
        },
    )
    .unwrap();
    router_with_authorization(lqs, "http://localhost", local_hook(true))
}

async fn call(
    app: Router,
    action: &str,
    value: Value,
    identity: Option<&str>,
) -> (StatusCode, Value) {
    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri("/")
        .header("x-amz-target", format!("AmazonSQS.{action}"));
    if let Some(identity) = identity {
        request = request.header("x-lqs-principal", identity);
    }
    let response = app
        .oneshot(request.body(Body::from(value.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), MAX_REQUEST_BYTES)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn authorization_covers_single_batch_and_management_without_side_effects() {
    let app = app(false);
    let queue = "http://localhost/000000000000/q";
    for action in [
        "SendMessage",
        "ReceiveMessage",
        "SendMessageBatch",
        "DeleteMessage",
        "DeleteMessageBatch",
        "ChangeMessageVisibility",
        "ChangeMessageVisibilityBatch",
        "GetQueueAttributes",
        "TagQueue",
        "UntagQueue",
        "ListQueueTags",
        "PurgeQueue",
        "DeleteQueue",
        "SetQueueAttributes",
        "AddPermission",
        "RemovePermission",
    ] {
        let mut request = json!({"QueueUrl":queue, "MessageBody":"not sent", "ReceiptHandle":"unused", "VisibilityTimeout":0, "Tags":{"t":"v"}, "TagKeys":["t"], "Attributes":{"Policy":""}, "Label":"Grant", "AWSAccountIds":["444444444444"], "Actions":["SendMessage"]});
        request["Entries"] = match action {
            "SendMessageBatch" => json!([{"Id":"one", "MessageBody":"not sent"}]),
            _ => json!([{"Id":"one", "ReceiptHandle":"unused", "VisibilityTimeout":0}]),
        };
        let response = call(app.clone(), action, request, Some("444444444444")).await;
        assert_eq!(
            response.0,
            StatusCode::FORBIDDEN,
            "{action}: {:?}",
            response.1
        );
    }
    let sent = call(
        app.clone(),
        "SendMessageBatch",
        json!({"QueueUrl":queue, "Entries":[{"Id":"one", "MessageBody":"allowed"}]}),
        Some("222222222222"),
    )
    .await;
    assert_eq!(sent.0, StatusCode::OK);
    assert_eq!(
        call(
            app.clone(),
            "ReceiveMessage",
            json!({"QueueUrl":queue}),
            Some("222222222222")
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            app.clone(),
            "ReceiveMessage",
            json!({"QueueUrl":queue}),
            None
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let received = call(
        app.clone(),
        "ReceiveMessage",
        json!({"QueueUrl":queue, "MessageSystemAttributeNames":["All"]}),
        Some("333333333333"),
    )
    .await;
    assert_eq!(received.0, StatusCode::OK);
    assert_eq!(
        received.1["Messages"][0]["Attributes"]["ApproximateReceiveCount"],
        "1"
    );
    let receipt = received.1["Messages"][0]["ReceiptHandle"].clone();
    assert_eq!(
        call(
            app.clone(),
            "DeleteMessage",
            json!({"QueueUrl":queue, "ReceiptHandle":receipt}),
            Some("333333333333")
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let metrics = call(
        app.clone(),
        "GetQueueAttributes",
        json!({"QueueUrl":queue, "AttributeNames":["All"]}),
        Some("111111111111"),
    )
    .await;
    assert_eq!(
        metrics.1["Attributes"]["ApproximateNumberOfMessagesNotVisible"],
        "1"
    );
    assert_eq!(metrics.1["Attributes"]["Policy"], policy(true));
    assert_eq!(
        call(
            app,
            "DeleteMessageBatch",
            json!({"QueueUrl":queue, "Entries":[{"Id":"one", "ReceiptHandle":receipt}]}),
            Some("111111111111")
        )
        .await
        .0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn revocation_interrupts_long_poll_and_blocks_cached_fifo_replay() {
    let app = app(true);
    let queue = "http://localhost/000000000000/q.fifo";
    let request =
        json!({"QueueUrl":queue, "WaitTimeSeconds":20, "ReceiveRequestAttemptId":"retry"});
    let waiting = tokio::spawn(call(
        app.clone(),
        "ReceiveMessage",
        request.clone(),
        Some("333333333333"),
    ));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!waiting.is_finished());
    assert_eq!(
        call(
            app.clone(),
            "SetQueueAttributes",
            json!({"QueueUrl":queue, "Attributes":{"Policy":policy(false)}}),
            Some("111111111111")
        )
        .await
        .0,
        StatusCode::OK
    );
    let denied = tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(denied.0, StatusCode::FORBIDDEN);
    call(
        app.clone(),
        "SetQueueAttributes",
        json!({"QueueUrl":queue, "Attributes":{"Policy":policy(true)}}),
        Some("111111111111"),
    )
    .await;
    call(
        app.clone(),
        "SendMessage",
        json!({"QueueUrl":queue, "MessageBody":"body", "MessageGroupId":"g"}),
        Some("222222222222"),
    )
    .await;
    assert_eq!(
        call(
            app.clone(),
            "ReceiveMessage",
            request.clone(),
            Some("333333333333")
        )
        .await
        .0,
        StatusCode::OK
    );
    call(
        app.clone(),
        "SetQueueAttributes",
        json!({"QueueUrl":queue, "Attributes":{"Policy":policy(false)}}),
        Some("111111111111"),
    )
    .await;
    assert_eq!(
        call(app, "ReceiveMessage", request, Some("333333333333"))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn default_rejects_self_asserted_identity_and_custom_hook_receives_raw_request() {
    let default = router(Lqs::new(), "http://localhost");
    assert_eq!(
        call(
            default.clone(),
            "CreateQueue",
            json!({"QueueName":"q"}),
            Some("111111111111")
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(default, "CreateQueue", json!({"QueueName":"q"}), None)
            .await
            .0,
        StatusCode::OK
    );
    let calls = Arc::new(AtomicU64::new(0));
    let seen = calls.clone();
    let secured = router_with_authorization(
        Lqs::new(),
        "http://localhost",
        Arc::new(move |request| {
            seen.fetch_add(1, Ordering::Relaxed);
            assert_eq!(request.method, Method::POST);
            assert_eq!(request.uri.path(), "/");
            assert_eq!(request.queue_name.as_deref(), Some("q"));
            assert!(
                std::str::from_utf8(&request.body)
                    .unwrap()
                    .contains("QueueName")
            );
            // Embedders can authenticate here, or reject global operations entirely.
            Err(LqsError::AccessDenied)
        }),
    );
    assert_eq!(
        call(secured, "CreateQueue", json!({"QueueName":"q"}), None)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn explicit_deny_overrides_admin_and_permissions_cannot_be_self_elevated() {
    let app = app(false);
    let queue = "http://localhost/000000000000/q";
    assert_eq!(call(app.clone(), "AddPermission", json!({"QueueUrl":queue,"Label":"Read","AWSAccountIds":["444444444444"],"Actions":["ReceiveMessage"]}), Some("111111111111")).await.0, StatusCode::OK);
    assert_eq!(
        call(
            app.clone(),
            "ReceiveMessage",
            json!({"QueueUrl":queue}),
            Some("444444444444")
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            app.clone(),
            "RemovePermission",
            json!({"QueueUrl":queue,"Label":"Read"}),
            Some("444444444444")
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            app.clone(),
            "RemovePermission",
            json!({"QueueUrl":queue,"Label":"Read"}),
            Some("111111111111")
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            app.clone(),
            "ReceiveMessage",
            json!({"QueueUrl":queue}),
            Some("444444444444")
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let mut deny: Value = serde_json::from_str(&policy(true)).unwrap();
    deny["Statement"].as_array_mut().unwrap().push(json!({"Effect":"Deny", "Principal":"*", "Action":["sqs:SendMessage","sqs:ReceiveMessage","sqs:DeleteMessage"], "Resource":"*"}));
    call(
        app.clone(),
        "SetQueueAttributes",
        json!({"QueueUrl":queue,"Attributes":{"Policy":deny.to_string()}}),
        Some("111111111111"),
    )
    .await;
    assert_eq!(
        call(
            app,
            "SendMessage",
            json!({"QueueUrl":queue,"MessageBody":"denied"}),
            Some("111111111111")
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
}
