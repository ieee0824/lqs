use super::*;
use std::time::Duration;
use tower::ServiceExt;

async fn call(app: Router, action: &str, value: Value) -> (StatusCode, Value) {
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/")
        .header("x-amz-target", format!("AmazonSQS.{action}"))
        .body(Body::from(value.to_string()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), MAX_REQUEST_BYTES)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

fn test_app(options: QueueOptions) -> Router {
    let mut lqs = Lqs::new();
    lqs.create_queue("q", QueueType::Standard, options).unwrap();
    lqs.create_queue("other", QueueType::Standard, QueueOptions::default())
        .unwrap();
    router(lqs, "http://localhost")
}

#[tokio::test]
async fn waiting_receive_releases_lock_and_returns_on_arrival() {
    let app = test_app(QueueOptions::default());
    let pending = tokio::spawn(call(
        app.clone(),
        "ReceiveMessage",
        json!({"QueueUrl":"/q", "WaitTimeSeconds":20}),
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!pending.is_finished());
    let (status, _) = tokio::time::timeout(
        Duration::from_secs(1),
        call(
            app.clone(),
            "SendMessage",
            json!({"QueueUrl":"/q", "MessageBody":"arrived"}),
        ),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::OK);
    let (status, received) = tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(received["Messages"][0]["Body"], "arrived");
}

#[tokio::test]
async fn competing_fifo_attempt_waiters_share_the_completed_response() {
    let mut lqs = Lqs::new();
    lqs.create_queue(
        "q.fifo",
        QueueType::Fifo,
        QueueOptions {
            content_based_deduplication: true,
            ..QueueOptions::default()
        },
    )
    .unwrap();
    let app = router(lqs, "http://localhost");
    let request =
        json!({"QueueUrl":"/q.fifo", "WaitTimeSeconds":2, "ReceiveRequestAttemptId":"shared"});
    let a = tokio::spawn(call(app.clone(), "ReceiveMessage", request.clone()));
    let b = tokio::spawn(call(app.clone(), "ReceiveMessage", request));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!a.is_finished() && !b.is_finished());
    let (status, _) = call(
        app.clone(),
        "SendMessage",
        json!({"QueueUrl":"/q.fifo", "MessageBody":"arrived", "MessageGroupId":"g"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let a = tokio::time::timeout(Duration::from_secs(1), a)
        .await
        .unwrap()
        .unwrap();
    let b = tokio::time::timeout(Duration::from_secs(1), b)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.0, StatusCode::OK);
    assert_eq!(a, b);
    assert_eq!(a.1["Messages"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn empty_long_poll_waits_until_its_original_deadline() {
    let app = test_app(QueueOptions {
        receive_wait_time_ms: 1000,
        ..QueueOptions::default()
    });
    let start = tokio::time::Instant::now();
    let pending = tokio::spawn(call(
        app.clone(),
        "ReceiveMessage",
        json!({"QueueUrl":"/q"}),
    ));
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let (status, _) = call(
            app.clone(),
            "SendMessage",
            json!({"QueueUrl":"/other", "MessageBody":"unrelated"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(!pending.is_finished());
    }
    // A setting update must not extend an already-running request's timeout.
    call(
        app.clone(),
        "SetQueueAttributes",
        json!({"QueueUrl":"/q", "Attributes":{"ReceiveMessageWaitTimeSeconds":"20"}}),
    )
    .await;
    let (status, value) = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status, StatusCode::OK);
    assert!(value["Messages"].as_array().unwrap().is_empty());
    assert!(start.elapsed() >= Duration::from_secs(1));
    assert!(start.elapsed() < Duration::from_secs(2));
    let start = tokio::time::Instant::now();
    let (status, value) = call(
        app,
        "ReceiveMessage",
        json!({"QueueUrl":"/q", "WaitTimeSeconds":0}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(value["Messages"].as_array().unwrap().is_empty());
    assert!(start.elapsed() < Duration::from_millis(500));
}

#[tokio::test]
async fn delay_and_visibility_expiry_are_observed_without_notifications() {
    let app = test_app(QueueOptions {
        delay_ms: 200,
        visibility_timeout_ms: 200,
        ..QueueOptions::default()
    });
    call(
        app.clone(),
        "SendMessage",
        json!({"QueueUrl":"/q", "MessageBody":"timer"}),
    )
    .await;
    for _ in 0..2 {
        let start = tokio::time::Instant::now();
        let (status, value) = call(
            app.clone(),
            "ReceiveMessage",
            json!({"QueueUrl":"/q", "WaitTimeSeconds":1}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["Messages"][0]["Body"], "timer");
        assert!(start.elapsed() >= Duration::from_millis(150));
        assert!(start.elapsed() < Duration::from_secs(1));
    }
}

#[tokio::test]
async fn quota_waits_for_delete_and_cancellation_leaves_no_receiver() {
    let app = test_app(QueueOptions {
        max_in_flight: 1,
        ..QueueOptions::default()
    });
    for body in ["one", "two"] {
        call(
            app.clone(),
            "SendMessage",
            json!({"QueueUrl":"/q", "MessageBody":body}),
        )
        .await;
    }
    let (_, first) = call(app.clone(), "ReceiveMessage", json!({"QueueUrl":"/q"})).await;
    let (status, value) = call(app.clone(), "ReceiveMessage", json!({"QueueUrl":"/q"})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["__type"], "com.amazonaws.sqs#OverLimit");
    let pending = tokio::spawn(call(
        app.clone(),
        "ReceiveMessage",
        json!({"QueueUrl":"/q", "WaitTimeSeconds":20}),
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!pending.is_finished());
    let (status, _) = call(
        app.clone(),
        "DeleteMessage",
        json!({"QueueUrl":"/q", "ReceiptHandle":first["Messages"][0]["ReceiptHandle"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, second) = tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second["Messages"][0]["Body"], "two");
    let pending = tokio::spawn(call(
        app.clone(),
        "ReceiveMessage",
        json!({"QueueUrl":"/other", "WaitTimeSeconds":20}),
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!pending.is_finished());
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    call(
        app.clone(),
        "SendMessage",
        json!({"QueueUrl":"/other", "MessageBody":"not-stolen"}),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let (_, value) = call(app, "ReceiveMessage", json!({"QueueUrl":"/other"})).await;
    assert_eq!(value["Messages"][0]["Body"], "not-stolen");
}

#[tokio::test]
async fn invalid_receive_parameters_fail_without_waiting_or_claiming() {
    let app = test_app(QueueOptions::default());
    call(
        app.clone(),
        "SendMessage",
        json!({"QueueUrl":"/q", "MessageBody":"keep"}),
    )
    .await;
    for params in [
        json!({"WaitTimeSeconds":21}),
        json!({"WaitTimeSeconds":-1}),
        json!({"WaitTimeSeconds":0.5}),
        json!({"WaitTimeSeconds":"1"}),
        json!({"MaxNumberOfMessages":0}),
        json!({"MaxNumberOfMessages":11}),
    ] {
        let mut value = params;
        value["QueueUrl"] = json!("/q");
        let (status, _) = tokio::time::timeout(
            Duration::from_millis(500),
            call(app.clone(), "ReceiveMessage", value),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    let (_, value) = call(
        app,
        "ReceiveMessage",
        json!({"QueueUrl":"/q", "WaitTimeSeconds":20}),
    )
    .await;
    assert_eq!(value["Messages"][0]["Body"], "keep");
}

#[tokio::test]
async fn competing_waiters_do_not_duplicate_or_return_empty_early() {
    let app = test_app(QueueOptions::default());
    let mut first = tokio::spawn(call(
        app.clone(),
        "ReceiveMessage",
        json!({"QueueUrl":"/q", "WaitTimeSeconds":20}),
    ));
    let mut second = tokio::spawn(call(
        app.clone(),
        "ReceiveMessage",
        json!({"QueueUrl":"/q", "WaitTimeSeconds":20}),
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!first.is_finished() && !second.is_finished());
    call(
        app.clone(),
        "SendMessage",
        json!({"QueueUrl":"/q", "MessageBody":"first"}),
    )
    .await;
    let (value, remaining) = tokio::select! {
        result = &mut first => (result.unwrap().1, second),
        result = &mut second => (result.unwrap().1, first),
        _ = tokio::time::sleep(Duration::from_secs(1)) => panic!("neither waiter received the message"),
    };
    assert_eq!(value["Messages"][0]["Body"], "first");
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!remaining.is_finished());
    call(
        app.clone(),
        "SendMessage",
        json!({"QueueUrl":"/q", "MessageBody":"second"}),
    )
    .await;
    let (_, value) = tokio::time::timeout(Duration::from_secs(1), remaining)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value["Messages"][0]["Body"], "second");
}

#[tokio::test]
async fn external_database_writes_and_quota_updates_wake_pollers() {
    let path = std::env::temp_dir().join(format!(
        "lqs-external-poll-{}-{}.sqlite",
        std::process::id(),
        unix_time_ms()
    ));
    let mut external = Lqs::open(&path).unwrap();
    external
        .create_queue(
            "q",
            QueueType::Standard,
            QueueOptions {
                max_in_flight: 1,
                ..QueueOptions::default()
            },
        )
        .unwrap();
    let app = router(Lqs::open(&path).unwrap(), "http://localhost");
    let pending = tokio::spawn(call(
        app.clone(),
        "ReceiveMessage",
        json!({"QueueUrl":"/q", "WaitTimeSeconds":20}),
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!pending.is_finished());
    external
        .send("q", SendRequest::standard("external"), unix_time_ms())
        .unwrap();
    let (_, value) = tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value["Messages"][0]["Body"], "external");
    external
        .send("q", SendRequest::standard("quota-change"), unix_time_ms())
        .unwrap();
    let pending = tokio::spawn(call(
        app.clone(),
        "ReceiveMessage",
        json!({"QueueUrl":"/q", "WaitTimeSeconds":20}),
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!pending.is_finished());
    external
        .set_queue_attributes(
            "q",
            QueueUpdate {
                max_in_flight: Some(2),
                ..QueueUpdate::default()
            },
            unix_time_ms(),
        )
        .unwrap();
    let (_, value) = tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value["Messages"][0]["Body"], "quota-change");
    assert_eq!(external.in_flight_count("q", unix_time_ms()).unwrap(), 2);
    drop(app);
    drop(external);
    std::fs::remove_file(path).unwrap();
}
