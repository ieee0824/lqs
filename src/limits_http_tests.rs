use super::*;
use futures_util::future::join_all;
use futures_util::stream;
use tower::ServiceExt;

fn limited_app(limits: HttpLimits, hook: Arc<AuthorizationHook>) -> Router {
    router_with_authorization_and_limits(Lqs::new(), "http://localhost", hook, limits)
}

fn json_request(action: &str, value: Value) -> Request {
    axum::http::Request::builder()
        .method("POST")
        .uri("/")
        .header("x-amz-target", format!("AmazonSQS.{action}"))
        .body(Body::from(value.to_string()))
        .unwrap()
}

async fn status(app: Router, request: Request) -> StatusCode {
    app.oneshot(request).await.unwrap().status()
}

#[tokio::test]
async fn slow_body_times_out_and_releases_processing_slot() {
    let app = limited_app(
        HttpLimits {
            max_in_flight: 1,
            requests_per_second: 100,
            body_read_timeout: Duration::from_millis(30),
        },
        security_http::local_hook(false),
    );
    let pending = stream::pending::<Result<Bytes, std::io::Error>>();
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/")
        .header("x-amz-target", "AmazonSQS.ListQueues")
        .body(Body::from_stream(pending))
        .unwrap();
    assert_eq!(
        status(app.clone(), request).await,
        StatusCode::REQUEST_TIMEOUT
    );
    assert_eq!(
        status(app, json_request("ListQueues", json!({}))).await,
        StatusCode::OK
    );
}

#[tokio::test]
async fn body_flood_is_bounded_and_recovers_after_deadlines() {
    let app = limited_app(
        HttpLimits {
            max_in_flight: 2,
            requests_per_second: 100,
            body_read_timeout: Duration::from_millis(80),
        },
        security_http::local_hook(false),
    );
    let responses = join_all((0..32).map(|_| {
        let app = app.clone();
        async move {
            let request = axum::http::Request::builder()
                .method("POST")
                .uri("/")
                .header("x-amz-target", "AmazonSQS.ListQueues")
                .body(Body::from_stream(stream::pending::<
                    Result<Bytes, std::io::Error>,
                >()))
                .unwrap();
            status(app, request).await
        }
    }))
    .await;
    assert_eq!(
        responses
            .iter()
            .filter(|status| **status == StatusCode::REQUEST_TIMEOUT)
            .count(),
        2
    );
    assert_eq!(
        responses
            .iter()
            .filter(|status| **status == StatusCode::SERVICE_UNAVAILABLE)
            .count(),
        30
    );
    assert_eq!(
        status(app, json_request("ListQueues", json!({}))).await,
        StatusCode::OK
    );
}

#[tokio::test]
async fn concurrent_limit_rejects_then_recovers_after_cancellation() {
    let app = limited_app(
        HttpLimits {
            max_in_flight: 1,
            requests_per_second: 100,
            body_read_timeout: Duration::from_secs(1),
        },
        security_http::local_hook(false),
    );
    assert_eq!(
        status(
            app.clone(),
            json_request("CreateQueue", json!({"QueueName":"q"}))
        )
        .await,
        StatusCode::OK
    );
    let waiting = tokio::spawn(status(
        app.clone(),
        json_request(
            "ReceiveMessage",
            json!({"QueueUrl":"http://localhost/000000000000/q","WaitTimeSeconds":20}),
        ),
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished());
    assert_eq!(
        status(app.clone(), json_request("ListQueues", json!({}))).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    waiting.abort();
    let _ = waiting.await;
    assert_eq!(
        status(app, json_request("ListQueues", json!({}))).await,
        StatusCode::OK
    );
}

#[tokio::test]
async fn rate_limit_precedes_authentication_and_recovers() {
    let credential = "1234567890abcdefghijklmnopqrstuv";
    let app = limited_app(
        HttpLimits {
            max_in_flight: 2,
            requests_per_second: 1,
            body_read_timeout: Duration::from_secs(1),
        },
        security_http::bearer_hook(credential, "111111111111"),
    );
    assert_eq!(
        status(app.clone(), json_request("ListQueues", json!({}))).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        status(app.clone(), json_request("ListQueues", json!({}))).await,
        StatusCode::TOO_MANY_REQUESTS
    );
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let mut authorized = json_request("ListQueues", json!({}));
    authorized.headers_mut().insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {credential}")).unwrap(),
    );
    assert_eq!(status(app, authorized).await, StatusCode::OK);
}

#[tokio::test]
async fn twenty_second_long_poll_can_finish_after_body_deadline() {
    let app = limited_app(
        HttpLimits {
            max_in_flight: 2,
            requests_per_second: 100,
            body_read_timeout: Duration::from_millis(30),
        },
        security_http::local_hook(false),
    );
    assert_eq!(
        status(
            app.clone(),
            json_request("CreateQueue", json!({"QueueName":"q"}))
        )
        .await,
        StatusCode::OK
    );
    let waiting = tokio::spawn(status(
        app.clone(),
        json_request(
            "ReceiveMessage",
            json!({"QueueUrl":"http://localhost/000000000000/q","WaitTimeSeconds":20}),
        ),
    ));
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(!waiting.is_finished());
    assert_eq!(
        status(
            app,
            json_request(
                "SendMessage",
                json!({"QueueUrl":"http://localhost/000000000000/q","MessageBody":"ready"})
            )
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap(),
        StatusCode::OK
    );
}
