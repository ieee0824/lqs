use super::*;
use crate::{BatchEntry, VisibilityChange};

fn entry<T>(id: &str, value: T) -> BatchEntry<T> {
    BatchEntry {
        id: id.into(),
        value,
    }
}

#[test]
fn all_batch_envelopes_are_validated_before_any_write() {
    let mut lqs = Lqs::new();
    lqs.create_queue("q", QueueType::Standard, QueueOptions::default())
        .unwrap();
    for (ids, expected) in [
        (vec![], "EmptyBatchRequest"),
        (vec!["duplicate", "duplicate"], "BatchEntryIdsNotDistinct"),
        (vec!["ok", "bad id"], "InvalidBatchEntryId"),
        (vec![""], "InvalidBatchEntryId"),
        (vec!["a"; 11], "TooManyEntriesInBatchRequest"),
    ] {
        let sends = ids
            .iter()
            .map(|id| entry(id, SendRequest::standard("body")))
            .collect();
        assert_eq!(
            lqs.send_batch("q", sends, 0).unwrap_err(),
            LqsError::InvalidBatch(expected)
        );
        let deletes = ids
            .iter()
            .map(|id| entry(id, "handle".to_owned()))
            .collect();
        assert_eq!(
            lqs.delete_batch("q", deletes).unwrap_err(),
            LqsError::InvalidBatch(expected)
        );
        let changes = ids
            .iter()
            .map(|id| {
                entry(
                    id,
                    VisibilityChange {
                        receipt_handle: "handle".into(),
                        timeout_ms: 0,
                    },
                )
            })
            .collect();
        assert_eq!(
            lqs.change_visibility_batch("q", changes, 0).unwrap_err(),
            LqsError::InvalidBatch(expected)
        );
        assert_eq!(lqs.queue_depth("q").unwrap(), 0);
    }
    assert!(matches!(
        lqs.send_batch("missing", vec![entry("a", SendRequest::standard("x"))], 0),
        Err(LqsError::QueueNotFound(_))
    ));
    let too_large = vec![
        entry("a", SendRequest::standard("x".repeat(MAX_MESSAGE_BYTES))),
        entry("b", SendRequest::standard("x")),
    ];
    assert_eq!(
        lqs.send_batch("q", too_large, 0).unwrap_err(),
        LqsError::InvalidBatch("BatchRequestTooLong")
    );
    assert_eq!(lqs.queue_depth("q").unwrap(), 0);
    let exact = vec![
        entry(
            &"a".repeat(80),
            SendRequest::standard("é".repeat(MAX_MESSAGE_BYTES / 4)),
        ),
        entry(
            "b",
            SendRequest::standard("x".repeat(MAX_MESSAGE_BYTES / 2)),
        ),
    ];
    assert_eq!(lqs.send_batch("q", exact, 0).unwrap().successful.len(), 2);
    assert!(
        lqs.send_batch(
            "q",
            vec![entry(&"a".repeat(81), SendRequest::standard("x"))],
            0
        )
        .is_err()
    );
}

#[test]
fn send_partial_success_preserves_queue_limits_and_delay() {
    let mut lqs = Lqs::new();
    lqs.create_queue(
        "q",
        QueueType::Standard,
        QueueOptions {
            maximum_message_size: 1024,
            delay_ms: 1000,
            ..QueueOptions::default()
        },
    )
    .unwrap();
    let mut immediate = SendRequest::standard("now");
    immediate.delay_ms = Some(0);
    let result = lqs
        .send_batch(
            "q",
            vec![
                entry("a", SendRequest::standard("later")),
                entry("bad", SendRequest::standard("x".repeat(1025))),
                entry("b", immediate),
                entry("empty", SendRequest::standard("")),
            ],
            0,
        )
        .unwrap();
    assert_eq!(
        result
            .successful
            .iter()
            .map(|e| e.id.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert_eq!(result.failed.len(), 2);
    assert_eq!(lqs.receive("q", 10, 0).unwrap()[0].body, "now");
    assert_eq!(lqs.receive("q", 10, 1000).unwrap()[0].body, "later");
}

#[test]
fn fifo_batch_matches_single_send_order_deduplication_and_validation() {
    let mut lqs = Lqs::new();
    lqs.create_queue("q.fifo", QueueType::Fifo, QueueOptions::default())
        .unwrap();
    let make = |body: &str, key: &str| SendRequest {
        deduplication_id: Some(key.into()),
        ..SendRequest::fifo(body, "group")
    };
    let first = lqs.send("q.fifo", make("first", "one"), 0).unwrap();
    let mut invalid_group = make("bad", "unused");
    invalid_group.message_group_id = Some("bad group".into());
    let result = lqs
        .send_batch(
            "q.fifo",
            vec![
                entry("dup", make("duplicate", "one")),
                entry("missing", SendRequest::standard("bad")),
                entry("second", make("second", "two")),
                entry("invalid", invalid_group),
                entry("missingkey", SendRequest::fifo("bad", "group")),
                entry("emptykey", make("bad", "")),
                entry("longkey", make("bad", &"x".repeat(129))),
                entry("third", make("third", "three")),
            ],
            1,
        )
        .unwrap();
    assert_eq!(result.successful.len(), 3);
    assert_eq!(result.failed.len(), 5);
    assert_eq!(result.successful[0].value.message_id, first.message_id);
    assert!(result.successful[0].value.deduplicated);
    assert!(
        lqs.send("q.fifo", make("retry", "two"), 2)
            .unwrap()
            .deduplicated
    );
    for body in ["first", "second", "third"] {
        let messages = lqs.receive("q.fifo", 10, 3).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].body, body);
        lqs.delete("q.fifo", &messages[0].receipt_handle).unwrap();
    }
    assert_eq!(lqs.queue_depth("q.fifo").unwrap(), 0);
    // Content-based deduplication also spans entries and single sends.
    lqs.create_queue(
        "content.fifo",
        QueueType::Fifo,
        QueueOptions {
            content_based_deduplication: true,
            ..QueueOptions::default()
        },
    )
    .unwrap();
    let result = lqs
        .send_batch(
            "content.fifo",
            vec![
                entry("a", SendRequest::fifo("same", "g")),
                entry("b", SendRequest::fifo("same", "g")),
            ],
            0,
        )
        .unwrap();
    assert!(result.successful[1].value.deduplicated);
    assert_eq!(lqs.queue_depth("content.fifo").unwrap(), 1);
}

#[test]
fn delete_and_visibility_batches_apply_only_successful_entries() {
    let mut lqs = Lqs::new();
    lqs.create_queue("q", QueueType::Standard, QueueOptions::default())
        .unwrap();
    for body in ["a", "b"] {
        lqs.send("q", SendRequest::standard(body), 0).unwrap();
    }
    let messages = lqs.receive("q", 10, 0).unwrap();
    let change = |handle: &str, timeout_ms| VisibilityChange {
        receipt_handle: handle.into(),
        timeout_ms,
    };
    let result = lqs
        .change_visibility_batch(
            "q",
            vec![
                entry("release", change(&messages[0].receipt_handle, 0)),
                entry("invalid", change("bad", 1000)),
                entry("range", change(&messages[1].receipt_handle, 43_200_001)),
            ],
            1,
        )
        .unwrap();
    assert_eq!(result.successful.len(), 1);
    assert_eq!(result.failed.len(), 2);
    let retried = lqs.receive("q", 10, 1).unwrap();
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].body, "a");
    let result = lqs
        .delete_batch(
            "q",
            vec![
                entry("stale", messages[0].receipt_handle.clone()),
                entry("ok", retried[0].receipt_handle.clone()),
                entry("bad", "nope".into()),
                entry("also", messages[1].receipt_handle.clone()),
            ],
        )
        .unwrap();
    assert_eq!(result.successful.len(), 2);
    assert_eq!(result.failed.len(), 2);
    assert_eq!(lqs.queue_depth("q").unwrap(), 0);
}

#[test]
fn database_failure_rolls_back_only_its_entry_and_successes_survive_reopen() {
    let path = std::env::temp_dir().join(format!(
        "lqs-batch-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    {
        let mut lqs = Lqs::open(&path).unwrap();
        lqs.create_queue("q.fifo", QueueType::Fifo, QueueOptions::default())
            .unwrap();
        lqs.connection.execute_batch("CREATE TRIGGER fail_key BEFORE INSERT ON deduplication_keys WHEN NEW.deduplication_id = 'fail' BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
        let requests = ["before", "fail", "after"]
            .iter()
            .map(|id| {
                entry(
                    id,
                    SendRequest {
                        deduplication_id: Some((*id).into()),
                        ..SendRequest::fifo(*id, "g")
                    },
                )
            })
            .collect();
        let result = lqs.send_batch("q.fifo", requests, 0).unwrap();
        assert_eq!(result.successful.len(), 2);
        assert_eq!(result.failed[0].id, "fail");
        assert!(matches!(result.failed[0].value, LqsError::Database(_)));
        assert_eq!(lqs.queue_depth("q.fifo").unwrap(), 2);
    }
    {
        let mut lqs = Lqs::open(&path).unwrap();
        for body in ["before", "after"] {
            let received = lqs.receive("q.fifo", 10, 1).unwrap();
            assert_eq!(received.len(), 1);
            assert_eq!(received[0].body, body);
            lqs.delete("q.fifo", &received[0].receipt_handle).unwrap();
        }
        assert_eq!(lqs.queue_depth("q.fifo").unwrap(), 0);
    }
    std::fs::remove_file(path).unwrap();
}
