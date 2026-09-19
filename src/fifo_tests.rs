use super::*;

fn fifo_queue(lqs: &mut Lqs, scope: DeduplicationScope) {
    lqs.create_queue(
        "q.fifo",
        QueueType::Fifo,
        QueueOptions {
            content_based_deduplication: true,
            deduplication_scope: scope,
            ..QueueOptions::default()
        },
    )
    .unwrap();
}
fn attempt(id: &str) -> ReceiveOptions {
    ReceiveOptions {
        receive_request_attempt_id: Some(id.into()),
        ..ReceiveOptions::default()
    }
}

#[test]
fn sha256_explicit_override_deleted_keys_and_fixed_window() {
    let mut lqs = Lqs::new();
    fifo_queue(&mut lqs, DeduplicationScope::Queue);
    let digest = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    assert_eq!(stable_content_id("abc"), digest);
    let first = lqs
        .send("q.fifo", SendRequest::fifo("abc", "a"), 1000)
        .unwrap();
    let received = lqs.receive("q.fifo", 1, 1000).unwrap().remove(0);
    assert_eq!(received.system_attributes["MessageDeduplicationId"], digest);
    lqs.delete("q.fifo", &received.receipt_handle).unwrap();
    let duplicate = SendRequest {
        deduplication_id: Some(digest.into()),
        ..SendRequest::fifo("different", "b")
    };
    let retry = lqs.send("q.fifo", duplicate.clone(), 300_999).unwrap();
    assert!(retry.deduplicated);
    assert_eq!(retry.message_id, first.message_id);
    // Retrying does not extend the original five-minute window.
    assert!(!lqs.send("q.fifo", duplicate, 301_000).unwrap().deduplicated);
    assert!(
        !lqs.send(
            "q.fifo",
            SendRequest {
                deduplication_id: Some("override".into()),
                ..SendRequest::fifo("abc", "a")
            },
            301_001
        )
        .unwrap()
        .deduplicated
    );
    assert!(
        lqs.create_queue(
            "bad.fifo",
            QueueType::Fifo,
            QueueOptions {
                deduplication_window_ms: 1,
                ..QueueOptions::default()
            }
        )
        .is_err()
    );
}

#[test]
fn scopes_and_throughput_update_atomically_and_apply_to_batches() {
    let mut lqs = Lqs::new();
    fifo_queue(&mut lqs, DeduplicationScope::MessageGroup);
    let sent = lqs
        .send_batch(
            "q.fifo",
            vec![
                crate::BatchEntry {
                    id: "a".into(),
                    value: SendRequest::fifo("same", "a"),
                },
                crate::BatchEntry {
                    id: "b".into(),
                    value: SendRequest::fifo("same", "b"),
                },
                crate::BatchEntry {
                    id: "c".into(),
                    value: SendRequest::fifo("same", "a"),
                },
            ],
            0,
        )
        .unwrap();
    assert_eq!(
        sent.successful
            .iter()
            .map(|e| e.value.deduplicated)
            .collect::<Vec<_>>(),
        [false, false, true]
    );
    lqs.set_queue_attributes(
        "q.fifo",
        QueueUpdate {
            fifo_throughput_limit: Some(FifoThroughputLimit::PerMessageGroupId),
            ..QueueUpdate::default()
        },
        1,
    )
    .unwrap();
    let before = lqs.queue_config("q.fifo").unwrap();
    assert!(
        lqs.set_queue_attributes(
            "q.fifo",
            QueueUpdate {
                delay_ms: Some(1000),
                deduplication_scope: Some(DeduplicationScope::Queue),
                ..QueueUpdate::default()
            },
            1
        )
        .is_err()
    );
    assert_eq!(lqs.queue_config("q.fifo").unwrap(), before);
    lqs.set_queue_attributes(
        "q.fifo",
        QueueUpdate {
            deduplication_scope: Some(DeduplicationScope::Queue),
            fifo_throughput_limit: Some(FifoThroughputLimit::PerQueue),
            ..QueueUpdate::default()
        },
        1,
    )
    .unwrap();
    assert!(
        lqs.send("q.fifo", SendRequest::fifo("same", "c"), 2)
            .unwrap()
            .deduplicated
    );
    assert_eq!(lqs.queue_depth("q.fifo").unwrap(), 2);
    assert!(
        lqs.create_queue(
            "bad.fifo",
            QueueType::Fifo,
            QueueOptions {
                fifo_throughput_limit: FifoThroughputLimit::PerMessageGroupId,
                ..QueueOptions::default()
            }
        )
        .is_err()
    );
    lqs.create_queue("s", QueueType::Standard, QueueOptions::default())
        .unwrap();
    assert!(
        lqs.set_queue_attributes(
            "s",
            QueueUpdate {
                deduplication_scope: Some(DeduplicationScope::Queue),
                ..QueueUpdate::default()
            },
            0
        )
        .is_err()
    );
    assert!(
        lqs.send(
            "s",
            SendRequest {
                deduplication_id: Some("id".into()),
                ..SendRequest::standard("body")
            },
            0
        )
        .is_err()
    );
}

#[test]
fn receive_replay_preserves_result_extends_visibility_and_expires_at_five_minutes() {
    let mut lqs = Lqs::new();
    fifo_queue(&mut lqs, DeduplicationScope::Queue);
    lqs.set_queue_attributes(
        "q.fifo",
        QueueUpdate {
            max_in_flight: Some(2),
            ..QueueUpdate::default()
        },
        0,
    )
    .unwrap();
    for g in ["a", "b"] {
        lqs.send("q.fifo", SendRequest::fifo(g, g), 0).unwrap();
    }
    let first = lqs
        .receive_with_options("q.fifo", 10, attempt("r"), 1000)
        .unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(
        lqs.receive_with_options("q.fifo", 10, attempt("r"), 20_000)
            .unwrap(),
        first
    );
    assert!(lqs.receive("q.fifo", 10, 31_000).unwrap().is_empty());
    assert_eq!(lqs.in_flight_count("q.fifo", 49_999).unwrap(), 2);
    assert!(
        lqs.receive_with_options("q.fifo", 1, attempt("r"), 30_000)
            .is_err()
    );
    assert_eq!(
        lqs.receive_with_options("q.fifo", 10, attempt("r"), 300_999)
            .unwrap(),
        first
    );
    // Expiration uses the initial call time, not the most recent retry.
    assert!(
        lqs.receive_with_options("q.fifo", 10, attempt("r"), 301_000)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        lqs.receive("q.fifo", 10, 330_999).unwrap()[0].receive_count,
        2
    );
}

#[test]
fn modified_deleted_expired_or_redelivered_messages_invalidate_the_entire_attempt() {
    for modification in ["delete", "visibility", "retention", "redeliver"] {
        let mut lqs = Lqs::new();
        fifo_queue(&mut lqs, DeduplicationScope::Queue);
        for g in ["a", "b"] {
            lqs.send("q.fifo", SendRequest::fifo(g, g), 0).unwrap();
        }
        let first = lqs
            .receive_with_options("q.fifo", 10, attempt("r"), 0)
            .unwrap();
        let now = match modification {
            "delete" => {
                lqs.delete("q.fifo", &first[0].receipt_handle).unwrap();
                1
            }
            "visibility" => {
                lqs.change_visibility("q.fifo", &first[0].receipt_handle, 30_000, 1)
                    .unwrap();
                1
            }
            "retention" => {
                lqs.set_queue_attributes(
                    "q.fifo",
                    QueueUpdate {
                        message_retention_ms: Some(60_000),
                        ..QueueUpdate::default()
                    },
                    1,
                )
                .unwrap();
                60_000
            }
            _ => {
                lqs.receive("q.fifo", 1, 30_000).unwrap();
                30_001
            }
        };
        assert!(
            lqs.receive_with_options("q.fifo", 10, attempt("r"), now)
                .is_err(),
            "{modification}"
        );
    }
}

#[test]
fn empty_results_are_idempotent_and_identifiers_are_queue_scoped() {
    let mut lqs = Lqs::new();
    fifo_queue(&mut lqs, DeduplicationScope::Queue);
    assert!(
        lqs.receive_with_options("q.fifo", 1, attempt("empty"), 0)
            .unwrap()
            .is_empty()
    );
    lqs.send("q.fifo", SendRequest::fifo("later", "a"), 1)
        .unwrap();
    assert!(
        lqs.receive_with_options("q.fifo", 1, attempt("empty"), 1)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        lqs.receive_with_options("q.fifo", 1, attempt("new"), 1)
            .unwrap()
            .len(),
        1
    );
    lqs.create_queue(
        "other.fifo",
        QueueType::Fifo,
        QueueOptions {
            content_based_deduplication: true,
            ..QueueOptions::default()
        },
    )
    .unwrap();
    lqs.send("other.fifo", SendRequest::fifo("other", "a"), 1)
        .unwrap();
    assert_eq!(
        lqs.receive_with_options("other.fifo", 1, attempt("empty"), 1)
            .unwrap()
            .len(),
        1
    );
    for id in ["".to_owned(), " ".into(), "x".repeat(129), "日本語".into()] {
        assert!(
            lqs.receive_with_options("q.fifo", 1, attempt(&id), 1)
                .is_err()
        );
    }
    lqs.create_queue("s", QueueType::Standard, QueueOptions::default())
        .unwrap();
    assert!(lqs.receive_with_options("s", 1, attempt("r"), 1).is_err());
    assert!(
        lqs.receive_with_options(
            "q.fifo",
            1,
            ReceiveOptions {
                visibility_timeout_ms: Some(43_200_001),
                ..ReceiveOptions::default()
            },
            1
        )
        .is_err()
    );
}

#[test]
fn receive_override_zero_does_not_duplicate_messages_in_one_response() {
    let mut lqs = Lqs::new();
    fifo_queue(&mut lqs, DeduplicationScope::Queue);
    lqs.send("q.fifo", SendRequest::fifo("a", "a"), 0).unwrap();
    let options = ReceiveOptions {
        visibility_timeout_ms: Some(0),
        ..attempt("zero")
    };
    let first = lqs
        .receive_with_options("q.fifo", 10, options.clone(), 0)
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(
        lqs.receive_with_options("q.fifo", 10, options, 1).unwrap(),
        first
    );
    assert_eq!(lqs.receive("q.fifo", 1, 1).unwrap()[0].receive_count, 2);
}

#[test]
fn attempts_and_group_keys_persist_across_connections_and_restart() {
    let path = std::env::temp_dir().join(format!(
        "lqs-fifo-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let first = {
        let mut lqs = Lqs::open(&path).unwrap();
        fifo_queue(&mut lqs, DeduplicationScope::MessageGroup);
        lqs.send("q.fifo", SendRequest::fifo("same", "a"), 0)
            .unwrap();
        lqs.receive_with_options("q.fifo", 1, attempt("persist"), 0)
            .unwrap()
    };
    {
        let mut a = Lqs::open(&path).unwrap();
        let mut b = Lqs::open(&path).unwrap();
        assert_eq!(
            a.receive_with_options("q.fifo", 1, attempt("persist"), 1)
                .unwrap(),
            first
        );
        assert_eq!(
            a.queue_config("q.fifo").unwrap().deduplication_scope,
            DeduplicationScope::MessageGroup
        );
        assert!(
            a.send("q.fifo", SendRequest::fifo("same", "a"), 1)
                .unwrap()
                .deduplicated
        );
        assert!(
            !b.send("q.fifo", SendRequest::fifo("same", "b"), 1)
                .unwrap()
                .deduplicated
        );
        b.change_visibility("q.fifo", &first[0].receipt_handle, 0, 2)
            .unwrap();
        assert!(
            a.receive_with_options("q.fifo", 1, attempt("persist"), 2)
                .is_err()
        );
    }
    std::fs::remove_file(path).unwrap();
}

#[test]
fn legacy_fifo_schema_preserves_live_and_deleted_explicit_keys() {
    let mut lqs = Lqs::new();
    fifo_queue(&mut lqs, DeduplicationScope::Queue);
    for id in ["deleted", "live"] {
        lqs.send(
            "q.fifo",
            SendRequest {
                deduplication_id: Some(id.into()),
                ..SendRequest::fifo(id, "g")
            },
            0,
        )
        .unwrap();
    }
    let first = lqs.receive("q.fifo", 1, 0).unwrap().remove(0);
    lqs.delete("q.fifo", &first.receipt_handle).unwrap();
    // Reconstruct the pre-#8 deduplication table and queue columns.
    lqs.connection.execute_batch("ALTER TABLE deduplication_keys RENAME TO new_keys;
        CREATE TABLE deduplication_keys (queue_name TEXT NOT NULL REFERENCES queues(name) ON DELETE CASCADE,
            deduplication_id TEXT NOT NULL, message_id TEXT NOT NULL, seen_at_ms INTEGER NOT NULL,
            PRIMARY KEY(queue_name, deduplication_id));
        INSERT INTO deduplication_keys SELECT queue_name, deduplication_id, message_id, seen_at_ms FROM new_keys;
        DROP TABLE new_keys;
        ALTER TABLE queues DROP COLUMN deduplication_scope;
        ALTER TABLE queues DROP COLUMN fifo_throughput_limit;
        UPDATE queues SET deduplication_window_ms = 1;").unwrap();
    let mut lqs = Lqs::from_connection(lqs.connection).unwrap();
    assert_eq!(
        lqs.queue_config("q.fifo").unwrap().deduplication_window_ms,
        300_000
    );
    assert_eq!(lqs.queue_depth("q.fifo").unwrap(), 1);
    lqs.set_queue_attributes(
        "q.fifo",
        QueueUpdate {
            deduplication_scope: Some(DeduplicationScope::MessageGroup),
            ..QueueUpdate::default()
        },
        1,
    )
    .unwrap();
    for (id, group) in [("live", "g"), ("deleted", "unknown-group")] {
        assert!(
            lqs.send(
                "q.fifo",
                SendRequest {
                    deduplication_id: Some(id.into()),
                    ..SendRequest::fifo(id, group)
                },
                299_999
            )
            .unwrap()
            .deduplicated
        );
    }
    assert!(
        !lqs.send(
            "q.fifo",
            SendRequest {
                deduplication_id: Some("deleted".into()),
                ..SendRequest::fifo("deleted", "new")
            },
            300_000
        )
        .unwrap()
        .deduplicated
    );
}

#[test]
fn simultaneous_receive_attempts_claim_once() {
    let path = std::env::temp_dir().join(format!(
        "lqs-fifo-concurrent-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut lqs = Lqs::open(&path).unwrap();
    fifo_queue(&mut lqs, DeduplicationScope::Queue);
    for g in ["a", "b"] {
        lqs.send("q.fifo", SendRequest::fifo(g, g), 0).unwrap();
    }
    let connections: Vec<_> = (0..4).map(|_| Lqs::open(&path).unwrap()).collect();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
    let threads: Vec<_> = connections
        .into_iter()
        .map(|mut connection| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                connection
                    .receive_with_options("q.fifo", 10, attempt("same"), 1)
                    .unwrap()
            })
        })
        .collect();
    let responses: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(responses[0].len(), 2);
    for result in &responses {
        assert_eq!(result, &responses[0]);
    }
    assert_eq!(lqs.in_flight_count("q.fifo", 1).unwrap(), 2);
    drop(lqs);
    std::fs::remove_file(path).unwrap();
}
