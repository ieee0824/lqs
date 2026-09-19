use super::*;

fn temp_db() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "lqs-manage-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn create(lqs: &mut Lqs, name: &str) {
    let kind = if name.ends_with(".fifo") {
        QueueType::Fifo
    } else {
        QueueType::Standard
    };
    lqs.create_queue_with_tags_at(
        name,
        kind,
        QueueOptions {
            content_based_deduplication: kind == QueueType::Fifo,
            ..QueueOptions::default()
        },
        QueueTags::new(),
        0,
    )
    .unwrap();
}

#[test]
fn queue_visibility_zero_and_metrics() {
    let mut lqs = Lqs::new();
    lqs.create_queue_with_tags_at(
        "q",
        QueueType::Standard,
        QueueOptions::default(),
        QueueTags::new(),
        1000,
    )
    .unwrap();
    lqs.send("q", SendRequest::standard("flight"), 1000)
        .unwrap();
    let first = lqs.receive("q", 1, 1000).unwrap();
    lqs.send("q", SendRequest::standard("visible"), 1000)
        .unwrap();
    lqs.send(
        "q",
        SendRequest {
            delay_ms: Some(5000),
            ..SendRequest::standard("delayed")
        },
        1000,
    )
    .unwrap();
    let metrics = lqs.queue_metrics("q", 1000).unwrap();
    assert_eq!(
        (metrics.visible, metrics.not_visible, metrics.delayed),
        (1, 1, 1)
    );
    assert_eq!(
        (metrics.created_at_ms, metrics.modified_at_ms),
        (1000, 1000)
    );
    lqs.set_queue_attributes(
        "q",
        QueueUpdate {
            visibility_timeout_ms: Some(0),
            message_retention_ms: Some(60_000),
            ..QueueUpdate::default()
        },
        2000,
    )
    .unwrap();
    assert_eq!(lqs.receive("q", 1, 2000).unwrap()[0].body, "visible");
    assert_eq!(lqs.in_flight_count("q", 2000).unwrap(), 1);
    assert_eq!(lqs.queue_metrics("q", 2000).unwrap().modified_at_ms, 2000);
    assert_eq!(lqs.queue_config("q").unwrap().visibility_timeout_ms, 0);
    assert_eq!(lqs.queue_metrics("q", 31_000).unwrap().visible, 3);
    let expired = lqs.queue_metrics("q", 61_000).unwrap();
    assert_eq!(
        (expired.visible, expired.not_visible, expired.delayed),
        (0, 0, 0)
    );
    // Metrics are non-mutating; expired messages are filtered without a receive.
    assert_eq!(lqs.queue_depth("q").unwrap(), 3);
    assert!(
        lqs.change_visibility("q", &first[0].receipt_handle, 1, 61_000)
            .is_err()
    );
}

#[test]
fn list_queues_is_case_sensitive_literal_and_paginated_without_offsets() {
    let mut lqs = Lqs::new();
    for name in ["x_a", "x_b", "x_c.fifo", "X_a", "xxa"] {
        create(&mut lqs, name);
    }
    assert!(
        lqs.list_queues("%", None, None)
            .unwrap()
            .queue_names
            .is_empty()
    );
    let first = lqs.list_queues("x_", Some(2), None).unwrap();
    assert_eq!(first.queue_names, ["x_a", "x_b"]);
    lqs.delete_queue("x_a", 1).unwrap();
    lqs.delete_queue("x_b", 1).unwrap();
    let next = lqs
        .list_queues("x_", Some(2), first.next_cursor.as_deref())
        .unwrap();
    assert_eq!(next.queue_names, ["x_c.fifo"]);
    assert!(next.next_cursor.is_none());
    assert!(
        lqs.list_queues("X_", Some(2), first.next_cursor.as_deref())
            .is_err()
    );
    for bad in ["", "invalid!", "WzEsIiIsIiJd"] {
        assert!(lqs.list_queues("", Some(2), Some(bad)).is_err());
    }
    for bad in [0, 1001, usize::MAX] {
        assert!(lqs.list_queues("", Some(bad), None).is_err());
    }
    for i in 0..1001 {
        create(&mut lqs, &format!("many-{i:04}"));
    }
    let default = lqs.list_queues("many-", None, None).unwrap();
    assert_eq!(default.queue_names.len(), 1000);
    assert!(default.next_cursor.is_none());
    assert!(
        lqs.list_queues("many-", Some(1000), None)
            .unwrap()
            .next_cursor
            .is_some()
    );
}

#[test]
fn tags_validate_atomically_overwrite_and_survive_restart_with_settings() {
    let path = temp_db();
    {
        let mut lqs = Lqs::open(&path).unwrap();
        lqs.create_queue_with_tags_at(
            "q",
            QueueType::Standard,
            QueueOptions::default(),
            QueueTags::from([("環境".into(), "開発".into())]),
            1000,
        )
        .unwrap();
        lqs.tag_queue(
            "q",
            QueueTags::from([("empty".into(), "".into()), ("環境".into(), "本番".into())]),
        )
        .unwrap();
        lqs.untag_queue("q", &["missing".into(), "empty".into()])
            .unwrap();
        let before = lqs.list_queue_tags("q").unwrap();
        for (key, value) in [
            ("aws:reserved".into(), "v".into()),
            ("key".into(), "AWS:value".into()),
            ("".into(), "v".into()),
            ("x".repeat(129), "v".into()),
            ("key".into(), "x".repeat(257)),
            ("<bad>".into(), "v".into()),
            ("key".into(), "\u{b}".into()),
        ] {
            assert!(
                lqs.tag_queue(
                    "q",
                    QueueTags::from([("good".into(), "value".into()), (key, value)])
                )
                .is_err()
            );
            assert_eq!(lqs.list_queue_tags("q").unwrap(), before);
        }
        lqs.set_queue_attributes(
            "q",
            QueueUpdate {
                visibility_timeout_ms: Some(0),
                delay_ms: Some(1000),
                ..QueueUpdate::default()
            },
            5000,
        )
        .unwrap();
        let settings = lqs.queue_config("q").unwrap();
        assert!(
            lqs.set_queue_attributes(
                "q",
                QueueUpdate {
                    visibility_timeout_ms: Some(43_200_001),
                    delay_ms: Some(0),
                    ..QueueUpdate::default()
                },
                6000
            )
            .is_err()
        );
        assert_eq!(lqs.queue_config("q").unwrap(), settings);
        assert_eq!(lqs.queue_metrics("q", 6000).unwrap().modified_at_ms, 5000);
        for i in 0..49 {
            lqs.tag_queue("q", QueueTags::from([(format!("k{i}"), "v".into())]))
                .unwrap();
        }
        assert!(
            lqs.tag_queue(
                "q",
                QueueTags::from([
                    ("k0".into(), "must roll back".into()),
                    ("overflow".into(), "v".into())
                ])
            )
            .is_err()
        );
        assert_eq!(lqs.list_queue_tags("q").unwrap()["k0"], "v");
    }
    {
        let lqs = Lqs::open(&path).unwrap();
        assert_eq!(lqs.list_queue_tags("q").unwrap()["環境"], "本番");
        assert_eq!(lqs.list_queue_tags("q").unwrap().len(), 50);
        assert_eq!(lqs.queue_config("q").unwrap().visibility_timeout_ms, 0);
        assert_eq!(lqs.queue_config("q").unwrap().delay_ms, 1000);
        assert_eq!(lqs.queue_metrics("q", 6000).unwrap().created_at_ms, 1000);
    }
    std::fs::remove_file(path).unwrap();
}

#[test]
fn purge_removes_all_delivery_states_only_in_target_and_persists_cooldown() {
    let path = temp_db();
    {
        let mut lqs = Lqs::open(&path).unwrap();
        for name in ["q", "other", "dlq"] {
            create(&mut lqs, name);
        }
        lqs.set_redrive_policy(
            "q",
            Some(RedrivePolicy {
                dead_letter_queue: "dlq".into(),
                max_receive_count: 1,
            }),
        )
        .unwrap();
        lqs.tag_queue("q", QueueTags::from([("keep".into(), "tag".into())]))
            .unwrap();
        lqs.send("q", SendRequest::standard("inflight"), 0).unwrap();
        let receipt = lqs.receive("q", 1, 0).unwrap()[0].receipt_handle.clone();
        lqs.send("q", SendRequest::standard("visible"), 0).unwrap();
        lqs.send(
            "q",
            SendRequest {
                delay_ms: Some(900_000),
                ..SendRequest::standard("delayed")
            },
            0,
        )
        .unwrap();
        for name in ["other", "dlq"] {
            lqs.send(name, SendRequest::standard("keep"), 0).unwrap();
        }
        lqs.purge_queue("q", 1000).unwrap();
        assert_eq!(lqs.queue_depth("q").unwrap(), 0);
        assert!(lqs.delete("q", &receipt).is_err());
        assert_eq!(lqs.queue_depth("other").unwrap(), 1);
        assert_eq!(lqs.queue_depth("dlq").unwrap(), 1);
        assert!(lqs.redrive_policy("q").unwrap().is_some());
        assert_eq!(lqs.list_queue_tags("q").unwrap()["keep"], "tag");
        lqs.send("q", SendRequest::standard("after"), 1001).unwrap();
        assert_eq!(
            lqs.purge_queue("q", 60_999),
            Err(LqsError::PurgeQueueInProgress)
        );
        assert_eq!(lqs.queue_depth("q").unwrap(), 1);
    }
    {
        let mut lqs = Lqs::open(&path).unwrap();
        assert_eq!(
            lqs.purge_queue("q", 60_999),
            Err(LqsError::PurgeQueueInProgress)
        );
        lqs.purge_queue("q", 61_000).unwrap();
        assert_eq!(lqs.queue_depth("q").unwrap(), 0);
        assert_eq!(lqs.queue_depth("other").unwrap(), 1);
    }
    std::fs::remove_file(path).unwrap();
}

#[test]
fn fifo_purge_preserves_send_history_and_invalidates_receive_history() {
    let mut lqs = Lqs::new();
    create(&mut lqs, "q.fifo");
    let send = SendRequest::fifo("same", "g");
    let original = lqs.send("q.fifo", send.clone(), 0).unwrap();
    let options = ReceiveOptions {
        receive_request_attempt_id: Some("attempt".into()),
        ..ReceiveOptions::default()
    };
    lqs.receive_with_options("q.fifo", 1, options.clone(), 0)
        .unwrap();
    lqs.purge_queue("q.fifo", 1).unwrap();
    assert!(lqs.receive_with_options("q.fifo", 1, options, 2).is_err());
    let duplicate = lqs.send("q.fifo", send, 2).unwrap();
    assert!(duplicate.deduplicated);
    assert_eq!(duplicate.message_id, original.message_id);
    assert_eq!(lqs.queue_depth("q.fifo").unwrap(), 0);
}

#[test]
fn delete_detaches_dlq_relationships_without_deleting_other_queues_messages() {
    for target in ["source.fifo", "dead.fifo"] {
        let mut lqs = Lqs::new();
        for name in ["source.fifo", "dead.fifo", "other.fifo"] {
            create(&mut lqs, name);
        }
        lqs.set_redrive_policy(
            "source.fifo",
            Some(RedrivePolicy {
                dead_letter_queue: "dead.fifo".into(),
                max_receive_count: 1,
            }),
        )
        .unwrap();
        lqs.send("source.fifo", SendRequest::fifo("dead", "g"), 0)
            .unwrap();
        lqs.receive("source.fifo", 1, 0).unwrap();
        assert!(lqs.receive("source.fifo", 1, 30_000).unwrap().is_empty());
        lqs.send("source.fifo", SendRequest::fifo("source", "h"), 30_000)
            .unwrap();
        lqs.send("other.fifo", SendRequest::fifo("other", "g"), 0)
            .unwrap();
        lqs.tag_queue(target, QueueTags::from([("delete".into(), "tag".into())]))
            .unwrap();
        lqs.receive_with_options(
            target,
            1,
            ReceiveOptions {
                receive_request_attempt_id: Some("r".into()),
                ..ReceiveOptions::default()
            },
            30_000,
        )
        .unwrap();
        lqs.delete_queue(target, 31_000).unwrap();
        assert!(!lqs.queue_exists(target).unwrap());
        for table in [
            "messages",
            "deduplication_keys",
            "queue_tags",
            "receive_attempts",
            "receive_attempt_members",
        ] {
            let count: usize = lqs
                .connection
                .query_row(
                    &format!("SELECT COUNT(*) FROM {table} WHERE queue_name = ?1"),
                    [target],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 0, "{table}");
        }
        let survivor = if target == "source.fifo" {
            "dead.fifo"
        } else {
            "source.fifo"
        };
        assert_eq!(lqs.queue_depth(survivor).unwrap(), 1);
        assert_eq!(lqs.queue_depth("other.fifo").unwrap(), 1);
        assert!(lqs.redrive_policy(survivor).unwrap().is_none());
        assert!(
            !lqs.connection
                .prepare("PRAGMA foreign_key_check")
                .unwrap()
                .exists([])
                .unwrap()
        );
        assert_eq!(
            lqs.create_queue_with_tags_at(
                target,
                QueueType::Fifo,
                QueueOptions::default(),
                QueueTags::new(),
                90_999
            ),
            Err(LqsError::QueueDeletedRecently)
        );
        lqs.create_queue_with_tags_at(
            target,
            QueueType::Fifo,
            QueueOptions::default(),
            QueueTags::new(),
            91_000,
        )
        .unwrap();
        assert!(lqs.list_queue_tags(target).unwrap().is_empty());
        assert_eq!(lqs.queue_depth(target).unwrap(), 0);
    }
}

#[test]
fn destructive_failures_roll_back_and_deleted_names_survive_restart() {
    let path = temp_db();
    {
        let mut lqs = Lqs::open(&path).unwrap();
        create(&mut lqs, "q");
        create(&mut lqs, "dead");
        lqs.set_redrive_policy(
            "q",
            Some(RedrivePolicy {
                dead_letter_queue: "dead".into(),
                max_receive_count: 1,
            }),
        )
        .unwrap();
        lqs.send("q", SendRequest::standard("keep"), 0).unwrap();
        lqs.connection.execute_batch("CREATE TRIGGER fail_purge BEFORE UPDATE OF last_purge_ms ON queues BEGIN SELECT RAISE(ABORT, 'injected'); END;
            CREATE TRIGGER fail_delete BEFORE DELETE ON queues BEGIN SELECT RAISE(ABORT, 'injected'); END;").unwrap();
        assert!(lqs.purge_queue("q", 1).is_err());
        assert!(lqs.delete_queue("q", 1).is_err());
        assert_eq!(lqs.queue_depth("q").unwrap(), 1);
        assert!(lqs.redrive_policy("q").unwrap().is_some());
        lqs.connection
            .execute_batch("DROP TRIGGER fail_purge; DROP TRIGGER fail_delete;")
            .unwrap();
        lqs.purge_queue("q", 1).unwrap();
        lqs.delete_queue("q", 1).unwrap();
    }
    {
        let mut lqs = Lqs::open(&path).unwrap();
        assert_eq!(
            lqs.create_queue_with_tags_at(
                "q",
                QueueType::Standard,
                QueueOptions::default(),
                QueueTags::new(),
                60_000
            ),
            Err(LqsError::QueueDeletedRecently)
        );
        lqs.create_queue_with_tags_at(
            "q",
            QueueType::Standard,
            QueueOptions::default(),
            QueueTags::new(),
            60_001,
        )
        .unwrap();
        assert!(matches!(
            lqs.purge_queue("missing", 0),
            Err(LqsError::QueueNotFound(_))
        ));
        assert!(matches!(
            lqs.delete_queue("missing", 0),
            Err(LqsError::QueueNotFound(_))
        ));
    }
    std::fs::remove_file(path).unwrap();
}

#[test]
fn legacy_visibility_check_upgrade_keeps_children_and_foreign_keys() {
    let connection = Connection::open_in_memory().unwrap();
    connection.execute_batch("CREATE TABLE queues (
        name TEXT PRIMARY KEY NOT NULL, queue_type TEXT NOT NULL,
        visibility_timeout_ms INTEGER NOT NULL CHECK(visibility_timeout_ms > 0),
        content_based_deduplication INTEGER NOT NULL, deduplication_window_ms INTEGER NOT NULL);
        INSERT INTO queues VALUES ('q.fifo', 'fifo', 30000, 1, 300000), ('dead.fifo', 'fifo', 30000, 1, 300000);
        CREATE TABLE messages (sequence INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT UNIQUE, queue_name TEXT NOT NULL REFERENCES queues(name) ON DELETE CASCADE, body TEXT NOT NULL, group_id TEXT, receipt_handle TEXT UNIQUE, invisible_until_ms INTEGER, receive_count INTEGER NOT NULL DEFAULT 0, created_at_ms INTEGER NOT NULL);
        INSERT INTO messages VALUES (1, 'msg-0000000000000001', 'q.fifo', 'preserve', 'g', NULL, NULL, 0, 0);
        CREATE INDEX custom_queue_index ON queues(queue_type);").unwrap();
    let mut lqs = Lqs::from_connection(connection).unwrap();
    assert_eq!(lqs.queue_depth("q.fifo").unwrap(), 1);
    assert_eq!(
        lqs.connection
            .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert!(
        lqs.connection
            .prepare("SELECT name FROM sqlite_master WHERE name = 'custom_queue_index'")
            .unwrap()
            .exists([])
            .unwrap()
    );
    assert_eq!(lqs.queue_metrics("q.fifo", 1).unwrap().created_at_ms, 0);
    lqs.set_queue_attributes(
        "q.fifo",
        QueueUpdate {
            visibility_timeout_ms: Some(0),
            ..QueueUpdate::default()
        },
        1,
    )
    .unwrap();
    assert_eq!(lqs.receive("q.fifo", 1, 1).unwrap()[0].body, "preserve");
    lqs.delete_queue("q.fifo", 2).unwrap();
    let count: usize = lqs
        .connection
        .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn concurrent_purges_enforce_one_shared_cooldown() {
    let path = temp_db();
    let mut lqs = Lqs::open(&path).unwrap();
    create(&mut lqs, "q");
    lqs.send("q", SendRequest::standard("delete"), 0).unwrap();
    let connections: Vec<_> = (0..3).map(|_| Lqs::open(&path).unwrap()).collect();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let threads: Vec<_> = connections
        .into_iter()
        .map(|mut connection| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                connection.purge_queue("q", 1)
            })
        })
        .collect();
    let results: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| **result == Err(LqsError::PurgeQueueInProgress))
            .count(),
        2
    );
    assert_eq!(lqs.queue_depth("q").unwrap(), 0);
    drop(lqs);
    std::fs::remove_file(path).unwrap();
}
