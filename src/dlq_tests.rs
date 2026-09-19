use super::*;

fn policy(target: &str, count: u32) -> Option<RedrivePolicy> {
    Some(RedrivePolicy {
        dead_letter_queue: target.into(),
        max_receive_count: count,
    })
}

fn setup(kind: QueueType) -> (Lqs, &'static str, &'static str) {
    let (source, dlq) = if kind == QueueType::Fifo {
        ("source.fifo", "dead.fifo")
    } else {
        ("source", "dead")
    };
    let mut lqs = Lqs::new();
    lqs.create_queue(
        dlq,
        kind,
        QueueOptions {
            content_based_deduplication: kind == QueueType::Fifo,
            ..QueueOptions::default()
        },
    )
    .unwrap();
    lqs.create_queue(
        source,
        kind,
        QueueOptions {
            visibility_timeout_ms: 10,
            content_based_deduplication: kind == QueueType::Fifo,
            redrive_policy: policy(dlq, 2),
            ..QueueOptions::default()
        },
    )
    .unwrap();
    (lqs, source, dlq)
}

#[test]
fn threshold_waits_for_visibility_and_moves_exactly_once() {
    for kind in [QueueType::Standard, QueueType::Fifo] {
        let (mut lqs, source, dlq) = setup(kind);
        let sent = lqs
            .send(source, SendRequest::fifo("failed", "a"), 0)
            .unwrap();
        let first = lqs.receive(source, 1, 0).unwrap().remove(0);
        assert_eq!(first.receive_count, 1);
        assert!(lqs.receive(source, 1, 9).unwrap().is_empty());
        let second = lqs.receive(source, 1, 10).unwrap().remove(0);
        assert_eq!(second.receive_count, 2);
        assert!(lqs.receive(source, 1, 19).unwrap().is_empty());
        assert_eq!(lqs.queue_depth(dlq).unwrap(), 0);
        assert!(lqs.receive(source, 1, 20).unwrap().is_empty());
        assert_eq!(lqs.queue_depth(source).unwrap(), 0);
        assert_eq!(lqs.queue_depth(dlq).unwrap(), 1);
        assert!(lqs.receive(source, 1, 21).unwrap().is_empty());
        assert_eq!(lqs.queue_depth(dlq).unwrap(), 1);
        assert!(matches!(
            lqs.delete(source, &second.receipt_handle),
            Err(LqsError::InvalidReceiptHandle(_))
        ));
        let dead = lqs.receive(dlq, 1, 21).unwrap().remove(0);
        assert_eq!(dead.message_id, sent.message_id);
        assert_eq!(dead.body, "failed");
        assert_eq!(dead.message_group_id.as_deref(), Some("a"));
        assert_eq!(dead.receive_count, 1);
        let timestamp: i64 = lqs
            .connection
            .query_row(
                "SELECT created_at_ms FROM messages WHERE queue_name = ?1",
                [dlq],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(timestamp, if kind == QueueType::Fifo { 20 } else { 0 });
    }
}

#[test]
fn fifo_moves_only_the_head_and_appends_to_destination() {
    let (mut lqs, source, dlq) = setup(QueueType::Fifo);
    lqs.set_redrive_policy(source, policy(dlq, 1)).unwrap();
    lqs.send(source, SendRequest::fifo("a1", "a"), 0).unwrap();
    lqs.send(source, SendRequest::fifo("a2", "a"), 0).unwrap();
    lqs.send(source, SendRequest::fifo("b1", "b"), 0).unwrap();
    // A newer destination message must remain ahead of the older source message.
    lqs.send(dlq, SendRequest::fifo("already-dead", "a"), 0)
        .unwrap();
    let received = lqs.receive(source, 10, 0).unwrap();
    assert_eq!(
        received.iter().map(|m| m.body.as_str()).collect::<Vec<_>>(),
        ["a1", "b1"]
    );
    assert!(lqs.receive(source, 10, 1).unwrap().is_empty());
    let next = lqs.receive(source, 10, 10).unwrap();
    assert_eq!(
        next.iter().map(|m| m.body.as_str()).collect::<Vec<_>>(),
        ["a2"]
    );
    let dead = lqs.receive(dlq, 10, 10).unwrap();
    assert_eq!(
        dead.iter().map(|m| m.body.as_str()).collect::<Vec<_>>(),
        ["already-dead", "b1"]
    );
    lqs.delete(dlq, &dead[0].receipt_handle).unwrap();
    assert_eq!(lqs.receive(dlq, 10, 11).unwrap()[0].body, "a1");
}

#[test]
fn invalid_policy_does_not_create_or_modify_queues() {
    let (mut lqs, source, dlq) = setup(QueueType::Standard);
    lqs.create_queue("wrong.fifo", QueueType::Fifo, QueueOptions::default())
        .unwrap();
    for invalid in [
        policy(dlq, 0),
        policy(dlq, 1001),
        policy(source, 1),
        policy("wrong.fifo", 1),
        policy("missing", 1),
    ] {
        assert!(lqs.set_redrive_policy(source, invalid.clone()).is_err());
        assert_eq!(lqs.redrive_policy(source).unwrap(), policy(dlq, 2));
    }
    for invalid in [
        policy(dlq, 0),
        policy(dlq, 1001),
        policy("new", 1),
        policy("wrong.fifo", 1),
        policy("missing", 1),
    ] {
        assert!(
            lqs.create_queue(
                "new",
                QueueType::Standard,
                QueueOptions {
                    redrive_policy: invalid,
                    ..QueueOptions::default()
                }
            )
            .is_err()
        );
        assert!(matches!(
            lqs.queue_depth("new"),
            Err(LqsError::QueueNotFound(_))
        ));
    }
    assert_eq!(lqs.list_dead_letter_source_queues(dlq).unwrap(), [source]);
    lqs.set_redrive_policy(source, None).unwrap();
    assert!(lqs.list_dead_letter_source_queues(dlq).unwrap().is_empty());
    assert_eq!(lqs.redrive_policy(source).unwrap(), None);
}

#[test]
fn transfer_failure_rolls_back_source_and_destination() {
    let (mut lqs, source, dlq) = setup(QueueType::Standard);
    lqs.set_redrive_policy(source, policy(dlq, 1)).unwrap();
    lqs.send(source, SendRequest::standard("keep"), 0).unwrap();
    let first = lqs.receive(source, 1, 0).unwrap().remove(0);
    lqs.connection.execute_batch("CREATE TRIGGER fail_dlq BEFORE INSERT ON messages WHEN NEW.queue_name = 'dead' BEGIN SELECT RAISE(ABORT, 'test failure'); END;").unwrap();
    assert!(lqs.receive(source, 1, 10).is_err());
    assert_eq!(lqs.queue_depth(source).unwrap(), 1);
    assert_eq!(lqs.queue_depth(dlq).unwrap(), 0);
    let count: i64 = lqs
        .connection
        .query_row(
            "SELECT receive_count FROM messages WHERE receipt_handle = ?1",
            [&first.receipt_handle],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    lqs.connection
        .execute_batch("DROP TRIGGER fail_dlq")
        .unwrap();
    assert!(lqs.receive(source, 1, 10).unwrap().is_empty());
    assert_eq!(lqs.queue_depth(dlq).unwrap(), 1);
}

#[test]
fn redrive_preserves_origin_isolation_and_resets_identity_and_retries() {
    let (mut lqs, source, dlq) = setup(QueueType::Fifo);
    lqs.set_redrive_policy(source, policy(dlq, 1)).unwrap();
    lqs.send(source, SendRequest::fifo("same-body", "a"), 0)
        .unwrap();
    let first = lqs.receive(source, 1, 0).unwrap().remove(0);
    lqs.receive(source, 1, 10).unwrap();
    let dead = lqs.receive(dlq, 1, 11).unwrap().remove(0);
    assert_eq!(lqs.redrive_dead_letters(dlq, source, 10, 12).unwrap(), 0);
    lqs.change_visibility(dlq, &dead.receipt_handle, 0, 12)
        .unwrap();
    lqs.send(source, SendRequest::fifo("newer", "a"), 12)
        .unwrap();
    // Directly sent DLQ messages have no source provenance and cannot be redriven.
    lqs.send(dlq, SendRequest::fifo("manual", "b"), 12).unwrap();
    lqs.create_queue(
        "other.fifo",
        QueueType::Fifo,
        QueueOptions {
            visibility_timeout_ms: 1,
            content_based_deduplication: true,
            redrive_policy: policy(dlq, 1),
            ..QueueOptions::default()
        },
    )
    .unwrap();
    lqs.send("other.fifo", SendRequest::fifo("other-source", "c"), 10)
        .unwrap();
    lqs.receive("other.fifo", 1, 10).unwrap();
    lqs.receive("other.fifo", 1, 11).unwrap();
    lqs.connection.execute_batch("CREATE TRIGGER fail_redrive BEFORE INSERT ON messages WHEN NEW.queue_name = 'source.fifo' BEGIN SELECT RAISE(ABORT, 'test failure'); END;").unwrap();
    assert!(lqs.redrive_dead_letters(dlq, source, 10, 12).is_err());
    assert_eq!(lqs.queue_depth(dlq).unwrap(), 3);
    lqs.connection
        .execute_batch("DROP TRIGGER fail_redrive")
        .unwrap();
    assert_eq!(lqs.redrive_dead_letters(dlq, source, 0, 12).unwrap(), 0);
    assert_eq!(lqs.redrive_dead_letters(dlq, source, 10, 12).unwrap(), 1);
    assert_eq!(lqs.queue_depth(dlq).unwrap(), 2);
    let newer = lqs.receive(source, 10, 13).unwrap().remove(0);
    assert_eq!(newer.body, "newer");
    lqs.delete(source, &newer.receipt_handle).unwrap();
    let redriven = lqs.receive(source, 10, 13).unwrap().remove(0);
    assert_eq!(redriven.body, first.body);
    assert_ne!(redriven.message_id, first.message_id);
    assert_eq!(redriven.receive_count, 1);
    assert!(lqs.delete(dlq, &dead.receipt_handle).is_err());
}

#[test]
fn old_database_upgrade_and_restart_preserve_policy_counts_and_origins() {
    let directory = std::env::temp_dir().join(format!(
        "lqs-dlq-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("database.sqlite");
    {
        let mut lqs = Lqs::open(&path).unwrap();
        lqs.create_queue("source", QueueType::Standard, QueueOptions::default())
            .unwrap();
        lqs.send("source", SendRequest::standard("persisted"), 0)
            .unwrap();
        lqs.receive("source", 1, 0).unwrap();
        // Reproduce the previous schema: queues/messages remain unchanged.
        lqs.connection
            .execute_batch("DROP TABLE dead_letter_origins; DROP TABLE redrive_policies;")
            .unwrap();
    }
    {
        let mut lqs = Lqs::open(&path).unwrap();
        lqs.create_queue("dead", QueueType::Standard, QueueOptions::default())
            .unwrap();
        lqs.set_redrive_policy("source", policy("dead", 2)).unwrap();
    }
    {
        let mut lqs = Lqs::open(&path).unwrap();
        assert_eq!(lqs.redrive_policy("source").unwrap(), policy("dead", 2));
        assert_eq!(
            lqs.list_dead_letter_source_queues("dead").unwrap(),
            ["source"]
        );
        assert_eq!(
            lqs.receive("source", 1, 30_000).unwrap()[0].receive_count,
            2
        );
    }
    {
        let mut lqs = Lqs::open(&path).unwrap();
        assert!(lqs.receive("source", 1, 60_000).unwrap().is_empty());
        assert_eq!(lqs.queue_depth("dead").unwrap(), 1);
    }
    {
        let mut lqs = Lqs::open(&path).unwrap();
        assert_eq!(
            lqs.redrive_dead_letters("dead", "source", 10, 60_000)
                .unwrap(),
            1
        );
        assert_eq!(
            lqs.receive("source", 1, 60_001).unwrap()[0].body,
            "persisted"
        );
    }
    std::fs::remove_dir_all(directory).unwrap();
}
