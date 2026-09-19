use super::*;

#[test]
fn legacy_upgrade_and_reopen_preserve_delivery_settings_and_messages() {
    let directory = std::env::temp_dir().join(format!(
        "lqs-delivery-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("queue.sqlite");
    {
        let mut lqs = Lqs::open(&path).unwrap();
        lqs.create_queue("q", QueueType::Standard, QueueOptions::default())
            .unwrap();
        lqs.send("q", SendRequest::standard("legacy"), 0).unwrap();
        lqs.receive("q", 1, 0).unwrap();
        lqs.connection.execute_batch("ALTER TABLE queues DROP COLUMN delay_ms; ALTER TABLE queues DROP COLUMN message_retention_ms; ALTER TABLE queues DROP COLUMN maximum_message_size; ALTER TABLE messages DROP COLUMN available_at_ms;").unwrap();
    }
    {
        let mut lqs = Lqs::open(&path).unwrap();
        let config = lqs.queue_config("q").unwrap();
        assert_eq!(config.delay_ms, 0);
        assert_eq!(config.message_retention_ms, 345_600_000);
        assert_eq!(config.maximum_message_size, MAX_MESSAGE_BYTES);
        let legacy = lqs.receive("q", 1, 30_000).unwrap().remove(0);
        assert_eq!(legacy.receive_count, 2);
        assert_eq!(legacy.body, "legacy");
        lqs.delete("q", &legacy.receipt_handle).unwrap();
        lqs.set_queue_attributes(
            "q",
            QueueUpdate {
                delay_ms: Some(1000),
                message_retention_ms: Some(60_000),
                maximum_message_size: Some(1024),
                ..QueueUpdate::default()
            },
            30_000,
        )
        .unwrap();
        lqs.send("q", SendRequest::standard("new"), 30_000).unwrap();
    }
    {
        let mut lqs = Lqs::open(&path).unwrap();
        let config = lqs.queue_config("q").unwrap();
        assert_eq!(
            (
                config.delay_ms,
                config.message_retention_ms,
                config.maximum_message_size
            ),
            (1000, 60_000, 1024)
        );
        assert!(lqs.receive("q", 1, 30_999).unwrap().is_empty());
        assert_eq!(lqs.receive("q", 1, 31_000).unwrap()[0].body, "new");
    }
    {
        let mut lqs = Lqs::open(&path).unwrap();
        assert!(lqs.receive("q", 1, 90_000).unwrap().is_empty());
        assert_eq!(lqs.queue_depth("q").unwrap(), 0);
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn dlq_retention_uses_standard_original_time_and_fifo_transfer_time() {
    for (source, dead, kind) in [
        ("s", "d", QueueType::Standard),
        ("s.fifo", "d.fifo", QueueType::Fifo),
    ] {
        let mut lqs = Lqs::new();
        lqs.create_queue(
            dead,
            kind,
            QueueOptions {
                message_retention_ms: 60_000,
                ..QueueOptions::default()
            },
        )
        .unwrap();
        lqs.create_queue(
            source,
            kind,
            QueueOptions {
                content_based_deduplication: true,
                message_retention_ms: 120_000,
                redrive_policy: Some(RedrivePolicy {
                    dead_letter_queue: dead.into(),
                    max_receive_count: 1,
                }),
                ..QueueOptions::default()
            },
        )
        .unwrap();
        lqs.send(source, SendRequest::fifo("failed", "a"), 0)
            .unwrap();
        lqs.receive(source, 1, 0).unwrap();
        assert!(lqs.receive(source, 1, 60_000).unwrap().is_empty());
        let received = lqs.receive(dead, 1, 60_000).unwrap();
        assert_eq!(received.len(), usize::from(kind == QueueType::Fifo));
        // Expired dead letters must not be resurrected by redrive.
        assert_eq!(
            lqs.redrive_dead_letters(dead, source, 10, 120_000).unwrap(),
            0
        );
        assert_eq!(lqs.queue_depth(dead).unwrap(), 0);
        lqs.send(source, SendRequest::fifo("expired-in-source", "b"), 120_000)
            .unwrap();
        lqs.receive(source, 1, 120_000).unwrap();
        lqs.receive(source, 1, 240_000).unwrap();
        assert_eq!(lqs.queue_depth(source).unwrap(), 0);
        assert_eq!(lqs.queue_depth(dead).unwrap(), 0);
    }
}

#[test]
fn retention_update_expires_existing_messages_without_resetting_deduplication() {
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
    let first = lqs
        .send("q.fifo", SendRequest::fifo("same", "a"), 0)
        .unwrap();
    lqs.set_queue_attributes(
        "q.fifo",
        QueueUpdate {
            message_retention_ms: Some(60_000),
            ..QueueUpdate::default()
        },
        60_000,
    )
    .unwrap();
    assert_eq!(lqs.queue_depth("q.fifo").unwrap(), 0);
    let duplicate = lqs
        .send("q.fifo", SendRequest::fifo("same", "a"), 60_000)
        .unwrap();
    assert!(duplicate.deduplicated);
    assert_eq!(duplicate.message_id, first.message_id);
    assert!(lqs.receive("q.fifo", 1, 60_000).unwrap().is_empty());
}

fn options() -> QueueOptions {
    QueueOptions {
        message_retention_ms: 60_000,
        delay_ms: 1000,
        ..QueueOptions::default()
    }
}

#[test]
fn standard_delay_override_and_visibility_are_independent() {
    let mut lqs = Lqs::new();
    lqs.create_queue("q", QueueType::Standard, options())
        .unwrap();
    lqs.send("q", SendRequest::standard("queue-delay"), 0)
        .unwrap();
    let mut immediate = SendRequest::standard("immediate");
    immediate.delay_ms = Some(0);
    lqs.send("q", immediate, 0).unwrap();
    let mut delayed = SendRequest::standard("custom-delay");
    delayed.delay_ms = Some(2000);
    lqs.send("q", delayed, 0).unwrap();
    assert_eq!(lqs.receive("q", 10, 999).unwrap()[0].body, "immediate");
    let first = lqs.receive("q", 10, 1000).unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].body, "queue-delay");
    lqs.change_visibility("q", &first[0].receipt_handle, 0, 1001)
        .unwrap();
    let retry = lqs.receive("q", 10, 1001).unwrap();
    assert_eq!(retry[0].receive_count, 2);
    assert_eq!(retry[0].body, "queue-delay");
    assert!(lqs.receive("q", 10, 1999).unwrap().is_empty());
    assert_eq!(lqs.receive("q", 10, 2000).unwrap()[0].body, "custom-delay");
}

#[test]
fn fifo_queue_delay_preserves_group_order_and_rejects_overrides() {
    let mut lqs = Lqs::new();
    lqs.create_queue(
        "q.fifo",
        QueueType::Fifo,
        QueueOptions {
            content_based_deduplication: true,
            ..options()
        },
    )
    .unwrap();
    for delay in [0, 1, 900_000] {
        let mut invalid = SendRequest::fifo("invalid", "a");
        invalid.delay_ms = Some(delay);
        assert!(matches!(
            lqs.send("q.fifo", invalid, 0),
            Err(LqsError::InvalidDeliveryOptions(_))
        ));
    }
    lqs.send("q.fifo", SendRequest::fifo("a1", "a"), 0).unwrap();
    lqs.send("q.fifo", SendRequest::fifo("a2", "a"), 1).unwrap();
    assert!(lqs.receive("q.fifo", 10, 999).unwrap().is_empty());
    let first = lqs.receive("q.fifo", 10, 1001).unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].body, "a1");
    lqs.delete("q.fifo", &first[0].receipt_handle).unwrap();
    assert_eq!(lqs.receive("q.fifo", 10, 1002).unwrap()[0].body, "a2");
}

#[test]
fn expiration_covers_available_delayed_and_inflight_and_unblocks_fifo() {
    let mut lqs = Lqs::new();
    for (queue, kind, delay) in [
        ("visible", QueueType::Standard, 0),
        ("delayed", QueueType::Standard, 900_000),
        ("inflight.fifo", QueueType::Fifo, 0),
    ] {
        lqs.create_queue(
            queue,
            kind,
            QueueOptions {
                delay_ms: delay,
                content_based_deduplication: true,
                ..options()
            },
        )
        .unwrap();
        lqs.send(queue, SendRequest::fifo("old", "a"), 0).unwrap();
        if kind == QueueType::Fifo {
            let first = lqs.receive(queue, 1, 0).unwrap().remove(0);
            lqs.change_visibility(queue, &first.receipt_handle, 120_000, 0)
                .unwrap();
            lqs.send(queue, SendRequest::fifo("new", "a"), 1000)
                .unwrap();
            assert!(lqs.receive(queue, 10, 59_999).unwrap().is_empty());
            assert_eq!(lqs.receive(queue, 10, 60_000).unwrap()[0].body, "new");
        } else {
            if delay == 0 {
                assert_eq!(lqs.receive(queue, 1, 59_999).unwrap().len(), 1);
            }
            assert!(lqs.receive(queue, 10, 60_000).unwrap().is_empty());
            assert_eq!(lqs.queue_depth(queue).unwrap(), 0);
        }
    }
    // Send performs physical cleanup, including expired invisible messages.
    lqs.send("inflight.fifo", SendRequest::fifo("latest", "b"), 61_000)
        .unwrap();
    assert_eq!(lqs.queue_depth("inflight.fifo").unwrap(), 1);
}

#[test]
fn body_limits_count_utf8_bytes_and_reject_before_fifo_deduplication() {
    let mut lqs = Lqs::new();
    lqs.create_queue("q", QueueType::Standard, QueueOptions::default())
        .unwrap();
    for size in [1, MAX_MESSAGE_BYTES] {
        lqs.send("q", SendRequest::standard("a".repeat(size)), 0)
            .unwrap();
    }
    for size in [0, MAX_MESSAGE_BYTES + 1] {
        assert!(matches!(
            lqs.send("q", SendRequest::standard("a".repeat(size)), 0),
            Err(LqsError::InvalidMessageSize { .. })
        ));
    }
    lqs.create_queue(
        "small.fifo",
        QueueType::Fifo,
        QueueOptions {
            maximum_message_size: 1024,
            ..QueueOptions::default()
        },
    )
    .unwrap();
    let mut req = SendRequest::fifo("é".repeat(512), "a");
    req.deduplication_id = Some("same".into());
    lqs.send("small.fifo", req.clone(), 0).unwrap();
    req.body.push('é');
    assert_eq!(
        lqs.send("small.fifo", req, 0),
        Err(LqsError::InvalidMessageSize {
            size: 1026,
            maximum: 1024
        })
    );
    assert_eq!(lqs.queue_depth("small.fifo").unwrap(), 1);
}

#[test]
fn settings_updates_are_atomic_and_fifo_delay_is_retroactive() {
    for (name, kind) in [("s", QueueType::Standard), ("f.fifo", QueueType::Fifo)] {
        let mut lqs = Lqs::new();
        lqs.create_queue(
            name,
            kind,
            QueueOptions {
                content_based_deduplication: true,
                ..options()
            },
        )
        .unwrap();
        lqs.send(name, SendRequest::fifo("first", "a"), 0).unwrap();
        lqs.set_queue_attributes(
            name,
            QueueUpdate {
                delay_ms: Some(2000),
                ..QueueUpdate::default()
            },
            500,
        )
        .unwrap();
        let received = lqs.receive(name, 10, 1000).unwrap();
        assert_eq!(received.len(), usize::from(kind == QueueType::Standard));
        if kind == QueueType::Fifo {
            assert_eq!(lqs.receive(name, 10, 2000).unwrap().len(), 1);
        }
        for update in [
            QueueUpdate {
                delay_ms: Some(900_001),
                ..QueueUpdate::default()
            },
            QueueUpdate {
                message_retention_ms: Some(59_999),
                ..QueueUpdate::default()
            },
            QueueUpdate {
                message_retention_ms: Some(1_209_600_001),
                ..QueueUpdate::default()
            },
            QueueUpdate {
                maximum_message_size: Some(1023),
                ..QueueUpdate::default()
            },
            QueueUpdate {
                maximum_message_size: Some(MAX_MESSAGE_BYTES + 1),
                ..QueueUpdate::default()
            },
            QueueUpdate {
                delay_ms: Some(0),
                redrive_policy: Some(Some(RedrivePolicy {
                    dead_letter_queue: "missing".into(),
                    max_receive_count: 1,
                })),
                ..QueueUpdate::default()
            },
        ] {
            let before = lqs.queue_config(name).unwrap();
            assert!(lqs.set_queue_attributes(name, update, 2001).is_err());
            assert_eq!(lqs.queue_config(name).unwrap(), before);
        }
    }
}
