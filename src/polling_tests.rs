use super::*;

fn temporary_database() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "lqs-poll-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn inflight_capacity_is_atomic_and_queue_scoped_for_both_types() {
    for (name, kind) in [("q", QueueType::Standard), ("q.fifo", QueueType::Fifo)] {
        let mut lqs = Lqs::new();
        lqs.create_queue(
            name,
            kind,
            QueueOptions {
                max_in_flight: 2,
                content_based_deduplication: true,
                ..QueueOptions::default()
            },
        )
        .unwrap();
        lqs.create_queue("other", QueueType::Standard, QueueOptions::default())
            .unwrap();
        for i in 0..3 {
            lqs.send(name, SendRequest::fifo(i.to_string(), i.to_string()), 0)
                .unwrap();
        }
        for invalid in [0, 11, usize::MAX] {
            assert_eq!(
                lqs.receive(name, invalid, 0),
                Err(LqsError::InvalidReceiveOptions)
            );
        }
        assert_eq!(lqs.in_flight_count(name, 0).unwrap(), 0);
        let first = lqs.receive(name, 10, 0).unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(lqs.in_flight_count(name, 0).unwrap(), 2);
        match kind {
            QueueType::Standard => assert_eq!(lqs.receive(name, 1, 1), Err(LqsError::OverLimit)),
            QueueType::Fifo => assert!(lqs.receive(name, 1, 1).unwrap().is_empty()),
        }
        lqs.send("other", SendRequest::standard("independent"), 1)
            .unwrap();
        assert_eq!(lqs.receive("other", 1, 1).unwrap().len(), 1);
        lqs.delete(name, &first[0].receipt_handle).unwrap();
        assert_eq!(lqs.in_flight_count(name, 1).unwrap(), 1);
        assert_eq!(lqs.receive(name, 10, 1).unwrap()[0].body, "2");
        lqs.change_visibility(name, &first[1].receipt_handle, 0, 2)
            .unwrap();
        assert_eq!(lqs.in_flight_count(name, 2).unwrap(), 1);
        let retry = lqs.receive(name, 10, 2).unwrap();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].receive_count, 2);
        assert_eq!(lqs.in_flight_count(name, 30_001).unwrap(), 1);
        assert_eq!(lqs.in_flight_count(name, 30_002).unwrap(), 0);
        // An expired receipt cannot be revived to bypass admission control.
        assert_eq!(
            lqs.change_visibility(name, &retry[0].receipt_handle, 10_000, 30_002),
            Err(LqsError::MessageNotInflight)
        );
    }
}

#[test]
fn retention_delay_and_quota_changes_do_not_leave_stale_counts() {
    let mut lqs = Lqs::new();
    lqs.create_queue(
        "q",
        QueueType::Standard,
        QueueOptions {
            max_in_flight: 2,
            message_retention_ms: 60_000,
            visibility_timeout_ms: 120_000,
            ..QueueOptions::default()
        },
    )
    .unwrap();
    for body in ["a", "b"] {
        lqs.send("q", SendRequest::standard(body), 0).unwrap();
    }
    lqs.receive("q", 10, 0).unwrap();
    lqs.set_queue_attributes(
        "q",
        QueueUpdate {
            max_in_flight: Some(1),
            ..QueueUpdate::default()
        },
        1,
    )
    .unwrap();
    assert_eq!(lqs.in_flight_count("q", 1).unwrap(), 2); // Existing deliveries are not revoked.
    assert_eq!(lqs.receive("q", 1, 1), Err(LqsError::OverLimit));
    let mut delayed = SendRequest::standard("delayed");
    delayed.delay_ms = Some(1000);
    lqs.send("q", delayed, 59_999).unwrap();
    assert_eq!(lqs.in_flight_count("q", 59_999).unwrap(), 2);
    assert_eq!(lqs.in_flight_count("q", 60_000).unwrap(), 0);
    assert!(lqs.receive("q", 10, 60_000).unwrap().is_empty());
    assert_eq!(lqs.receive("q", 10, 60_999).unwrap()[0].body, "delayed");
    assert_eq!(lqs.in_flight_count("q", 60_999).unwrap(), 1);
}

#[test]
fn polling_settings_validate_migrate_and_persist_without_resetting_inflight() {
    let path = temporary_database();
    {
        let mut lqs = Lqs::open(&path).unwrap();
        lqs.create_queue("q", QueueType::Standard, QueueOptions::default())
            .unwrap();
        lqs.send("q", SendRequest::standard("keep"), 0).unwrap();
        lqs.receive("q", 1, 0).unwrap();
        lqs.connection.execute_batch("ALTER TABLE queues DROP COLUMN receive_wait_time_ms; ALTER TABLE queues DROP COLUMN max_in_flight; DROP INDEX messages_by_queue_inflight;").unwrap();
    }
    {
        let mut lqs = Lqs::open(&path).unwrap();
        let config = lqs.queue_config("q").unwrap();
        assert_eq!(config.receive_wait_time_ms, 0);
        assert_eq!(config.max_in_flight, 120_000);
        assert_eq!(lqs.in_flight_count("q", 1).unwrap(), 1);
        for (wait, cap) in [(20_001, 1), (0, 0), (0, 120_001)] {
            assert_eq!(
                lqs.create_queue(
                    "bad",
                    QueueType::Standard,
                    QueueOptions {
                        receive_wait_time_ms: wait,
                        max_in_flight: cap,
                        ..QueueOptions::default()
                    }
                ),
                Err(LqsError::InvalidReceiveOptions)
            );
            assert!(matches!(
                lqs.queue_config("bad"),
                Err(LqsError::QueueNotFound(_))
            ));
            assert_eq!(
                lqs.set_queue_attributes(
                    "q",
                    QueueUpdate {
                        delay_ms: Some(1000),
                        receive_wait_time_ms: Some(wait),
                        max_in_flight: Some(cap),
                        ..QueueUpdate::default()
                    },
                    1
                ),
                Err(LqsError::InvalidReceiveOptions)
            );
            assert_eq!(lqs.queue_config("q").unwrap(), config);
        }
        lqs.set_queue_attributes(
            "q",
            QueueUpdate {
                receive_wait_time_ms: Some(20_000),
                max_in_flight: Some(1),
                ..QueueUpdate::default()
            },
            1,
        )
        .unwrap();
    }
    {
        let mut lqs = Lqs::open(&path).unwrap();
        let config = lqs.queue_config("q").unwrap();
        assert_eq!(
            (config.receive_wait_time_ms, config.max_in_flight),
            (20_000, 1)
        );
        assert_eq!(lqs.receive("q", 1, 1), Err(LqsError::OverLimit));
        assert_eq!(lqs.in_flight_count("q", 30_000).unwrap(), 0);
        assert_eq!(lqs.receive("q", 1, 30_000).unwrap()[0].receive_count, 2);
    }
    std::fs::remove_file(path).unwrap();
}

#[test]
fn concurrent_connections_cannot_over_admit_messages() {
    let path = temporary_database();
    let mut lqs = Lqs::open(&path).unwrap();
    lqs.create_queue(
        "q",
        QueueType::Standard,
        QueueOptions {
            max_in_flight: 1,
            ..QueueOptions::default()
        },
    )
    .unwrap();
    for i in 0..4 {
        lqs.send("q", SendRequest::standard(i.to_string()), 0)
            .unwrap();
    }
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
    let connections: Vec<_> = (0..4).map(|_| Lqs::open(&path).unwrap()).collect();
    let threads: Vec<_> = connections
        .into_iter()
        .map(|mut connection| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                connection.receive("q", 10, 1)
            })
        })
        .collect();
    let mut delivered = 0;
    for thread in threads {
        match thread.join().unwrap() {
            Ok(messages) => delivered += messages.len(),
            Err(error) => assert_eq!(error, LqsError::OverLimit),
        }
    }
    assert_eq!(delivered, 1);
    assert_eq!(lqs.in_flight_count("q", 1).unwrap(), 1);
    drop(lqs);
    std::fs::remove_file(path).unwrap();
}
