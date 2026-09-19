use super::*;
use crate::{BatchEntry, MessageAttribute, MessageAttributeValue};

fn text(kind: &str, value: &str) -> MessageAttribute {
    MessageAttribute {
        data_type: kind.into(),
        value: MessageAttributeValue::String(value.into()),
    }
}
fn attributes() -> MessageAttributes {
    MessageAttributes::from([
        ("text".into(), text("String.custom", "日本語<&>")),
        (
            "number".into(),
            text("Number.int", "12345678901234567890123456789012345678"),
        ),
        (
            "binary".into(),
            MessageAttribute {
                data_type: "Binary.image".into(),
                value: MessageAttributeValue::Binary(vec![0, 255, 1, 128]),
            },
        ),
    ])
}
fn path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "lqs-attrs-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn attributes_and_system_metadata_survive_restart_and_retries() {
    let path = path();
    let sent;
    {
        let mut lqs = Lqs::open(&path).unwrap();
        lqs.create_queue(
            "q.fifo",
            QueueType::Fifo,
            QueueOptions {
                visibility_timeout_ms: 10,
                ..QueueOptions::default()
            },
        )
        .unwrap();
        sent = lqs
            .send(
                "q.fifo",
                SendRequest {
                    message_attributes: attributes(),
                    deduplication_id: Some("explicit".into()),
                    ..SendRequest::fifo("body", "g")
                },
                50,
            )
            .unwrap();
    }
    for (now, count) in [(100, "1"), (110, "2")] {
        let mut lqs = Lqs::open(&path).unwrap();
        let received = lqs.receive("q.fifo", 1, now).unwrap().remove(0);
        assert_eq!(received.message_attributes, attributes());
        assert_eq!(
            sent.md5_of_message_attributes,
            message_attributes_md5(&received.message_attributes)
        );
        assert_eq!(received.system_attributes["SentTimestamp"], "50");
        assert_eq!(
            received.system_attributes["ApproximateFirstReceiveTimestamp"],
            "100"
        );
        assert_eq!(received.system_attributes["ApproximateReceiveCount"], count);
        assert_eq!(
            received.system_attributes["MessageDeduplicationId"],
            "explicit"
        );
        assert_eq!(received.system_attributes["MessageGroupId"], "g");
        assert_eq!(received.system_attributes["SequenceNumber"], "1");
    }
    std::fs::remove_file(path).unwrap();
}

#[test]
fn legacy_metadata_upgrade_preserves_counts_and_recovers_dedup_id() {
    let path = path();
    {
        let mut lqs = Lqs::open(&path).unwrap();
        lqs.create_queue(
            "q.fifo",
            QueueType::Fifo,
            QueueOptions {
                visibility_timeout_ms: 10,
                content_based_deduplication: true,
                ..QueueOptions::default()
            },
        )
        .unwrap();
        lqs.send("q.fifo", SendRequest::fifo("old", "g"), 0)
            .unwrap();
        lqs.receive("q.fifo", 1, 1).unwrap();
        lqs.connection.execute_batch("ALTER TABLE messages DROP COLUMN message_attributes; ALTER TABLE messages DROP COLUMN first_received_at_ms; ALTER TABLE messages DROP COLUMN message_deduplication_id; ALTER TABLE messages DROP COLUMN total_receive_count;").unwrap();
    }
    {
        let mut lqs = Lqs::open(&path).unwrap();
        let message = lqs.receive("q.fifo", 1, 11).unwrap().remove(0);
        assert!(message.message_attributes.is_empty());
        assert_eq!(message.system_attributes["SentTimestamp"], "0");
        assert_eq!(message.system_attributes["ApproximateReceiveCount"], "2");
        assert_eq!(
            message.system_attributes["ApproximateFirstReceiveTimestamp"],
            "11"
        ); // First known time for pre-metadata databases.
        assert_eq!(
            message.system_attributes["MessageDeduplicationId"],
            stable_content_id("old")
        );
    }
    std::fs::remove_file(path).unwrap();
}

#[test]
fn content_deduplication_ignores_attributes_but_validates_them_first() {
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
    let original = SendRequest {
        message_attributes: attributes(),
        ..SendRequest::fifo("body", "g")
    };
    let first = lqs.send("q.fifo", original.clone(), 0).unwrap();
    let mut changed = original.clone();
    changed
        .message_attributes
        .insert("text".into(), text("String.other", "changed"));
    let duplicate = lqs.send("q.fifo", changed, 1).unwrap();
    assert!(duplicate.deduplicated);
    assert_eq!(first.message_id, duplicate.message_id);
    assert_ne!(
        first.md5_of_message_attributes,
        duplicate.md5_of_message_attributes
    );
    let mut invalid = original.clone();
    invalid
        .message_attributes
        .insert("bad".into(), text("Number", "NaN"));
    assert!(matches!(
        lqs.send("q.fifo", invalid, 2),
        Err(LqsError::InvalidMessageAttributes(_))
    ));
    let received = lqs.receive("q.fifo", 1, 3).unwrap().remove(0);
    assert_eq!(received.message_attributes, attributes());
    assert_eq!(lqs.queue_depth("q.fifo").unwrap(), 1);
    let mut other = original;
    other.body = "different".into();
    assert!(!lqs.send("q.fifo", other, 4).unwrap().deduplicated);
}

#[test]
fn dlq_and_redrive_preserve_attributes_and_track_system_metadata() {
    for (kind, source, dlq) in [
        (QueueType::Standard, "s", "d"),
        (QueueType::Fifo, "s.fifo", "d.fifo"),
    ] {
        let mut lqs = Lqs::new();
        lqs.create_queue(
            dlq,
            kind,
            QueueOptions {
                visibility_timeout_ms: 10,
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
                redrive_policy: Some(RedrivePolicy {
                    dead_letter_queue: dlq.into(),
                    max_receive_count: 1,
                }),
                ..QueueOptions::default()
            },
        )
        .unwrap();
        lqs.send(
            source,
            SendRequest {
                message_attributes: attributes(),
                ..SendRequest::fifo("body", "g")
            },
            0,
        )
        .unwrap();
        lqs.receive(source, 1, 1).unwrap();
        assert!(lqs.receive(source, 1, 11).unwrap().is_empty());
        let dead = lqs.receive(dlq, 1, 12).unwrap().remove(0);
        assert_eq!(dead.message_attributes, attributes());
        assert_eq!(dead.system_attributes["ApproximateReceiveCount"], "2");
        assert_eq!(
            dead.system_attributes["ApproximateFirstReceiveTimestamp"],
            "1"
        );
        assert_eq!(
            dead.system_attributes["SentTimestamp"],
            if kind == QueueType::Fifo { "11" } else { "0" }
        );
        assert!(dead.system_attributes["DeadLetterQueueSourceArn"].ends_with(source));
        assert_eq!(lqs.redrive_dead_letters(dlq, source, 1, 22).unwrap(), 1);
        let redriven = lqs.receive(source, 1, 23).unwrap().remove(0);
        assert_eq!(redriven.message_attributes, attributes());
        assert_eq!(redriven.system_attributes["ApproximateReceiveCount"], "1");
        assert_eq!(
            redriven.system_attributes["ApproximateFirstReceiveTimestamp"],
            "23"
        );
        assert_eq!(redriven.system_attributes["SentTimestamp"], "22");
        assert!(
            !redriven
                .system_attributes
                .contains_key("DeadLetterQueueSourceArn")
        );
    }
}

#[test]
fn attribute_sizes_apply_to_individual_messages_and_whole_batches() {
    let mut lqs = Lqs::new();
    lqs.create_queue(
        "q",
        QueueType::Standard,
        QueueOptions {
            maximum_message_size: 1024,
            ..QueueOptions::default()
        },
    )
    .unwrap();
    let attrs = MessageAttributes::from([(
        "b".into(),
        MessageAttribute {
            data_type: "Binary.custom".into(),
            value: MessageAttributeValue::Binary(vec![0; 20]),
        },
    )]);
    let overhead = message_attributes_size(&attrs);
    let valid = SendRequest {
        message_attributes: attrs,
        ..SendRequest::standard("x".repeat(1024 - overhead))
    };
    lqs.send("q", valid.clone(), 0).unwrap();
    let mut oversized = valid.clone();
    oversized.body.push('x');
    assert!(matches!(
        lqs.send("q", oversized, 0),
        Err(LqsError::InvalidMessageSize { size: 1025, .. })
    ));
    lqs.create_queue("batch", QueueType::Standard, QueueOptions::default())
        .unwrap();
    let large = SendRequest {
        message_attributes: valid.message_attributes,
        ..SendRequest::standard("x".repeat(MAX_MESSAGE_BYTES / 2))
    };
    assert_eq!(
        lqs.send_batch(
            "batch",
            vec![
                BatchEntry {
                    id: "a".into(),
                    value: large.clone()
                },
                BatchEntry {
                    id: "b".into(),
                    value: large
                }
            ],
            0
        )
        .unwrap_err(),
        LqsError::InvalidBatch("BatchRequestTooLong")
    );
    assert_eq!(lqs.queue_depth("batch").unwrap(), 0);
}

#[test]
fn names_types_numbers_and_values_are_validated() {
    let mut lqs = Lqs::new();
    lqs.create_queue("q", QueueType::Standard, QueueOptions::default())
        .unwrap();
    let send = |lqs: &mut Lqs, name: &str, attribute: MessageAttribute| {
        lqs.send(
            "q",
            SendRequest {
                message_attributes: MessageAttributes::from([(name.into(), attribute)]),
                ..SendRequest::standard("body")
            },
            0,
        )
    };
    for name in [
        "",
        ".bad",
        "bad.",
        "bad..name",
        "AWS.x",
        "aMaZoN.x",
        "bad name",
        "日本語",
    ] {
        assert!(send(&mut lqs, name, text("String", "value")).is_err());
    }
    for attribute in [
        text("Unknown", "v"),
        text("String.", "v"),
        text("String", ""),
        text("String", "\0"),
        text("Binary", "wrong"),
        MessageAttribute {
            data_type: "String".into(),
            value: MessageAttributeValue::Binary(vec![1]),
        },
    ] {
        assert!(send(&mut lqs, "x", attribute).is_err());
    }
    for number in [
        "NaN",
        "inf",
        "1e127",
        "1e-129",
        "9e126",
        "123456789012345678901234567890123456789",
        "1.2.3",
        " 1",
        "1e",
        "--1",
    ] {
        assert!(
            send(&mut lqs, "n", text("Number", number)).is_err(),
            "{number}"
        );
    }
    for (number, expected) in [
        ("+001.2300", "1.23"),
        ("-0.000", "0"),
        ("1e2", "100"),
        ("-1e-2", "-0.01"),
    ] {
        send(&mut lqs, "n", text("Number.custom", number)).unwrap();
        let message = lqs.receive("q", 1, 0).unwrap().remove(0);
        assert_eq!(
            message.message_attributes["n"],
            text("Number.custom", expected)
        );
        lqs.delete("q", &message.receipt_handle).unwrap();
    }
    for number in [
        "1e-128",
        "1e126",
        "-1e126",
        "12345678901234567890123456789012345678",
    ] {
        send(&mut lqs, "n", text("Number", number)).unwrap();
    }
    let mut attrs = MessageAttributes::new();
    for i in 0..10 {
        attrs.insert(format!("n{i}"), text("String", "v"));
    }
    lqs.send(
        "q",
        SendRequest {
            message_attributes: attrs.clone(),
            ..SendRequest::standard("body")
        },
        0,
    )
    .unwrap();
    attrs.insert("eleven".into(), text("String", "v"));
    assert!(
        lqs.send(
            "q",
            SendRequest {
                message_attributes: attrs,
                ..SendRequest::standard("body")
            },
            0
        )
        .is_err()
    );
    assert!(send(&mut lqs, &"a".repeat(257), text("String", "v")).is_err());
    send(&mut lqs, &"a".repeat(256), text("String", "v")).unwrap();
}
