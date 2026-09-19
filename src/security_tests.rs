use super::*;
use serde_json::json;

fn policy() -> String {
    json!({"Version":"2012-10-17", "Statement":[
        {"Sid":"Admin", "Effect":"Allow", "Principal":{"AWS":"111111111111"}, "Action":"sqs:*", "Resource":"*"},
        {"Sid":"Producer", "Effect":"Allow", "Principal":{"AWS":"222222222222"}, "Action":"sqs:Send*", "Resource":"arn:aws:sqs:us-east-1:000000000000:q*"},
        {"Sid":"DenyDelete", "Effect":"Deny", "Principal":"*", "Action":"sqs:DeleteMessage", "Resource":"*"}
    ]}).to_string()
}

#[test]
fn policies_match_principals_resources_actions_and_deny_overrides_allow() {
    let security = QueueSecurity {
        policy: Some(policy()),
        ..QueueSecurity::default()
    };
    let queue = "arn:aws:sqs:us-east-1:000000000000:q";
    let admin = RequestIdentity::aws("arn:aws:iam::111111111111:user/admin").unwrap();
    let producer = RequestIdentity::aws("222222222222").unwrap();
    assert!(security.authorize(&admin, "ReceiveMessage", queue).is_ok());
    assert!(security.authorize(&producer, "SendMessage", queue).is_ok());
    assert!(
        security
            .authorize(&producer, "SendMessageBatch", queue)
            .is_ok()
    );
    for identity in [&admin, &producer, &RequestIdentity::Anonymous] {
        assert_eq!(
            security.authorize(identity, "DeleteMessage", queue),
            Err(LqsError::AccessDenied)
        );
        assert_eq!(
            security.authorize(identity, "DeleteMessageBatch", queue),
            Err(LqsError::AccessDenied)
        );
    }
    assert_eq!(
        security.authorize(&producer, "ReceiveMessage", queue),
        Err(LqsError::AccessDenied)
    );
    assert_eq!(
        security.authorize(&producer, "SetQueueAttributes", queue),
        Err(LqsError::AccessDenied)
    );
    assert_eq!(
        security.authorize(
            &producer,
            "SendMessage",
            "arn:aws:sqs:us-east-1:000000000000:other"
        ),
        Err(LqsError::AccessDenied)
    );
    assert_eq!(
        security.authorize(&RequestIdentity::Anonymous, "SendMessage", queue),
        Err(LqsError::AccessDenied)
    );
    // An account-root principal also represents identities in that account.
    let root = QueueSecurity {policy:Some(json!({"Statement":{"Effect":"Allow", "Principal":{"AWS":"arn:aws:iam::222222222222:root"}, "Action":"sqs:SendMessage", "Resource":queue}}).to_string()), ..QueueSecurity::default()};
    assert!(
        root.authorize(
            &RequestIdentity::aws("arn:aws:iam::222222222222:role/producer").unwrap(),
            "SendMessage",
            queue
        )
        .is_ok()
    );
}

#[test]
fn malformed_or_unsupported_policies_fail_closed() {
    let valid = json!({"Statement":[{"Effect":"Allow", "Principal":"*", "Action":"sqs:SendMessage", "Resource":"*"}]});
    for (field, value) in [
        ("Condition", json!({"Bool":{"aws:SecureTransport":"true"}})),
        ("NotPrincipal", json!("*")),
        ("NotAction", json!("sqs:DeleteMessage")),
        ("Effect", json!("allow")),
        ("Principal", json!({"Service":"sns.amazonaws.com"})),
        ("Action", json!("s3:*")),
        (
            "Resource",
            json!("arn:aws:sqs:us-east-1:000000000000:${queue}"),
        ),
    ] {
        let mut policy = valid.clone();
        policy["Statement"][0][field] = value;
        let security = QueueSecurity {
            policy: Some(policy.to_string()),
            ..QueueSecurity::default()
        };
        assert!(security.validate().is_err(), "{field}");
        assert!(
            security
                .authorize(&RequestIdentity::Anonymous, "SendMessage", "*")
                .is_err()
        );
    }
    for bad in [
        "null".to_owned(),
        "{}".into(),
        "not json".into(),
        " ".repeat(8193),
    ] {
        assert!(
            QueueSecurity {
                policy: Some(bad),
                ..QueueSecurity::default()
            }
            .validate()
            .is_err()
        );
    }
    let deny_all = QueueSecurity {
        policy: Some("{\"Statement\":[]}".into()),
        ..QueueSecurity::default()
    };
    assert_eq!(
        deny_all.authorize(&RequestIdentity::Anonymous, "SendMessage", "*"),
        Err(LqsError::AccessDenied)
    );
    assert!(
        QueueSecurity::default()
            .authorize(&RequestIdentity::Anonymous, "SendMessage", "*")
            .is_ok()
    );
}

#[test]
fn permissions_merge_remove_labels_preserve_denies_and_validate_atomically() {
    let mut lqs = Lqs::new();
    lqs.create_queue(
        "q",
        QueueType::Standard,
        QueueOptions {
            security: QueueSecurity {
                policy: Some(policy()),
                ..QueueSecurity::default()
            },
            ..QueueOptions::default()
        },
    )
    .unwrap();
    lqs.add_permission(
        "q",
        "Consumer",
        &["333333333333".into()],
        &["ReceiveMessage".into(), "DeleteMessage".into()],
        1000,
    )
    .unwrap();
    let consumer = RequestIdentity::aws("333333333333").unwrap();
    assert!(
        lqs.authorize_queue("q", &consumer, "ReceiveMessage")
            .is_ok()
    );
    assert_eq!(
        lqs.authorize_queue("q", &consumer, "DeleteMessage"),
        Err(LqsError::AccessDenied)
    );
    let before = lqs.queue_security("q").unwrap();
    for (label, account, action) in [
        ("Consumer", "333333333333", "ReceiveMessage"),
        ("bad label", "333333333333", "ReceiveMessage"),
        ("New", "*", "ReceiveMessage"),
        ("New", "333333333333", "unknown"),
    ] {
        assert!(
            lqs.add_permission("q", label, &[account.into()], &[action.into()], 2000)
                .is_err()
        );
        assert_eq!(lqs.queue_security("q").unwrap(), before);
        assert_eq!(lqs.queue_metrics("q", 2000).unwrap().modified_at_ms, 1000);
    }
    lqs.remove_permission("q", "Consumer", 2000).unwrap();
    assert_eq!(
        lqs.authorize_queue("q", &consumer, "ReceiveMessage"),
        Err(LqsError::AccessDenied)
    );
    assert!(
        lqs.queue_security("q")
            .unwrap()
            .policy
            .unwrap()
            .contains("DenyDelete")
    );
    lqs.remove_permission("q", "missing", 2001).unwrap();
}

#[test]
fn encryption_configuration_switches_modes_without_claiming_payload_encryption() {
    let mut lqs = Lqs::new();
    lqs.create_queue("q", QueueType::Standard, QueueOptions::default())
        .unwrap();
    lqs.set_queue_attributes(
        "q",
        QueueUpdate {
            security: SecurityUpdate {
                sqs_managed_sse_enabled: Some(true),
                ..SecurityUpdate::default()
            },
            ..QueueUpdate::default()
        },
        1,
    )
    .unwrap();
    assert!(lqs.queue_security("q").unwrap().sqs_managed_sse_enabled);
    lqs.send("q", SendRequest::standard("plaintext"), 1)
        .unwrap();
    assert_eq!(
        lqs.receive("q", 1, 1).unwrap()[0].system_attributes["SqsManagedSseEnabled"],
        "false"
    );
    let before = lqs.queue_security("q").unwrap();
    for security in [
        SecurityUpdate {
            kms_master_key_id: Some("alias/example".into()),
            sqs_managed_sse_enabled: Some(true),
            ..SecurityUpdate::default()
        },
        SecurityUpdate {
            kms_data_key_reuse_period_seconds: Some(59),
            ..SecurityUpdate::default()
        },
        SecurityUpdate {
            policy: Some(Some("bad JSON".into())),
            ..SecurityUpdate::default()
        },
    ] {
        assert!(
            lqs.set_queue_attributes(
                "q",
                QueueUpdate {
                    security,
                    delay_ms: Some(1000),
                    ..QueueUpdate::default()
                },
                2
            )
            .is_err()
        );
        assert_eq!(lqs.queue_security("q").unwrap(), before);
        assert_eq!(lqs.queue_config("q").unwrap().delay_ms, 0);
    }
    lqs.set_queue_attributes(
        "q",
        QueueUpdate {
            security: SecurityUpdate {
                kms_master_key_id: Some("alias/example".into()),
                kms_data_key_reuse_period_seconds: Some(86400),
                ..SecurityUpdate::default()
            },
            ..QueueUpdate::default()
        },
        3,
    )
    .unwrap();
    let kms = lqs.queue_security("q").unwrap();
    assert!(!kms.sqs_managed_sse_enabled);
    assert_eq!(kms.kms_master_key_id.as_deref(), Some("alias/example"));
    lqs.set_queue_attributes(
        "q",
        QueueUpdate {
            security: SecurityUpdate {
                sqs_managed_sse_enabled: Some(true),
                ..SecurityUpdate::default()
            },
            ..QueueUpdate::default()
        },
        4,
    )
    .unwrap();
    assert!(lqs.queue_security("q").unwrap().kms_master_key_id.is_none());
    lqs.set_queue_attributes(
        "q",
        QueueUpdate {
            security: SecurityUpdate {
                sqs_managed_sse_enabled: Some(false),
                ..SecurityUpdate::default()
            },
            ..QueueUpdate::default()
        },
        5,
    )
    .unwrap();
    assert!(!lqs.queue_security("q").unwrap().sqs_managed_sse_enabled);
}

#[test]
fn security_settings_migrate_persist_and_are_deleted_with_queue() {
    let path = std::env::temp_dir().join(format!(
        "lqs-security-{}-{}.sqlite",
        std::process::id(),
        management::now_ms()
    ));
    {
        let mut lqs = Lqs::open(&path).unwrap();
        lqs.create_queue("q", QueueType::Standard, QueueOptions::default())
            .unwrap();
        lqs.connection
            .execute_batch("DROP TABLE queue_security;")
            .unwrap();
    }
    {
        let mut lqs = Lqs::open(&path).unwrap();
        assert_eq!(lqs.queue_security("q").unwrap(), QueueSecurity::default());
        lqs.set_queue_attributes(
            "q",
            QueueUpdate {
                security: SecurityUpdate {
                    policy: Some(Some(policy())),
                    kms_master_key_id: Some("alias/example".into()),
                    kms_data_key_reuse_period_seconds: Some(60),
                    ..SecurityUpdate::default()
                },
                ..QueueUpdate::default()
            },
            1000,
        )
        .unwrap();
        lqs.purge_queue("q", 1001).unwrap();
    }
    {
        let mut lqs = Lqs::open(&path).unwrap();
        assert_eq!(lqs.queue_security("q").unwrap().policy, Some(policy()));
        assert_eq!(
            lqs.queue_security("q")
                .unwrap()
                .kms_master_key_id
                .as_deref(),
            Some("alias/example")
        );
        assert_eq!(
            lqs.queue_security("q")
                .unwrap()
                .kms_data_key_reuse_period_seconds,
            60
        );
        assert_eq!(
            lqs.authorize_queue("q", &RequestIdentity::Anonymous, "SendMessage"),
            Err(LqsError::AccessDenied)
        );
        lqs.delete_queue("q", 2000).unwrap();
        let count: usize = lqs
            .connection
            .query_row("SELECT COUNT(*) FROM queue_security", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }
    std::fs::remove_file(path).unwrap();
}
