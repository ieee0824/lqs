use super::*;
use crate::{MessageAttribute, MessageAttributeValue};
use tower::ServiceExt;

fn paths() -> (std::path::PathBuf, std::path::PathBuf) {
    let directory = std::env::temp_dir().join(format!(
        "lqs-encryption-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&directory).unwrap();
    (directory.join("messages.sqlite"), directory)
}

fn contains(path: &Path, marker: &[u8]) -> bool {
    fs::read(path)
        .map(|data| data.windows(marker.len()).any(|window| window == marker))
        .unwrap_or(false)
}

#[test]
fn encrypted_database_and_wal_hide_message_attributes_and_fifo_replay() {
    let (path, directory) = paths();
    let key = [b'k'; 32];
    let body = "ENCRYPTED_BODY_MARKER_754";
    let attribute = "ENCRYPTED_ATTRIBUTE_MARKER_755";
    let mut lqs = Lqs::open_encrypted(&path, &key).unwrap();
    assert_eq!(lqs.storage_encryption(), StorageEncryption::SqlCipher);
    lqs.create_queue("q.fifo", QueueType::Fifo, QueueOptions::default())
        .unwrap();
    let mut request = SendRequest::fifo(body, "group");
    request.deduplication_id = Some("unique".into());
    request.message_attributes.insert(
        "label".into(),
        MessageAttribute {
            data_type: "String".into(),
            value: MessageAttributeValue::String(attribute.into()),
        },
    );
    lqs.send("q.fifo", request, 1).unwrap();
    let options = ReceiveOptions {
        receive_request_attempt_id: Some("replay".into()),
        ..ReceiveOptions::default()
    };
    let received = lqs
        .receive_with_options("q.fifo", 1, options.clone(), 2)
        .unwrap();
    assert_eq!(received[0].body, body);
    assert_eq!(
        received[0].message_attributes["label"].value,
        MessageAttributeValue::String(attribute.into())
    );
    for file in [path.clone(), path.with_extension("sqlite-wal")] {
        assert!(file.exists(), "{}", file.display());
        #[cfg(unix)]
        assert_eq!(fs::metadata(&file).unwrap().permissions().mode() & 0o077, 0);
        assert!(!contains(&file, body.as_bytes()), "{}", file.display());
        assert!(!contains(&file, attribute.as_bytes()), "{}", file.display());
    }
    drop(lqs);
    assert!(Lqs::open(&path).is_err());
    assert!(Lqs::open_encrypted(&path, &[b'x'; 32]).is_err());
    let mut reopened = Lqs::open_encrypted(&path, &key).unwrap();
    let replay = reopened
        .receive_with_options("q.fifo", 1, options, 3)
        .unwrap();
    assert_eq!(replay, received);
    drop(reopened);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn encrypted_mode_rejects_plaintext_database_instead_of_rewriting_it() {
    let (path, directory) = paths();
    let mut plain = Lqs::open(&path).unwrap();
    plain
        .create_queue("q", QueueType::Standard, QueueOptions::default())
        .unwrap();
    drop(plain);
    #[cfg(unix)]
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(Lqs::open_encrypted(&path, &[b'k'; 32]).is_err());
    assert!(Lqs::open(&path).unwrap().queue_exists("q").unwrap());
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn offline_migration_copies_plaintext_to_new_encrypted_database() {
    let (source, directory) = paths();
    let target = directory.join("encrypted.sqlite");
    let marker = "MIGRATION_MARKER_756";
    let mut plain = Lqs::open(&source).unwrap();
    plain
        .create_queue("q", QueueType::Standard, QueueOptions::default())
        .unwrap();
    plain.send("q", SendRequest::standard(marker), 1).unwrap();
    drop(plain);
    migrate_plaintext_database(&source, &target, &[b'k'; 32]).unwrap();
    assert!(contains(&source, marker.as_bytes()));
    assert!(!contains(&target, marker.as_bytes()));
    let mut encrypted = Lqs::open_encrypted(&target, &[b'k'; 32]).unwrap();
    assert_eq!(encrypted.receive("q", 1, 2).unwrap()[0].body, marker);
    assert!(migrate_plaintext_database(&source, &target, &[b'k'; 32]).is_err());
    drop(encrypted);
    fs::remove_dir_all(directory).unwrap();
}

#[cfg(unix)]
#[test]
fn key_file_and_database_permissions_are_owner_only() {
    let (path, directory) = paths();
    let key_path = directory.join("key");
    fs::write(&key_path, [b'k'; 32]).unwrap();
    fs::set_permissions(&key_path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(read_database_key_file(&key_path).is_err());
    fs::set_permissions(&key_path, fs::Permissions::from_mode(0o600)).unwrap();
    let key = read_database_key_file(&key_path).unwrap();
    drop(Lqs::open_encrypted(&path, &key).unwrap());
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(Lqs::open_encrypted(&path, &key).is_err());
    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn status_api_separates_storage_encryption_from_sqs_settings() {
    async fn status(lqs: Lqs) -> serde_json::Value {
        let response = crate::router(lqs, "http://localhost")
            .oneshot(
                axum::http::Request::builder()
                    .uri("/status")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap(),
        )
        .unwrap()
    }
    assert_eq!(status(Lqs::new()).await["storageEncryption"], "none");
    let (path, directory) = paths();
    let encrypted = Lqs::open_encrypted(&path, &[b'k'; 32]).unwrap();
    let actual = status(encrypted).await;
    assert_eq!(actual["storageEncryption"], "sqlcipher");
    assert_eq!(actual["sqsEncryptionSettings"], "simulation");
    fs::remove_dir_all(directory).unwrap();
}
