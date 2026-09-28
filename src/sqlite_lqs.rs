use std::fmt;
use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

use crate::message_attributes::validate_message_attributes;
use crate::{MessageAttributes, message_attributes_md5, message_attributes_size};
use crate::{QueueSecurity, RequestIdentity, SecurityUpdate};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};

#[path = "security_sqlite.rs"]
mod security_sqlite;

#[cfg(test)]
#[path = "security_tests.rs"]
mod security_tests;

#[path = "fifo.rs"]
mod fifo;
pub use fifo::{DeduplicationScope, FifoThroughputLimit, ReceiveOptions};
use fifo::{replay_attempt, save_attempt, validate_fifo};

#[path = "management.rs"]
mod management;
pub use management::{QueueMetrics, QueuePage, QueueTags};

#[cfg(test)]
#[path = "management_tests.rs"]
mod management_tests;

#[cfg(test)]
#[path = "dlq_tests.rs"]
mod dlq_tests;

#[cfg(test)]
#[path = "delivery_tests.rs"]
mod delivery_tests;

#[cfg(test)]
#[path = "batch_tests.rs"]
mod batch_tests;

#[cfg(test)]
#[path = "polling_tests.rs"]
mod polling_tests;

#[cfg(test)]
#[path = "attribute_tests.rs"]
mod attribute_tests;

#[cfg(test)]
#[path = "fifo_tests.rs"]
mod fifo_tests;

#[cfg(test)]
#[path = "storage_tests.rs"]
mod storage_tests;

pub const MAX_MESSAGE_BYTES: usize = 1_048_576;
pub const DEFAULT_MAX_IN_FLIGHT: usize = 120_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueType {
    Standard,
    Fifo,
}

impl QueueType {
    fn as_db_value(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Fifo => "fifo",
        }
    }
    fn from_db_value(value: &str) -> Result<Self, LqsError> {
        match value {
            "standard" => Ok(Self::Standard),
            "fifo" => Ok(Self::Fifo),
            other => Err(LqsError::Database(format!(
                "unknown queue type in database: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueOptions {
    pub security: QueueSecurity,
    pub deduplication_scope: DeduplicationScope,
    pub fifo_throughput_limit: FifoThroughputLimit,
    pub visibility_timeout_ms: u64,
    pub content_based_deduplication: bool,
    pub deduplication_window_ms: u64,
    pub redrive_policy: Option<RedrivePolicy>,
    pub delay_ms: u64,
    pub message_retention_ms: u64,
    pub maximum_message_size: usize,
    pub receive_wait_time_ms: u64,
    /// Local quota, configurable downward for deterministic tests.
    pub max_in_flight: usize,
}

/// Partial update: None preserves the current setting.
#[derive(Debug, Clone, Default)]
pub struct QueueUpdate {
    pub security: SecurityUpdate,
    pub visibility_timeout_ms: Option<u64>,
    pub content_based_deduplication: Option<bool>,
    pub deduplication_scope: Option<DeduplicationScope>,
    pub fifo_throughput_limit: Option<FifoThroughputLimit>,
    pub delay_ms: Option<u64>,
    pub message_retention_ms: Option<u64>,
    pub maximum_message_size: Option<usize>,
    pub receive_wait_time_ms: Option<u64>,
    pub max_in_flight: Option<usize>,
    /// None preserves the policy; Some(None) removes it.
    pub redrive_policy: Option<Option<RedrivePolicy>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedrivePolicy {
    pub dead_letter_queue: String,
    pub max_receive_count: u32,
}

impl Default for QueueOptions {
    fn default() -> Self {
        Self {
            security: QueueSecurity::default(),
            deduplication_scope: DeduplicationScope::Queue,
            fifo_throughput_limit: FifoThroughputLimit::PerQueue,
            visibility_timeout_ms: 30_000,
            content_based_deduplication: false,
            deduplication_window_ms: 300_000,
            redrive_policy: None,
            delay_ms: 0,
            message_retention_ms: 345_600_000,
            maximum_message_size: MAX_MESSAGE_BYTES,
            receive_wait_time_ms: 0,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendRequest {
    pub body: String,
    pub message_group_id: Option<String>,
    pub deduplication_id: Option<String>,
    /// Standard-only override; Some(0) disables the queue's default delay.
    pub delay_ms: Option<u64>,
    pub message_attributes: MessageAttributes,
}

impl SendRequest {
    pub fn standard(body: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            message_group_id: None,
            deduplication_id: None,
            delay_ms: None,
            message_attributes: MessageAttributes::new(),
        }
    }
    pub fn fifo(body: impl Into<String>, group_id: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            message_group_id: Some(group_id.into()),
            deduplication_id: None,
            delay_ms: None,
            message_attributes: MessageAttributes::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendResult {
    pub message_id: String,
    pub deduplicated: bool,
    pub md5_of_message_attributes: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReceivedMessage {
    pub message_id: String,
    pub receipt_handle: String,
    pub body: String,
    pub message_group_id: Option<String>,
    pub receive_count: u32,
    pub message_attributes: MessageAttributes,
    pub system_attributes: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LqsError {
    AccessDenied,
    InvalidSecuritySettings(String),
    PurgeQueueInProgress,
    QueueDeletedRecently,
    InvalidQueueManagement(String),
    QueueAlreadyExists(String),
    QueueNotFound(String),
    InvalidFifoName(String),
    InvalidStandardName(String),
    FifoRequiresGroupId,
    DeduplicationIdRequired,
    EmptyGroupId,
    InvalidReceiptHandle(String),
    InvalidVisibilityTimeout,
    InvalidRedrivePolicy(String),
    InvalidDeliveryOptions(String),
    InvalidMessageSize { size: usize, maximum: usize },
    InvalidBatch(&'static str),
    InvalidMessageIdentifier(&'static str),
    InvalidReceiveOptions,
    OverLimit,
    MessageNotInflight,
    InvalidMessageAttributes(String),
    Database(String),
}

impl fmt::Display for LqsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AccessDenied => write!(f, "access denied"),
            Self::InvalidSecuritySettings(message) => {
                write!(f, "invalid security settings: {message}")
            }
            Self::PurgeQueueInProgress => {
                write!(f, "PurgeQueue was called within the last 60 seconds")
            }
            Self::QueueDeletedRecently => {
                write!(f, "wait 60 seconds before recreating a deleted queue")
            }
            Self::InvalidQueueManagement(message) => {
                write!(f, "invalid queue management request: {message}")
            }
            Self::QueueAlreadyExists(name) => write!(f, "queue already exists: {name}"),
            Self::QueueNotFound(name) => write!(f, "queue not found: {name}"),
            Self::InvalidFifoName(name) => write!(f, "FIFO queue name must end with .fifo: {name}"),
            Self::InvalidStandardName(name) => {
                write!(f, "Standard queue name must not end with .fifo: {name}")
            }
            Self::FifoRequiresGroupId => write!(f, "FIFO messages require message_group_id"),
            Self::DeduplicationIdRequired => write!(
                f,
                "FIFO messages require deduplication_id unless content-based deduplication is enabled"
            ),
            Self::EmptyGroupId => write!(f, "message_group_id may not be empty"),
            Self::InvalidReceiptHandle(handle) => write!(f, "invalid receipt handle: {handle}"),
            Self::InvalidVisibilityTimeout => {
                write!(f, "visibility timeout must be 0-43200000ms")
            }
            Self::Database(message) => write!(f, "database error: {message}"),
            Self::InvalidReceiveOptions => write!(
                f,
                "receive count must be 1-10, wait time 0-20000ms, and in-flight quota 1-120000"
            ),
            Self::OverLimit => write!(f, "in-flight message limit reached"),
            Self::MessageNotInflight => write!(f, "message is no longer in flight"),
            Self::InvalidMessageAttributes(reason) => {
                write!(f, "invalid message attributes: {reason}")
            }
            Self::InvalidBatch(code) => write!(f, "invalid batch request: {code}"),
            Self::InvalidMessageIdentifier(name) => write!(
                f,
                "{name} must be 1-128 ASCII letters, digits or punctuation characters"
            ),
            Self::InvalidRedrivePolicy(message) => write!(f, "invalid redrive policy: {message}"),
            Self::InvalidDeliveryOptions(message) => {
                write!(f, "invalid delivery options: {message}")
            }
            Self::InvalidMessageSize { size, maximum } => write!(
                f,
                "message including attributes is {size} bytes; body must be nonempty and total must not exceed {maximum} bytes"
            ),
        }
    }
}
impl std::error::Error for LqsError {}
impl From<rusqlite::Error> for LqsError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error.to_string())
    }
}

/// SQLite-backed LQS repository. A connection owns one database session.
pub struct Lqs {
    connection: Connection,
    storage_encryption: StorageEncryption,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageEncryption {
    None,
    SqlCipher,
}

impl StorageEncryption {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::SqlCipher => "sqlcipher",
        }
    }
}

fn ensure_private_database_file(path: &Path) -> Result<(), LqsError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    match options.open(path) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(path)
                .map_err(|error| LqsError::Database(error.to_string()))?;
            if !metadata.is_file() {
                return Err(LqsError::Database(
                    "encrypted database path must be a regular file".into(),
                ));
            }
            #[cfg(unix)]
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(LqsError::Database(
                    "encrypted database file must have owner-only permissions".into(),
                ));
            }
            Ok(())
        }
        Err(error) => Err(LqsError::Database(error.to_string())),
    }
}

pub fn read_database_key_file(path: impl AsRef<Path>) -> Result<Vec<u8>, LqsError> {
    let path = path.as_ref();
    let metadata = fs::metadata(path).map_err(|error| LqsError::Database(error.to_string()))?;
    if !metadata.is_file() {
        return Err(LqsError::Database(
            "SQLCipher key path must be a regular file".into(),
        ));
    }
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(LqsError::Database(
            "SQLCipher key file must have owner-only permissions".into(),
        ));
    }
    let key = fs::read(path).map_err(|error| LqsError::Database(error.to_string()))?;
    if !(32..=128).contains(&key.len()) {
        return Err(LqsError::Database(
            "SQLCipher key must contain 32 to 128 bytes".into(),
        ));
    }
    Ok(key)
}

fn open_keyed_connection(path: &Path, key: &[u8]) -> Result<Connection, LqsError> {
    if !(32..=128).contains(&key.len()) {
        return Err(LqsError::Database(
            "SQLCipher key must contain 32 to 128 bytes".into(),
        ));
    }
    ensure_private_database_file(path)?;
    let connection = Connection::open(path)?;
    let version: String = connection
        .query_row("PRAGMA cipher_version", [], |row| row.get(0))
        .map_err(|_| LqsError::Database("SQLCipher support is unavailable".into()))?;
    if version.is_empty() {
        return Err(LqsError::Database(
            "SQLCipher support is unavailable".into(),
        ));
    }
    // SAFETY: the connection handle remains valid and SQLCipher copies the key during this call.
    let result = unsafe {
        rusqlite::ffi::sqlite3_key(
            connection.handle(),
            key.as_ptr().cast(),
            i32::try_from(key.len()).expect("validated key length"),
        )
    };
    if result != rusqlite::ffi::SQLITE_OK {
        return Err(LqsError::Database(
            "failed to configure SQLCipher key".into(),
        ));
    }
    connection
        .query_row("SELECT count(*) FROM sqlite_master", [], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(|_| LqsError::Database("failed to unlock encrypted database".into()))?;
    connection.execute_batch("PRAGMA temp_store=MEMORY;")?;
    Ok(connection)
}

/// Copies a stopped plaintext database to a new encrypted file. The source is retained.
/// Keep the source and any backups protected until they can be securely retired.
pub fn migrate_plaintext_database(
    source: impl AsRef<Path>,
    target: impl AsRef<Path>,
    key: &[u8],
) -> Result<(), LqsError> {
    let source = source.as_ref();
    let target = target.as_ref();
    if fs::symlink_metadata(target).is_ok() {
        return Err(LqsError::Database(
            "encrypted migration target must not exist".into(),
        ));
    }
    let source_path = source
        .to_str()
        .ok_or_else(|| LqsError::Database("database path must be UTF-8".into()))?;
    let source_connection = Connection::open(source)?;
    source_connection.query_row("SELECT count(*) FROM sqlite_master", [], |row| {
        row.get::<_, i64>(0)
    })?;
    source_connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    drop(source_connection);
    let connection = open_keyed_connection(target, key)?;
    connection.execute("ATTACH DATABASE ?1 AS plaintext KEY ''", [source_path])?;
    connection.query_row("SELECT sqlcipher_export('main', 'plaintext')", [], |row| {
        row.get::<_, Option<String>>(0)
    })?;
    connection.execute_batch("DETACH DATABASE plaintext;")?;
    drop(connection);
    let _ = Lqs::open_encrypted(target, key)?;
    Ok(())
}

impl Lqs {
    /// Opens (and migrates) a persistent SQLite database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LqsError> {
        Self::from_connection(Connection::open(path)?)
    }

    /// Opens a SQLCipher database. The key is a raw secret supplied by the caller.
    /// Existing plaintext databases are not converted automatically.
    pub fn open_encrypted(path: impl AsRef<Path>, key: &[u8]) -> Result<Self, LqsError> {
        let connection = open_keyed_connection(path.as_ref(), key)?;
        Self::from_connection_with_encryption(connection, StorageEncryption::SqlCipher)
    }

    pub fn storage_encryption(&self) -> StorageEncryption {
        self.storage_encryption
    }

    /// Creates an isolated SQLite in-memory database, primarily useful for tests.
    pub fn in_memory() -> Result<Self, LqsError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    /// Creates an isolated in-memory SQLite database.
    pub fn new() -> Self {
        Self::in_memory().expect("opening an in-memory SQLite database must succeed")
    }

    fn from_connection(connection: Connection) -> Result<Self, LqsError> {
        Self::from_connection_with_encryption(connection, StorageEncryption::None)
    }

    fn from_connection_with_encryption(
        mut connection: Connection,
        storage_encryption: StorageEncryption,
    ) -> Result<Self, LqsError> {
        create_base_schema(&connection)?;
        migrate_existing_schema(&mut connection)?;
        Ok(Self {
            connection,
            storage_encryption,
        })
    }

    pub fn create_queue(
        &mut self,
        name: impl Into<String>,
        queue_type: QueueType,
        options: QueueOptions,
    ) -> Result<(), LqsError> {
        self.create_queue_with_tags_at(
            name,
            queue_type,
            options,
            QueueTags::new(),
            management::now_ms(),
        )
    }

    pub fn create_queue_with_tags_at(
        &mut self,
        name: impl Into<String>,
        queue_type: QueueType,
        options: QueueOptions,
        tags: QueueTags,
        now_ms: u64,
    ) -> Result<(), LqsError> {
        let name = name.into();
        validate_queue_creation(&name, queue_type, &options, &tags)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let deleted: Option<i64> = transaction
            .query_row(
                "SELECT deleted_at_ms FROM deleted_queues WHERE name = ?1",
                [&name],
                |row| row.get(0),
            )
            .optional()?;
        if deleted.is_some_and(|deleted| ms(now_ms) < deleted.saturating_add(60_000)) {
            return Err(LqsError::QueueDeletedRecently);
        }
        transaction.execute("DELETE FROM deleted_queues WHERE name = ?1", [&name])?;
        insert_queue(&transaction, &name, queue_type, &options, &tags, now_ms)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn send(
        &mut self,
        queue_name: &str,
        request: SendRequest,
        now_ms: u64,
    ) -> Result<SendResult, LqsError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let config = read_queue_config(&transaction, queue_name)?;
        let prepared = prepare_send(request, &config)?;
        expire_messages(&transaction, queue_name, ms(now_ms))?;
        if let Some(message_id) =
            find_duplicate(&transaction, queue_name, &config, &prepared, now_ms)?
        {
            transaction.commit()?;
            return Ok(SendResult {
                message_id,
                deduplicated: true,
                md5_of_message_attributes: prepared.attribute_md5,
            });
        }
        let message_id = insert_message(&transaction, queue_name, &prepared, now_ms)?;
        transaction.commit()?;
        Ok(SendResult {
            message_id,
            deduplicated: false,
            md5_of_message_attributes: prepared.attribute_md5,
        })
    }

    pub fn receive(
        &mut self,
        queue_name: &str,
        max_messages: usize,
        now_ms: u64,
    ) -> Result<Vec<ReceivedMessage>, LqsError> {
        self.receive_with_options(queue_name, max_messages, ReceiveOptions::default(), now_ms)
    }

    pub fn receive_with_options(
        &mut self,
        queue_name: &str,
        max_messages: usize,
        options: ReceiveOptions,
        now_ms: u64,
    ) -> Result<Vec<ReceivedMessage>, LqsError> {
        self.receive_page(queue_name, max_messages, &options, now_ms, true)
            .map(|(messages, _)| messages)
    }

    pub(crate) fn receive_page(
        &mut self,
        queue_name: &str,
        max_messages: usize,
        options: &ReceiveOptions,
        now_ms: u64,
        cache_empty: bool,
    ) -> Result<(Vec<ReceivedMessage>, bool), LqsError> {
        if !(1..=10).contains(&max_messages) {
            return Err(LqsError::InvalidReceiveOptions);
        }
        let now = ms(now_ms);
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let config = read_queue_config(&transaction, queue_name)?;
        options.validate(config.queue_type)?;
        let visibility = options
            .visibility_timeout_ms
            .unwrap_or(config.visibility_timeout_ms);
        expire_messages(&transaction, queue_name, now)?;
        if let Some(messages) =
            replay_attempt(&transaction, queue_name, options, max_messages, now)?
        {
            transaction.commit()?;
            return Ok((messages, true));
        }
        let in_flight = count_in_flight(&transaction, queue_name, now)?;
        if in_flight >= config.max_in_flight && config.queue_type == QueueType::Standard {
            transaction.commit()?;
            return Err(LqsError::OverLimit);
        }
        let limit = max_messages.min(config.max_in_flight.saturating_sub(in_flight));
        let received = collect_received(&transaction, queue_name, &config, limit, visibility, now)?;
        let complete = !received.is_empty() || cache_empty;
        if complete {
            save_attempt(
                &transaction,
                queue_name,
                options,
                max_messages,
                visibility,
                now,
                &received,
            )?;
        }
        transaction.commit()?;
        Ok((received, complete))
    }

    pub fn delete(&mut self, queue_name: &str, receipt_handle: &str) -> Result<(), LqsError> {
        self.queue_config(queue_name)?;
        let count = self.connection.execute(
            "DELETE FROM messages WHERE queue_name = ?1 AND receipt_handle = ?2",
            params![queue_name, receipt_handle],
        )?;
        if count == 1 {
            Ok(())
        } else {
            Err(LqsError::InvalidReceiptHandle(receipt_handle.to_owned()))
        }
    }

    pub fn change_visibility(
        &mut self,
        queue_name: &str,
        receipt_handle: &str,
        timeout_ms: u64,
        now_ms: u64,
    ) -> Result<(), LqsError> {
        if timeout_ms > 43_200_000 {
            return Err(LqsError::InvalidDeliveryOptions(
                "visibility timeout must be 0-43200000ms".into(),
            ));
        }
        self.queue_config(queue_name)?;
        let count = self.connection.execute(
            "UPDATE messages SET invisible_until_ms = ?1, visibility_revision = visibility_revision + 1 WHERE queue_name = ?2 AND receipt_handle = ?3
             AND invisible_until_ms > ?4 AND created_at_ms > ?4 - (SELECT message_retention_ms FROM queues WHERE name = ?2)",
            params![ms(now_ms).saturating_add(ms(timeout_ms)), queue_name, receipt_handle, ms(now_ms)],
        )?;
        if count == 1 {
            Ok(())
        } else {
            let exists: bool = self.connection.query_row("SELECT EXISTS(SELECT 1 FROM messages WHERE queue_name = ?1 AND receipt_handle = ?2)", params![queue_name, receipt_handle], |row| row.get(0))?;
            if exists {
                Err(LqsError::MessageNotInflight)
            } else {
                Err(LqsError::InvalidReceiptHandle(receipt_handle.to_owned()))
            }
        }
    }

    /// Current nonexpired messages with a visibility deadline strictly after now.
    pub fn in_flight_count(&self, queue_name: &str, now_ms: u64) -> Result<usize, LqsError> {
        self.queue_config(queue_name)?;
        count_in_flight(&self.connection, queue_name, ms(now_ms))
    }

    pub fn queue_depth(&self, queue_name: &str) -> Result<usize, LqsError> {
        self.queue_config(queue_name)?;
        Ok(self.connection.query_row(
            "SELECT COUNT(*) FROM messages WHERE queue_name = ?1",
            params![queue_name],
            |row| row.get(0),
        )?)
    }

    /// Atomically replaces or removes a queue's redrive policy.
    pub fn set_redrive_policy(
        &mut self,
        queue_name: &str,
        policy: Option<RedrivePolicy>,
    ) -> Result<(), LqsError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        write_redrive_policy(&transaction, queue_name, policy.as_ref())?;
        transaction.execute(
            "UPDATE queues SET modified_at_ms = ?2 WHERE name = ?1",
            params![queue_name, ms(management::now_ms())],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn redrive_policy(&self, queue_name: &str) -> Result<Option<RedrivePolicy>, LqsError> {
        queue_type(&self.connection, queue_name)?;
        read_redrive_policy(&self.connection, queue_name)
    }

    pub fn list_dead_letter_source_queues(
        &self,
        dead_letter_queue: &str,
    ) -> Result<Vec<String>, LqsError> {
        queue_type(&self.connection, dead_letter_queue)?;
        let mut statement = self.connection.prepare(
            "SELECT source_queue FROM redrive_policies WHERE dead_letter_queue = ?1 ORDER BY source_queue",
        )?;
        Ok(statement
            .query_map([dead_letter_queue], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?)
    }

    /// Re-enqueues available dead letters originally from `source_queue` as new messages.
    /// In-flight messages and blocked FIFO group members are left untouched.
    /// This synchronous primitive is the foundation for a future HTTP move-task API.
    pub fn redrive_dead_letters(
        &mut self,
        dead_letter_queue: &str,
        source_queue: &str,
        max_messages: usize,
        now_ms: u64,
    ) -> Result<usize, LqsError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let target_type = queue_type(&transaction, source_queue)?;
        if queue_type(&transaction, dead_letter_queue)? != target_type
            || dead_letter_queue == source_queue
        {
            return Err(LqsError::InvalidRedrivePolicy(
                "source and DLQ must be distinct queues of the same type".into(),
            ));
        }
        let mut moved = 0;
        expire_messages(&transaction, dead_letter_queue, ms(now_ms))?;
        expire_messages(&transaction, source_queue, ms(now_ms))?;
        while moved < max_messages {
            let candidate: Option<i64> = transaction.query_row(
                "SELECT m.sequence FROM messages m JOIN dead_letter_origins o ON o.sequence = m.sequence
                 WHERE m.queue_name = ?1 AND o.source_queue = ?2
                 AND (m.invisible_until_ms IS NULL OR m.invisible_until_ms <= ?3)
                 AND m.available_at_ms <= ?3
                 AND (?4 = 'standard' OR NOT EXISTS (
                     SELECT 1 FROM messages earlier WHERE earlier.queue_name = m.queue_name
                     AND earlier.group_id = m.group_id AND earlier.sequence < m.sequence))
                 ORDER BY m.sequence LIMIT 1",
                params![dead_letter_queue, source_queue, ms(now_ms), target_type.as_db_value()],
                |row| row.get(0),
            ).optional()?;
            let Some(sequence) = candidate else { break };
            move_message(&transaction, sequence, source_queue, ms(now_ms), true, true)?;
            moved += 1;
        }
        transaction.commit()?;
        Ok(moved)
    }

    pub(crate) fn queue_config(&self, queue_name: &str) -> Result<QueueConfig, LqsError> {
        read_queue_config(&self.connection, queue_name)
    }

    /// Updates delivery settings and optional redrive policy in one transaction.
    pub fn set_queue_attributes(
        &mut self,
        name: &str,
        update: QueueUpdate,
        now_ms: u64,
    ) -> Result<(), LqsError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = read_queue_config(&transaction, name)?;
        let resolved = resolve_queue_update(&current, update)?;
        security_sqlite::write(&transaction, name, &resolved.security)?;
        transaction.execute(
            "UPDATE queues SET visibility_timeout_ms = ?2, modified_at_ms = ?3 WHERE name = ?1",
            params![name, ms(resolved.visibility), ms(now_ms)],
        )?;
        transaction.execute("UPDATE queues SET content_based_deduplication = ?2, deduplication_scope = ?3, fifo_throughput_limit = ?4 WHERE name = ?1", params![name, resolved.content_based, resolved.scope.as_str(), resolved.throughput.as_str()])?;
        if let Some(policy) = resolved.redrive_policy {
            write_redrive_policy(&transaction, name, policy.as_ref())?;
        }
        transaction.execute("UPDATE queues SET delay_ms = ?1, message_retention_ms = ?2, maximum_message_size = ?3, receive_wait_time_ms = ?5, max_in_flight = ?6 WHERE name = ?4", params![ms(resolved.delay), ms(resolved.retention), resolved.maximum, name, ms(resolved.wait), resolved.max_in_flight])?;
        if current.queue_type == QueueType::Fifo && resolved.delay != current.delay_ms {
            transaction.execute("UPDATE messages SET available_at_ms = MIN(created_at_ms, ?1) + ?2 WHERE queue_name = ?3 AND receive_count = 0", params![i64::MAX - ms(resolved.delay), ms(resolved.delay), name])?;
        }
        expire_messages(&transaction, name, ms(now_ms))?;
        transaction.commit()?;
        Ok(())
    }
}

fn create_base_schema(connection: &Connection) -> Result<(), LqsError> {
    connection.execute_batch(
        "
            PRAGMA foreign_keys = ON;
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS queues (
                name TEXT PRIMARY KEY NOT NULL,
                queue_type TEXT NOT NULL CHECK(queue_type IN ('standard', 'fifo')),
                visibility_timeout_ms INTEGER NOT NULL CHECK(visibility_timeout_ms BETWEEN 0 AND 43200000),
                content_based_deduplication INTEGER NOT NULL CHECK(content_based_deduplication IN (0, 1)),
                deduplication_window_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS messages (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT UNIQUE,
                queue_name TEXT NOT NULL REFERENCES queues(name) ON DELETE CASCADE,
                body TEXT NOT NULL,
                group_id TEXT,
                receipt_handle TEXT UNIQUE,
                invisible_until_ms INTEGER,
                receive_count INTEGER NOT NULL DEFAULT 0,
                created_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS messages_by_queue_sequence ON messages(queue_name, sequence);
            CREATE INDEX IF NOT EXISTS messages_by_fifo_group ON messages(queue_name, group_id, sequence);
            CREATE TABLE IF NOT EXISTS deduplication_keys (
                queue_name TEXT NOT NULL REFERENCES queues(name) ON DELETE CASCADE,
                deduplication_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                seen_at_ms INTEGER NOT NULL,
                PRIMARY KEY(queue_name, deduplication_id)
            );
            CREATE TABLE IF NOT EXISTS redrive_policies (
                source_queue TEXT PRIMARY KEY NOT NULL REFERENCES queues(name) ON DELETE CASCADE,
                dead_letter_queue TEXT NOT NULL REFERENCES queues(name),
                max_receive_count INTEGER NOT NULL CHECK(max_receive_count BETWEEN 1 AND 1000),
                CHECK(source_queue != dead_letter_queue)
            );
            CREATE INDEX IF NOT EXISTS redrive_by_target ON redrive_policies(dead_letter_queue, source_queue);
            CREATE TABLE IF NOT EXISTS dead_letter_origins (
                sequence INTEGER PRIMARY KEY REFERENCES messages(sequence) ON DELETE CASCADE,
                source_queue TEXT NOT NULL REFERENCES queues(name)
            );
        ",
    )?;
    Ok(())
}

fn migrate_existing_schema(connection: &mut Connection) -> Result<(), LqsError> {
    // Additive, transactional migration also upgrades databases from before #4.
    // The v9 queue CHECK migration rebuilds the parent table without cascading
    // deletes. Foreign keys are validated before commit and re-enabled below.
    connection.pragma_update(None, "foreign_keys", "OFF")?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    add_delivery_columns(&transaction)?;
    transaction.execute_batch("CREATE INDEX IF NOT EXISTS messages_by_queue_created ON messages(queue_name, created_at_ms)")?;
    transaction.execute_batch("CREATE INDEX IF NOT EXISTS messages_by_queue_inflight ON messages(queue_name, invisible_until_ms)")?;
    fifo::migrate(&transaction)?;
    management::migrate(&transaction)?;
    security_sqlite::migrate(&transaction)?;
    if transaction
        .prepare("PRAGMA foreign_key_check")?
        .exists([])?
    {
        return Err(LqsError::Database(
            "foreign key check failed during migration".into(),
        ));
    }
    transaction.commit()?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    Ok(())
}

fn add_delivery_columns(transaction: &rusqlite::Transaction<'_>) -> Result<(), LqsError> {
    for (table, column, definition) in [
        (
            "queues",
            "delay_ms",
            "INTEGER NOT NULL DEFAULT 0 CHECK(delay_ms BETWEEN 0 AND 900000)",
        ),
        (
            "queues",
            "message_retention_ms",
            "INTEGER NOT NULL DEFAULT 345600000 CHECK(message_retention_ms BETWEEN 60000 AND 1209600000)",
        ),
        (
            "queues",
            "maximum_message_size",
            "INTEGER NOT NULL DEFAULT 1048576 CHECK(maximum_message_size BETWEEN 1024 AND 1048576)",
        ),
        ("messages", "available_at_ms", "INTEGER NOT NULL DEFAULT 0"),
        (
            "messages",
            "message_attributes",
            "TEXT NOT NULL DEFAULT '{}'",
        ),
        ("messages", "first_received_at_ms", "INTEGER"),
        ("messages", "message_deduplication_id", "TEXT"),
        (
            "messages",
            "total_receive_count",
            "INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "queues",
            "receive_wait_time_ms",
            "INTEGER NOT NULL DEFAULT 0 CHECK(receive_wait_time_ms BETWEEN 0 AND 20000)",
        ),
        (
            "queues",
            "max_in_flight",
            "INTEGER NOT NULL DEFAULT 120000 CHECK(max_in_flight BETWEEN 1 AND 120000)",
        ),
    ] {
        let mut statement = transaction.prepare(&format!("PRAGMA table_info({table})"))?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        if !columns.iter().any(|name| name == column) {
            transaction.execute_batch(&format!(
                "ALTER TABLE {table} ADD COLUMN {column} {definition}"
            ))?;
            if column == "message_deduplication_id" {
                transaction.execute("UPDATE messages SET message_deduplication_id = (SELECT deduplication_id FROM deduplication_keys WHERE deduplication_keys.queue_name = messages.queue_name AND deduplication_keys.message_id = messages.message_id LIMIT 1)", [])?;
            }
            if column == "total_receive_count" {
                transaction.execute(
                    "UPDATE messages SET total_receive_count = receive_count",
                    [],
                )?;
            }
        }
    }
    Ok(())
}

fn validate_queue_creation(
    name: &str,
    queue_type: QueueType,
    options: &QueueOptions,
    tags: &QueueTags,
) -> Result<(), LqsError> {
    options.security.validate()?;
    management::validate_tags(tags)?;
    if queue_type == QueueType::Fifo && !name.ends_with(".fifo") {
        return Err(LqsError::InvalidFifoName(name.to_owned()));
    }
    if queue_type == QueueType::Standard && name.ends_with(".fifo") {
        return Err(LqsError::InvalidStandardName(name.to_owned()));
    }
    if options.visibility_timeout_ms > 43_200_000 {
        return Err(LqsError::InvalidVisibilityTimeout);
    }
    validate_delivery_options(
        options.delay_ms,
        options.message_retention_ms,
        options.maximum_message_size,
    )?;
    validate_receive_options(options.receive_wait_time_ms, options.max_in_flight)?;
    validate_fifo(
        queue_type,
        options.content_based_deduplication,
        options.deduplication_scope,
        options.fifo_throughput_limit,
    )?;
    if options.deduplication_window_ms != 300_000 {
        return Err(LqsError::InvalidDeliveryOptions(
            "deduplication window is fixed at 300000ms".into(),
        ));
    }
    Ok(())
}

fn insert_queue(
    transaction: &rusqlite::Transaction<'_>,
    name: &str,
    queue_type: QueueType,
    options: &QueueOptions,
    tags: &QueueTags,
    now_ms: u64,
) -> Result<(), LqsError> {
    let result = transaction.execute(
        "INSERT INTO queues(name, queue_type, visibility_timeout_ms, content_based_deduplication, deduplication_window_ms, delay_ms, message_retention_ms, maximum_message_size, receive_wait_time_ms, max_in_flight)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![name, queue_type.as_db_value(), ms(options.visibility_timeout_ms), i64::from(options.content_based_deduplication), ms(options.deduplication_window_ms), ms(options.delay_ms), ms(options.message_retention_ms), options.maximum_message_size, ms(options.receive_wait_time_ms), options.max_in_flight],
    );
    match result {
        Ok(_) => {}
        Err(rusqlite::Error::SqliteFailure(error, _)) if error.extended_code == 1555 => {
            return Err(LqsError::QueueAlreadyExists(name.to_owned()));
        }
        Err(error) => return Err(error.into()),
    }
    security_sqlite::write(transaction, name, &options.security)?;
    transaction.execute(
        "UPDATE queues SET created_at_ms = ?2, modified_at_ms = ?2 WHERE name = ?1",
        params![name, ms(now_ms)],
    )?;
    management::write_tags(transaction, name, tags)?;
    transaction.execute(
        "UPDATE queues SET deduplication_scope = ?2, fifo_throughput_limit = ?3 WHERE name = ?1",
        params![
            name,
            options.deduplication_scope.as_str(),
            options.fifo_throughput_limit.as_str()
        ],
    )?;
    write_redrive_policy(transaction, name, options.redrive_policy.as_ref())?;
    Ok(())
}

struct ResolvedQueueUpdate {
    security: QueueSecurity,
    visibility: u64,
    delay: u64,
    retention: u64,
    maximum: usize,
    wait: u64,
    max_in_flight: usize,
    content_based: bool,
    scope: DeduplicationScope,
    throughput: FifoThroughputLimit,
    redrive_policy: Option<Option<RedrivePolicy>>,
}

fn resolve_queue_update(
    current: &QueueConfig,
    update: QueueUpdate,
) -> Result<ResolvedQueueUpdate, LqsError> {
    let security = current.security.updated(&update.security)?;
    let visibility = update
        .visibility_timeout_ms
        .unwrap_or(current.visibility_timeout_ms);
    if visibility > 43_200_000 {
        return Err(LqsError::InvalidVisibilityTimeout);
    }
    let delay = update.delay_ms.unwrap_or(current.delay_ms);
    let retention = update
        .message_retention_ms
        .unwrap_or(current.message_retention_ms);
    let maximum = update
        .maximum_message_size
        .unwrap_or(current.maximum_message_size);
    let wait = update
        .receive_wait_time_ms
        .unwrap_or(current.receive_wait_time_ms);
    let max_in_flight = update.max_in_flight.unwrap_or(current.max_in_flight);
    validate_delivery_options(delay, retention, maximum)?;
    validate_receive_options(wait, max_in_flight)?;
    let content_based = update
        .content_based_deduplication
        .unwrap_or(current.content_based_deduplication);
    let scope = update
        .deduplication_scope
        .unwrap_or(current.deduplication_scope);
    let throughput = update
        .fifo_throughput_limit
        .unwrap_or(current.fifo_throughput_limit);
    validate_fifo(current.queue_type, content_based, scope, throughput)?;
    if current.queue_type == QueueType::Standard
        && (update.content_based_deduplication.is_some()
            || update.deduplication_scope.is_some()
            || update.fifo_throughput_limit.is_some())
    {
        return Err(LqsError::InvalidDeliveryOptions(
            "FIFO attributes require a FIFO queue".into(),
        ));
    }
    Ok(ResolvedQueueUpdate {
        security,
        visibility,
        delay,
        retention,
        maximum,
        wait,
        max_in_flight,
        content_based,
        scope,
        throughput,
        redrive_policy: update.redrive_policy,
    })
}

#[derive(Debug, PartialEq, Eq)]
struct PreparedSend {
    body: String,
    group_id: Option<String>,
    deduplication_id: Option<String>,
    delay_ms: u64,
    attributes_json: String,
    attribute_md5: Option<String>,
}

// All validation and normalization depend only on the request and queue settings.
fn prepare_send(request: SendRequest, config: &QueueConfig) -> Result<PreparedSend, LqsError> {
    let SendRequest {
        body,
        message_group_id,
        deduplication_id,
        delay_ms,
        mut message_attributes,
    } = request;
    let size = body
        .len()
        .saturating_add(message_attributes_size(&message_attributes));
    if body.is_empty() || size > config.maximum_message_size {
        return Err(LqsError::InvalidMessageSize {
            size,
            maximum: config.maximum_message_size,
        });
    }
    validate_message_attributes(&mut message_attributes)?;
    // Normalizing a Number can expand exponent notation; validate the stored size too.
    let normalized_size = body
        .len()
        .saturating_add(message_attributes_size(&message_attributes));
    if normalized_size > config.maximum_message_size {
        return Err(LqsError::InvalidMessageSize {
            size: normalized_size,
            maximum: config.maximum_message_size,
        });
    }
    let attribute_md5 = message_attributes_md5(&message_attributes);
    let attributes_json = serde_json::to_string(&message_attributes)
        .map_err(|error| LqsError::Database(error.to_string()))?;
    if let Some(delay) = delay_ms
        && (config.queue_type == QueueType::Fifo || delay > 900_000)
    {
        return Err(LqsError::InvalidDeliveryOptions(
            "message delay must be 0-900000ms and is only allowed for Standard queues".into(),
        ));
    }
    let group_id = match config.queue_type {
        QueueType::Standard => message_group_id,
        QueueType::Fifo => {
            let group = message_group_id.ok_or(LqsError::FifoRequiresGroupId)?;
            if group.is_empty() {
                return Err(LqsError::EmptyGroupId);
            }
            Some(group)
        }
    };
    for (name, value) in [
        ("MessageGroupId", group_id.as_deref()),
        ("MessageDeduplicationId", deduplication_id.as_deref()),
    ] {
        if let Some(value) = value
            && (!(1..=128).contains(&value.len()) || !value.bytes().all(|b| b.is_ascii_graphic()))
        {
            return Err(LqsError::InvalidMessageIdentifier(name));
        }
    }
    if config.queue_type == QueueType::Standard && deduplication_id.is_some() {
        return Err(LqsError::InvalidDeliveryOptions(
            "MessageDeduplicationId is only allowed for FIFO queues".into(),
        ));
    }
    let deduplication_id = if config.queue_type == QueueType::Fifo {
        Some(match deduplication_id {
            Some(value) => value,
            None if config.content_based_deduplication => stable_content_id(&body),
            None => return Err(LqsError::DeduplicationIdRequired),
        })
    } else {
        None
    };
    Ok(PreparedSend {
        body,
        group_id,
        deduplication_id,
        delay_ms: delay_ms.unwrap_or(config.delay_ms),
        attributes_json,
        attribute_md5,
    })
}

fn find_duplicate(
    transaction: &rusqlite::Transaction<'_>,
    queue_name: &str,
    config: &QueueConfig,
    prepared: &PreparedSend,
    now_ms: u64,
) -> Result<Option<String>, LqsError> {
    let Some(key) = &prepared.deduplication_id else {
        return Ok(None);
    };
    if now_ms >= config.deduplication_window_ms {
        transaction.execute(
            "DELETE FROM deduplication_keys WHERE queue_name = ?1 AND seen_at_ms <= ?2",
            params![queue_name, ms(now_ms - config.deduplication_window_ms)],
        )?;
    }
    Ok(transaction.query_row(
        "SELECT message_id FROM deduplication_keys WHERE queue_name = ?1 AND deduplication_id = ?2 AND (?3 = 'queue' OR group_id = ?4 OR group_id = '') ORDER BY seen_at_ms, message_id LIMIT 1",
        params![queue_name, key, config.deduplication_scope.as_str(), prepared.group_id],
        |row| row.get(0),
    ).optional()?)
}

fn insert_message(
    transaction: &rusqlite::Transaction<'_>,
    queue_name: &str,
    prepared: &PreparedSend,
    now_ms: u64,
) -> Result<String, LqsError> {
    transaction.execute(
        "INSERT INTO messages(message_id, queue_name, body, group_id, created_at_ms, available_at_ms, message_attributes, message_deduplication_id) VALUES (NULL, ?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![queue_name, prepared.body, prepared.group_id, ms(now_ms), ms(now_ms).saturating_add(ms(prepared.delay_ms)), prepared.attributes_json, prepared.deduplication_id],
    )?;
    let sequence = transaction.last_insert_rowid();
    let message_id = format!("msg-{sequence:016x}");
    transaction.execute(
        "UPDATE messages SET message_id = ?1 WHERE sequence = ?2",
        params![message_id, sequence],
    )?;
    if let Some(key) = &prepared.deduplication_id {
        transaction.execute(
            "INSERT INTO deduplication_keys(queue_name, deduplication_id, message_id, seen_at_ms, group_id) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![queue_name, key, message_id, ms(now_ms), prepared.group_id],
        )?;
    }
    Ok(message_id)
}

fn read_queue_config(connection: &Connection, queue_name: &str) -> Result<QueueConfig, LqsError> {
    let row = connection.query_row(
            "SELECT queue_type, visibility_timeout_ms, content_based_deduplication, deduplication_window_ms, delay_ms, message_retention_ms, maximum_message_size, receive_wait_time_ms, max_in_flight FROM queues WHERE name = ?1",
            params![queue_name],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?, row.get::<_, i64>(3)?, row.get::<_, u64>(4)?, row.get::<_, u64>(5)?, row.get::<_, usize>(6)?, row.get::<_, u64>(7)?, row.get::<_, usize>(8)?)),
        ).optional()?.ok_or_else(|| LqsError::QueueNotFound(queue_name.to_owned()))?;
    Ok(QueueConfig {
        security: security_sqlite::read(connection, queue_name)?,
        deduplication_scope: connection
            .query_row(
                "SELECT deduplication_scope FROM queues WHERE name = ?1",
                [queue_name],
                |row| row.get::<_, String>(0),
            )?
            .parse()?,
        fifo_throughput_limit: connection
            .query_row(
                "SELECT fifo_throughput_limit FROM queues WHERE name = ?1",
                [queue_name],
                |row| row.get::<_, String>(0),
            )?
            .parse()?,
        queue_type: QueueType::from_db_value(&row.0)?,
        visibility_timeout_ms: row.1 as u64,
        content_based_deduplication: row.2 != 0,
        deduplication_window_ms: row.3 as u64,
        redrive_policy: read_redrive_policy(connection, queue_name)?,
        delay_ms: row.4,
        message_retention_ms: row.5,
        maximum_message_size: row.6,
        receive_wait_time_ms: row.7,
        max_in_flight: row.8,
    })
}
impl Default for Lqs {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QueueConfig {
    pub(crate) security: QueueSecurity,
    pub(crate) deduplication_scope: DeduplicationScope,
    pub(crate) fifo_throughput_limit: FifoThroughputLimit,
    pub(crate) queue_type: QueueType,
    pub(crate) visibility_timeout_ms: u64,
    pub(crate) content_based_deduplication: bool,
    pub(crate) deduplication_window_ms: u64,
    pub(crate) redrive_policy: Option<RedrivePolicy>,
    pub(crate) delay_ms: u64,
    pub(crate) message_retention_ms: u64,
    pub(crate) maximum_message_size: usize,
    pub(crate) receive_wait_time_ms: u64,
    pub(crate) max_in_flight: usize,
}

fn validate_receive_options(wait_ms: u64, max_in_flight: usize) -> Result<(), LqsError> {
    if wait_ms > 20_000 || !(1..=DEFAULT_MAX_IN_FLIGHT).contains(&max_in_flight) {
        return Err(LqsError::InvalidReceiveOptions);
    }
    Ok(())
}

fn count_in_flight(connection: &Connection, queue: &str, now: i64) -> Result<usize, LqsError> {
    Ok(connection.query_row("SELECT COUNT(*) FROM messages WHERE queue_name = ?1 AND invisible_until_ms > ?2 AND created_at_ms > ?2 - (SELECT message_retention_ms FROM queues WHERE name = ?1)", params![queue, now], |row| row.get(0))?)
}

fn validate_delivery_options(delay: u64, retention: u64, maximum: usize) -> Result<(), LqsError> {
    if delay > 900_000
        || !(60_000..=1_209_600_000).contains(&retention)
        || !(1024..=MAX_MESSAGE_BYTES).contains(&maximum)
    {
        return Err(LqsError::InvalidDeliveryOptions("delay must be 0-900000ms, retention 60000-1209600000ms, maximum message size 1024-1048576 bytes".into()));
    }
    Ok(())
}

fn expire_messages(connection: &Connection, queue: &str, now: i64) -> Result<(), LqsError> {
    connection.execute("DELETE FROM messages WHERE queue_name = ?1 AND created_at_ms <= ?2 - (SELECT message_retention_ms FROM queues WHERE name = ?1)", params![queue, now])?;
    Ok(())
}

fn queue_type(connection: &Connection, name: &str) -> Result<QueueType, LqsError> {
    let value: String = connection
        .query_row(
            "SELECT queue_type FROM queues WHERE name = ?1",
            [name],
            |row| row.get(0),
        )
        .optional()?
        .ok_or_else(|| LqsError::QueueNotFound(name.to_owned()))?;
    QueueType::from_db_value(&value)
}

fn read_redrive_policy(
    connection: &Connection,
    name: &str,
) -> Result<Option<RedrivePolicy>, LqsError> {
    Ok(connection.query_row(
        "SELECT dead_letter_queue, max_receive_count FROM redrive_policies WHERE source_queue = ?1", [name],
        |row| Ok(RedrivePolicy { dead_letter_queue: row.get(0)?, max_receive_count: row.get(1)? }),
    ).optional()?)
}

fn write_redrive_policy(
    transaction: &rusqlite::Transaction<'_>,
    source: &str,
    policy: Option<&RedrivePolicy>,
) -> Result<(), LqsError> {
    let source_type = queue_type(transaction, source)?;
    if let Some(policy) = policy {
        if !(1..=1000).contains(&policy.max_receive_count) || policy.dead_letter_queue == source {
            return Err(LqsError::InvalidRedrivePolicy(
                "maxReceiveCount must be 1-1000 and the DLQ must differ from the source".into(),
            ));
        }
        let target_type =
            queue_type(transaction, &policy.dead_letter_queue).map_err(|error| match error {
                LqsError::QueueNotFound(name) => {
                    LqsError::InvalidRedrivePolicy(format!("DLQ does not exist: {name}"))
                }
                other => other,
            })?;
        if target_type != source_type {
            return Err(LqsError::InvalidRedrivePolicy(
                "source and DLQ queue types must match".into(),
            ));
        }
        transaction.execute(
            "INSERT INTO redrive_policies(source_queue, dead_letter_queue, max_receive_count) VALUES (?1, ?2, ?3)
             ON CONFLICT(source_queue) DO UPDATE SET dead_letter_queue = excluded.dead_letter_queue, max_receive_count = excluded.max_receive_count",
            params![source, policy.dead_letter_queue, policy.max_receive_count],
        )?;
    } else {
        transaction.execute(
            "DELETE FROM redrive_policies WHERE source_queue = ?1",
            [source],
        )?;
    }
    Ok(())
}

/// Delete and reinsert in one transaction so destination ordering uses enqueue order,
/// not the source's old sequence. Any failure rolls the entire receive/redrive back.
fn move_message(
    transaction: &rusqlite::Transaction<'_>,
    sequence: i64,
    destination: &str,
    now: i64,
    reset_timestamp: bool,
    new_identity: bool,
) -> Result<i64, LqsError> {
    let StoredMessage { message_id, body, group_id, created_at, attributes, first_received, dedup, total_count } =
        transaction.query_row(
            "SELECT message_id, body, group_id, created_at_ms, message_attributes, first_received_at_ms, message_deduplication_id, total_receive_count FROM messages WHERE sequence = ?1",
            [sequence],
            |row| Ok(StoredMessage { message_id: row.get(0)?, body: row.get(1)?, group_id: row.get(2)?, created_at: row.get(3)?, attributes: row.get(4)?, first_received: row.get(5)?, dedup: row.get(6)?, total_count: row.get(7)? }),
        )?;
    transaction.execute("DELETE FROM messages WHERE sequence = ?1", [sequence])?;
    let delay = if new_identity {
        read_queue_config(transaction, destination)?.delay_ms
    } else {
        0
    };
    transaction.execute(
        "INSERT INTO messages(message_id, queue_name, body, group_id, created_at_ms, available_at_ms, message_attributes, first_received_at_ms, message_deduplication_id, total_receive_count) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![if new_identity { None } else { Some(message_id) }, destination, body, group_id, if reset_timestamp { now } else { created_at }, now.saturating_add(ms(delay)), attributes, if new_identity { None } else { first_received }, dedup, if new_identity { 0 } else { total_count }],
    )?;
    let moved = transaction.last_insert_rowid();
    if new_identity {
        transaction.execute(
            "UPDATE messages SET message_id = ?1 WHERE sequence = ?2",
            params![format!("msg-{moved:016x}"), moved],
        )?;
    }
    Ok(moved)
}
struct StoredMessage {
    message_id: String,
    body: String,
    group_id: Option<String>,
    created_at: i64,
    attributes: String,
    first_received: Option<i64>,
    dedup: Option<String>,
    total_count: i64,
}

struct Candidate {
    sequence: i64,
    message_id: String,
    body: String,
    group_id: Option<String>,
    receive_count: i64,
}

#[derive(Debug, PartialEq, Eq)]
enum CandidateAction<'a> {
    Stop,
    Redrive(&'a RedrivePolicy),
    Deliver,
}

fn candidate_action<'a>(
    candidate: &Candidate,
    received: &[ReceivedMessage],
    policy: Option<&'a RedrivePolicy>,
) -> CandidateAction<'a> {
    if received
        .iter()
        .any(|message| message.message_id == candidate.message_id)
    {
        CandidateAction::Stop
    } else if let Some(policy) = policy
        && candidate.receive_count >= i64::from(policy.max_receive_count)
    {
        CandidateAction::Redrive(policy)
    } else {
        CandidateAction::Deliver
    }
}

fn collect_received(
    transaction: &rusqlite::Transaction<'_>,
    queue_name: &str,
    config: &QueueConfig,
    limit: usize,
    visibility_ms: u64,
    now: i64,
) -> Result<Vec<ReceivedMessage>, LqsError> {
    let mut received = Vec::with_capacity(limit);
    let mut selected_sequences = Vec::with_capacity(limit);
    let mut preferred_group: Option<String> = None;
    let policy = read_redrive_policy(transaction, queue_name)?;
    while received.len() < limit {
        let Some(candidate) = next_receivable(
            transaction,
            queue_name,
            config.queue_type,
            now,
            &selected_sequences,
            preferred_group.as_deref(),
        )?
        else {
            break;
        };
        match candidate_action(&candidate, &received, policy.as_ref()) {
            CandidateAction::Stop => break,
            CandidateAction::Redrive(policy) => {
                redrive_candidate(
                    transaction,
                    queue_name,
                    config.queue_type,
                    &candidate,
                    policy,
                    now,
                )?;
            }
            CandidateAction::Deliver => {
                let sequence = candidate.sequence;
                let group = candidate.group_id.clone();
                let message = deliver_candidate(
                    transaction,
                    config.queue_type,
                    candidate,
                    visibility_ms,
                    now,
                )?;
                if config.queue_type == QueueType::Fifo {
                    selected_sequences.push(sequence);
                    preferred_group = group;
                }
                received.push(message);
            }
        }
    }
    Ok(received)
}

fn redrive_candidate(
    transaction: &rusqlite::Transaction<'_>,
    queue_name: &str,
    queue_type: QueueType,
    candidate: &Candidate,
    policy: &RedrivePolicy,
    now: i64,
) -> Result<(), LqsError> {
    let moved = move_message(
        transaction,
        candidate.sequence,
        &policy.dead_letter_queue,
        now,
        queue_type == QueueType::Fifo,
        false,
    )?;
    transaction.execute(
        "INSERT INTO dead_letter_origins(sequence, source_queue) VALUES (?1, ?2)",
        params![moved, queue_name],
    )?;
    Ok(())
}

fn deliver_candidate(
    transaction: &rusqlite::Transaction<'_>,
    queue_type: QueueType,
    candidate: Candidate,
    visibility_ms: u64,
    now: i64,
) -> Result<ReceivedMessage, LqsError> {
    let receive_count = candidate.receive_count + 1;
    let receipt_handle = format!(
        "receipt-{:016x}-{receive_count:08x}-{now:016x}",
        candidate.sequence
    );
    transaction.execute(
        "UPDATE messages SET receipt_handle = ?1, invisible_until_ms = ?2, receive_count = ?3, total_receive_count = total_receive_count + 1, first_received_at_ms = COALESCE(first_received_at_ms, ?5) WHERE sequence = ?4",
        params![receipt_handle, now.saturating_add(ms(visibility_ms)), receive_count, candidate.sequence, now],
    )?;
    let (message_attributes, system_attributes) =
        read_message_metadata(transaction, candidate.sequence, queue_type)?;
    Ok(ReceivedMessage {
        message_id: candidate.message_id,
        receipt_handle,
        body: candidate.body,
        message_group_id: candidate.group_id,
        receive_count: receive_count as u32,
        message_attributes,
        system_attributes,
    })
}

fn next_receivable(
    transaction: &rusqlite::Transaction<'_>,
    queue_name: &str,
    queue_type: QueueType,
    now: i64,
    selected_sequences: &[i64],
    preferred_group: Option<&str>,
) -> Result<Option<Candidate>, LqsError> {
    let (sql, parameters) = match queue_type {
        QueueType::Standard => (
            "SELECT sequence, message_id, body, group_id, receive_count FROM messages WHERE queue_name = ?1 AND available_at_ms <= ?2 AND (invisible_until_ms IS NULL OR invisible_until_ms <= ?2) ORDER BY sequence LIMIT 1".to_owned(),
            vec![
                rusqlite::types::Value::Text(queue_name.to_owned()),
                rusqlite::types::Value::Integer(now),
            ],
        ),
        QueueType::Fifo => {
            let mut parameters = vec![
                rusqlite::types::Value::Text(queue_name.to_owned()),
                rusqlite::types::Value::Integer(now),
                preferred_group
                    .map(|group| rusqlite::types::Value::Text(group.to_owned()))
                    .unwrap_or(rusqlite::types::Value::Null),
            ];
            parameters.extend(
                selected_sequences
                    .iter()
                    .copied()
                    .map(rusqlite::types::Value::Integer),
            );
            (fifo_candidate_sql(selected_sequences.len()), parameters)
        }
    };
    transaction
        .query_row(&sql, rusqlite::params_from_iter(parameters.iter()), |row| {
            Ok(Candidate {
                sequence: row.get(0)?,
                message_id: row.get(1)?,
                body: row.get(2)?,
                group_id: row.get(3)?,
                receive_count: row.get(4)?,
            })
        })
        .optional()
        .map_err(Into::into)
}

fn fifo_candidate_sql(selected_count: usize) -> String {
    // Only messages claimed by this receive call may be ignored as earlier group members.
    let placeholders = (4..4 + selected_count)
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let exclude_selected = if placeholders.is_empty() {
        String::new()
    } else {
        format!(" AND message.sequence NOT IN ({placeholders})")
    };
    let exclude_earlier_selected = if placeholders.is_empty() {
        String::new()
    } else {
        format!(" AND earlier.sequence NOT IN ({placeholders})")
    };
    format!(
        "SELECT message.sequence, message.message_id, message.body, message.group_id, message.receive_count
         FROM messages AS message
         WHERE message.queue_name = ?1 AND message.available_at_ms <= ?2
         AND (message.invisible_until_ms IS NULL OR message.invisible_until_ms <= ?2)
         {exclude_selected}
         AND NOT EXISTS (SELECT 1 FROM messages AS earlier
             WHERE earlier.queue_name = message.queue_name
             AND earlier.group_id = message.group_id
             AND earlier.sequence < message.sequence
             {exclude_earlier_selected})
         ORDER BY CASE WHEN message.group_id = ?3 THEN 0 ELSE 1 END, message.sequence
         LIMIT 1"
    )
}

fn ms(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn read_message_metadata(
    connection: &Connection,
    sequence: i64,
    kind: QueueType,
) -> Result<
    (
        MessageAttributes,
        std::collections::BTreeMap<String, String>,
    ),
    LqsError,
> {
    let (encoded, sent, first, count, group, dedup, id): (String, i64, i64, i64, Option<String>, Option<String>, String) = connection.query_row(
        "SELECT message_attributes, created_at_ms, first_received_at_ms, total_receive_count, group_id, message_deduplication_id, message_id FROM messages WHERE sequence = ?1",
        [sequence], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?)))?;
    let attributes =
        serde_json::from_str(&encoded).map_err(|error| LqsError::Database(error.to_string()))?;
    let mut system = std::collections::BTreeMap::from([
        ("SentTimestamp".into(), sent.to_string()),
        ("ApproximateFirstReceiveTimestamp".into(), first.to_string()),
        ("ApproximateReceiveCount".into(), count.to_string()),
        ("SenderId".into(), "000000000000".into()),
        ("SqsManagedSseEnabled".into(), "false".into()),
    ]);
    if let Some(group) = group {
        system.insert("MessageGroupId".into(), group);
    }
    if kind == QueueType::Fifo {
        let number = id
            .strip_prefix("msg-")
            .and_then(|value| u64::from_str_radix(value, 16).ok())
            .unwrap_or(sequence as u64);
        system.insert("SequenceNumber".into(), number.to_string());
        if let Some(dedup) = dedup {
            system.insert("MessageDeduplicationId".into(), dedup);
        }
    }
    let source: Option<String> = connection
        .query_row(
            "SELECT source_queue FROM dead_letter_origins WHERE sequence = ?1",
            [sequence],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(source) = source {
        system.insert(
            "DeadLetterQueueSourceArn".into(),
            format!("arn:aws:sqs:us-east-1:000000000000:{source}"),
        );
    }
    Ok((attributes, system))
}
fn stable_content_id(body: &str) -> String {
    format!("{:x}", Sha256::digest(body.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn config(kind: QueueType) -> QueueConfig {
        let defaults = QueueOptions::default();
        QueueConfig {
            security: defaults.security,
            deduplication_scope: defaults.deduplication_scope,
            fifo_throughput_limit: defaults.fifo_throughput_limit,
            queue_type: kind,
            visibility_timeout_ms: defaults.visibility_timeout_ms,
            content_based_deduplication: kind == QueueType::Fifo,
            deduplication_window_ms: defaults.deduplication_window_ms,
            redrive_policy: defaults.redrive_policy,
            delay_ms: defaults.delay_ms,
            message_retention_ms: defaults.message_retention_ms,
            maximum_message_size: defaults.maximum_message_size,
            receive_wait_time_ms: defaults.receive_wait_time_ms,
            max_in_flight: defaults.max_in_flight,
        }
    }

    #[test]
    fn send_preparation_is_deterministic_without_a_database() {
        let settings = config(QueueType::Fifo);
        let request = SendRequest::fifo("payload", "group");
        let first = prepare_send(request.clone(), &settings).unwrap();
        assert_eq!(first, prepare_send(request, &settings).unwrap());
        assert_eq!(first.deduplication_id, Some(stable_content_id("payload")));
        assert_eq!(first.group_id.as_deref(), Some("group"));
        assert_eq!(
            prepare_send(SendRequest::fifo("payload", ""), &settings),
            Err(LqsError::EmptyGroupId)
        );
    }

    #[test]
    fn queue_update_resolution_validates_before_storage() {
        let current = config(QueueType::Standard);
        let resolved = resolve_queue_update(
            &current,
            QueueUpdate {
                delay_ms: Some(900_000),
                ..QueueUpdate::default()
            },
        )
        .unwrap();
        assert_eq!(resolved.delay, 900_000);
        assert_eq!(resolved.visibility, current.visibility_timeout_ms);
        assert!(matches!(
            resolve_queue_update(
                &current,
                QueueUpdate {
                    content_based_deduplication: Some(true),
                    ..QueueUpdate::default()
                }
            ),
            Err(LqsError::InvalidDeliveryOptions(_))
        ));
    }

    #[test]
    fn candidate_decision_keeps_duplicate_and_redrive_boundaries_distinct() {
        let candidate = Candidate {
            sequence: 1,
            message_id: "m1".into(),
            body: "payload".into(),
            group_id: Some("group".into()),
            receive_count: 2,
        };
        let policy = RedrivePolicy {
            dead_letter_queue: "dlq.fifo".into(),
            max_receive_count: 2,
        };
        assert_eq!(
            candidate_action(&candidate, &[], Some(&policy)),
            CandidateAction::Redrive(&policy)
        );
        let already_received = ReceivedMessage {
            message_id: "m1".into(),
            receipt_handle: "receipt".into(),
            body: "payload".into(),
            message_group_id: Some("group".into()),
            receive_count: 3,
            message_attributes: MessageAttributes::new(),
            system_attributes: Default::default(),
        };
        assert_eq!(
            candidate_action(&candidate, &[already_received], Some(&policy)),
            CandidateAction::Stop
        );
        assert_eq!(
            candidate_action(&candidate, &[], None),
            CandidateAction::Deliver
        );
    }

    #[test]
    fn candidate_query_can_be_verified_independently_of_batch_delivery() {
        let mut lqs = fifo();
        lqs.send("orders.fifo", SendRequest::fifo("a-1", "a"), 0)
            .unwrap();
        lqs.send("orders.fifo", SendRequest::fifo("a-2", "a"), 1)
            .unwrap();
        lqs.send("orders.fifo", SendRequest::fifo("b-1", "b"), 2)
            .unwrap();
        let first = lqs.receive("orders.fifo", 1, 3).unwrap();
        assert_eq!(first[0].body, "a-1");

        let transaction = lqs.connection.transaction().unwrap();
        let candidate = next_receivable(&transaction, "orders.fifo", QueueType::Fifo, 3, &[], None)
            .unwrap()
            .unwrap();
        assert_eq!(candidate.body, "b-1");
    }

    fn fifo() -> Lqs {
        let mut lqs = Lqs::new();
        lqs.create_queue(
            "orders.fifo",
            QueueType::Fifo,
            QueueOptions {
                content_based_deduplication: true,
                ..QueueOptions::default()
            },
        )
        .unwrap();
        lqs
    }

    #[test]
    fn queue_names_must_match_the_queue_type() {
        let mut lqs = Lqs::new();

        assert_eq!(
            lqs.create_queue("events", QueueType::Standard, QueueOptions::default()),
            Ok(())
        );
        assert_eq!(
            lqs.create_queue("orders.fifo", QueueType::Fifo, QueueOptions::default()),
            Ok(())
        );
        assert_eq!(
            lqs.create_queue("invalid-fifo", QueueType::Fifo, QueueOptions::default()),
            Err(LqsError::InvalidFifoName("invalid-fifo".to_owned()))
        );
        assert_eq!(
            lqs.create_queue(
                "invalid-standard.fifo",
                QueueType::Standard,
                QueueOptions::default()
            ),
            Err(LqsError::InvalidStandardName(
                "invalid-standard.fifo".to_owned()
            ))
        );
    }

    #[test]
    fn fifo_keeps_order_and_allows_other_groups_in_parallel() {
        let mut lqs = fifo();
        lqs.send("orders.fifo", SendRequest::fifo("a-1", "a"), 0)
            .unwrap();
        lqs.send("orders.fifo", SendRequest::fifo("a-2", "a"), 1)
            .unwrap();
        lqs.send("orders.fifo", SendRequest::fifo("b-1", "b"), 2)
            .unwrap();
        let first = lqs.receive("orders.fifo", 10, 3).unwrap();
        assert_eq!(
            first.iter().map(|m| m.body.as_str()).collect::<Vec<_>>(),
            ["a-1", "a-2", "b-1"]
        );
        assert!(lqs.receive("orders.fifo", 10, 4).unwrap().is_empty());
        lqs.delete("orders.fifo", &first[0].receipt_handle).unwrap();
        assert!(lqs.receive("orders.fifo", 10, 4).unwrap().is_empty());
    }

    #[test]
    fn fifo_deduplicates_within_the_window() {
        let mut lqs = fifo();
        let first = lqs
            .send("orders.fifo", SendRequest::fifo("same", "a"), 0)
            .unwrap();
        let duplicate = lqs
            .send("orders.fifo", SendRequest::fifo("same", "a"), 299_999)
            .unwrap();
        assert!(duplicate.deduplicated);
        assert_eq!(duplicate.message_id, first.message_id);
        assert_eq!(lqs.queue_depth("orders.fifo").unwrap(), 1);
        assert!(
            !lqs.send("orders.fifo", SendRequest::fifo("same", "a"), 300_000)
                .unwrap()
                .deduplicated
        );
    }

    #[test]
    fn visibility_timeout_retries_the_message() {
        let mut lqs = fifo();
        lqs.send("orders.fifo", SendRequest::fifo("a-1", "a"), 0)
            .unwrap();
        let first = lqs.receive("orders.fifo", 1, 10).unwrap();
        assert!(lqs.receive("orders.fifo", 1, 20).unwrap().is_empty());
        let retried = lqs.receive("orders.fifo", 1, 30_010).unwrap();
        assert_eq!(retried[0].message_id, first[0].message_id);
        assert_eq!(retried[0].receive_count, 2);
    }

    #[test]
    fn sqlite_file_persists_data() {
        let path =
            std::env::temp_dir().join(format!("lqs-{}-persistence.sqlite", std::process::id()));
        let _ = fs::remove_file(&path);
        {
            let mut lqs = Lqs::open(&path).unwrap();
            lqs.create_queue("events", QueueType::Standard, QueueOptions::default())
                .unwrap();
            lqs.send("events", SendRequest::standard("saved"), 0)
                .unwrap();
        }
        let mut reopened = Lqs::open(&path).unwrap();
        assert_eq!(reopened.queue_depth("events").unwrap(), 1);
        assert_eq!(reopened.receive("events", 1, 1).unwrap()[0].body, "saved");
        fs::remove_file(path).unwrap();
    }
}
