use std::fmt;
use std::path::Path;

use crate::message_attributes::validate_message_attributes;
use crate::{MessageAttributes, message_attributes_md5, message_attributes_size};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};

#[path = "fifo.rs"]
mod fifo;
pub use fifo::{DeduplicationScope, FifoThroughputLimit, ReceiveOptions};
use fifo::{replay_attempt, save_attempt, validate_fifo};

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
                write!(f, "visibility timeout must be greater than zero")
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
}

impl Lqs {
    /// Opens (and migrates) a persistent SQLite database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LqsError> {
        Self::from_connection(Connection::open(path)?)
    }

    /// Creates an isolated SQLite in-memory database, primarily useful for tests.
    pub fn in_memory() -> Result<Self, LqsError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    /// Creates an isolated in-memory SQLite database.
    pub fn new() -> Self {
        Self::in_memory().expect("opening an in-memory SQLite database must succeed")
    }

    fn from_connection(mut connection: Connection) -> Result<Self, LqsError> {
        connection.execute_batch(
            "
            PRAGMA foreign_keys = ON;
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS queues (
                name TEXT PRIMARY KEY NOT NULL,
                queue_type TEXT NOT NULL CHECK(queue_type IN ('standard', 'fifo')),
                visibility_timeout_ms INTEGER NOT NULL CHECK(visibility_timeout_ms > 0),
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
        // Additive, transactional migration also upgrades databases from before #4.
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
        transaction.execute_batch("CREATE INDEX IF NOT EXISTS messages_by_queue_created ON messages(queue_name, created_at_ms)")?;
        transaction.execute_batch("CREATE INDEX IF NOT EXISTS messages_by_queue_inflight ON messages(queue_name, invisible_until_ms)")?;
        fifo::migrate(&transaction)?;
        transaction.commit()?;
        Ok(Self { connection })
    }

    pub fn create_queue(
        &mut self,
        name: impl Into<String>,
        queue_type: QueueType,
        options: QueueOptions,
    ) -> Result<(), LqsError> {
        let name = name.into();
        if queue_type == QueueType::Fifo && !name.ends_with(".fifo") {
            return Err(LqsError::InvalidFifoName(name));
        }
        if queue_type == QueueType::Standard && name.ends_with(".fifo") {
            return Err(LqsError::InvalidStandardName(name));
        }
        if options.visibility_timeout_ms == 0 {
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
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = transaction.execute(
            "INSERT INTO queues(name, queue_type, visibility_timeout_ms, content_based_deduplication, deduplication_window_ms, delay_ms, message_retention_ms, maximum_message_size, receive_wait_time_ms, max_in_flight)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![name, queue_type.as_db_value(), ms(options.visibility_timeout_ms), i64::from(options.content_based_deduplication), ms(options.deduplication_window_ms), ms(options.delay_ms), ms(options.message_retention_ms), options.maximum_message_size, ms(options.receive_wait_time_ms), options.max_in_flight],
        );
        match result {
            Ok(_) => {
                transaction.execute("UPDATE queues SET deduplication_scope = ?2, fifo_throughput_limit = ?3 WHERE name = ?1", params![name, options.deduplication_scope.as_str(), options.fifo_throughput_limit.as_str()])?;
                write_redrive_policy(&transaction, &name, options.redrive_policy.as_ref())?;
                transaction.commit()?;
                Ok(())
            }
            Err(rusqlite::Error::SqliteFailure(error, _)) if error.extended_code == 1555 => {
                Err(LqsError::QueueAlreadyExists(name))
            }
            Err(error) => Err(error.into()),
        }
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
                && (!(1..=128).contains(&value.len())
                    || !value.bytes().all(|b| b.is_ascii_graphic()))
            {
                return Err(LqsError::InvalidMessageIdentifier(name));
            }
        }
        expire_messages(&transaction, queue_name, ms(now_ms))?;
        if config.queue_type == QueueType::Standard && deduplication_id.is_some() {
            return Err(LqsError::InvalidDeliveryOptions(
                "MessageDeduplicationId is only allowed for FIFO queues".into(),
            ));
        }
        let deduplication_id = if config.queue_type == QueueType::Fifo {
            let key = match deduplication_id {
                Some(value) => value,
                None if config.content_based_deduplication => stable_content_id(&body),
                None => return Err(LqsError::DeduplicationIdRequired),
            };
            if now_ms >= config.deduplication_window_ms {
                transaction.execute(
                    "DELETE FROM deduplication_keys WHERE queue_name = ?1 AND seen_at_ms <= ?2",
                    params![queue_name, ms(now_ms - config.deduplication_window_ms)],
                )?;
            }
            if let Some(message_id) = transaction.query_row(
                "SELECT message_id FROM deduplication_keys WHERE queue_name = ?1 AND deduplication_id = ?2 AND (?3 = 'queue' OR group_id = ?4 OR group_id = '') ORDER BY seen_at_ms, message_id LIMIT 1",
                params![queue_name, key, config.deduplication_scope.as_str(), group_id], |row| row.get(0),
            ).optional()? {
                transaction.commit()?;
                return Ok(SendResult { message_id, deduplicated: true, md5_of_message_attributes: attribute_md5 });
            }
            Some(key)
        } else {
            None
        };
        transaction.execute(
            "INSERT INTO messages(message_id, queue_name, body, group_id, created_at_ms, available_at_ms, message_attributes, message_deduplication_id) VALUES (NULL, ?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![queue_name, body, group_id, ms(now_ms), ms(now_ms).saturating_add(ms(delay_ms.unwrap_or(config.delay_ms))), attributes_json, deduplication_id],
        )?;
        let sequence = transaction.last_insert_rowid();
        let message_id = format!("msg-{sequence:016x}");
        transaction.execute(
            "UPDATE messages SET message_id = ?1 WHERE sequence = ?2",
            params![message_id, sequence],
        )?;
        if let Some(key) = deduplication_id {
            transaction.execute(
                "INSERT INTO deduplication_keys(queue_name, deduplication_id, message_id, seen_at_ms, group_id) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![queue_name, key, message_id, ms(now_ms), group_id],
            )?;
        }
        transaction.commit()?;
        Ok(SendResult {
            message_id,
            deduplicated: false,
            md5_of_message_attributes: attribute_md5,
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
        let mut received: Vec<ReceivedMessage> = Vec::with_capacity(limit);
        let policy = read_redrive_policy(&transaction, queue_name)?;
        while received.len() < limit {
            let Some(candidate) =
                next_receivable(&transaction, queue_name, config.queue_type, now)?
            else {
                break;
            };
            if received
                .iter()
                .any(|message| message.message_id == candidate.message_id)
            {
                break;
            }
            if let Some(policy) = &policy
                && candidate.receive_count >= i64::from(policy.max_receive_count)
            {
                let moved = move_message(
                    &transaction,
                    candidate.sequence,
                    &policy.dead_letter_queue,
                    now,
                    config.queue_type == QueueType::Fifo,
                    false,
                )?;
                transaction.execute(
                    "INSERT INTO dead_letter_origins(sequence, source_queue) VALUES (?1, ?2)",
                    params![moved, queue_name],
                )?;
                continue;
            }
            let receive_count = candidate.receive_count + 1;
            let receipt_handle = format!(
                "receipt-{:016x}-{receive_count:08x}-{now:016x}",
                candidate.sequence
            );
            transaction.execute(
                "UPDATE messages SET receipt_handle = ?1, invisible_until_ms = ?2, receive_count = ?3, total_receive_count = total_receive_count + 1, first_received_at_ms = COALESCE(first_received_at_ms, ?5) WHERE sequence = ?4",
                params![receipt_handle, now.saturating_add(ms(visibility)), receive_count, candidate.sequence, now],
            )?;
            let (message_attributes, system_attributes) =
                read_message_metadata(&transaction, candidate.sequence, config.queue_type)?;
            received.push(ReceivedMessage {
                message_id: candidate.message_id,
                receipt_handle,
                body: candidate.body,
                message_group_id: candidate.group_id,
                receive_count: receive_count as u32,
                message_attributes,
                system_attributes,
            });
        }
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
        transaction.execute("UPDATE queues SET content_based_deduplication = ?2, deduplication_scope = ?3, fifo_throughput_limit = ?4 WHERE name = ?1", params![name, content_based, scope.as_str(), throughput.as_str()])?;
        if let Some(policy) = update.redrive_policy {
            write_redrive_policy(&transaction, name, policy.as_ref())?;
        }
        transaction.execute("UPDATE queues SET delay_ms = ?1, message_retention_ms = ?2, maximum_message_size = ?3, receive_wait_time_ms = ?5, max_in_flight = ?6 WHERE name = ?4", params![ms(delay), ms(retention), maximum, name, ms(wait), max_in_flight])?;
        if current.queue_type == QueueType::Fifo && delay != current.delay_ms {
            transaction.execute("UPDATE messages SET available_at_ms = MIN(created_at_ms, ?1) + ?2 WHERE queue_name = ?3 AND receive_count = 0", params![i64::MAX - ms(delay), ms(delay), name])?;
        }
        expire_messages(&transaction, name, ms(now_ms))?;
        transaction.commit()?;
        Ok(())
    }
}

fn read_queue_config(connection: &Connection, queue_name: &str) -> Result<QueueConfig, LqsError> {
    let row = connection.query_row(
            "SELECT queue_type, visibility_timeout_ms, content_based_deduplication, deduplication_window_ms, delay_ms, message_retention_ms, maximum_message_size, receive_wait_time_ms, max_in_flight FROM queues WHERE name = ?1",
            params![queue_name],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?, row.get::<_, i64>(3)?, row.get::<_, u64>(4)?, row.get::<_, u64>(5)?, row.get::<_, usize>(6)?, row.get::<_, u64>(7)?, row.get::<_, usize>(8)?)),
        ).optional()?.ok_or_else(|| LqsError::QueueNotFound(queue_name.to_owned()))?;
    Ok(QueueConfig {
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

fn next_receivable(
    transaction: &rusqlite::Transaction<'_>,
    queue_name: &str,
    queue_type: QueueType,
    now: i64,
) -> Result<Option<Candidate>, LqsError> {
    let sql = match queue_type {
        QueueType::Standard => {
            "SELECT sequence, message_id, body, group_id, receive_count FROM messages WHERE queue_name = ?1 AND available_at_ms <= ?2 AND (invisible_until_ms IS NULL OR invisible_until_ms <= ?2) ORDER BY sequence LIMIT 1"
        }
        QueueType::Fifo => {
            "SELECT message.sequence, message.message_id, message.body, message.group_id, message.receive_count FROM messages AS message WHERE message.queue_name = ?1 AND message.available_at_ms <= ?2 AND (message.invisible_until_ms IS NULL OR message.invisible_until_ms <= ?2) AND NOT EXISTS (SELECT 1 FROM messages AS earlier WHERE earlier.queue_name = message.queue_name AND earlier.group_id = message.group_id AND earlier.sequence < message.sequence) ORDER BY message.sequence LIMIT 1"
        }
    };
    transaction
        .query_row(sql, params![queue_name, now], |row| {
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
            ["a-1", "b-1"]
        );
        lqs.delete("orders.fifo", &first[0].receipt_handle).unwrap();
        assert_eq!(lqs.receive("orders.fifo", 10, 4).unwrap()[0].body, "a-2");
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
