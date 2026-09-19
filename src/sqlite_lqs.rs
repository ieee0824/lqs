use std::fmt;
use std::path::Path;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

#[cfg(test)]
#[path = "dlq_tests.rs"]
mod dlq_tests;

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
    pub visibility_timeout_ms: u64,
    pub content_based_deduplication: bool,
    pub deduplication_window_ms: u64,
    pub redrive_policy: Option<RedrivePolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedrivePolicy {
    pub dead_letter_queue: String,
    pub max_receive_count: u32,
}

impl Default for QueueOptions {
    fn default() -> Self {
        Self {
            visibility_timeout_ms: 30_000,
            content_based_deduplication: false,
            deduplication_window_ms: 300_000,
            redrive_policy: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendRequest {
    pub body: String,
    pub message_group_id: Option<String>,
    pub deduplication_id: Option<String>,
}

impl SendRequest {
    pub fn standard(body: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            message_group_id: None,
            deduplication_id: None,
        }
    }
    pub fn fifo(body: impl Into<String>, group_id: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            message_group_id: Some(group_id.into()),
            deduplication_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendResult {
    pub message_id: String,
    pub deduplicated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedMessage {
    pub message_id: String,
    pub receipt_handle: String,
    pub body: String,
    pub message_group_id: Option<String>,
    pub receive_count: u32,
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
            Self::InvalidRedrivePolicy(message) => write!(f, "invalid redrive policy: {message}"),
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

    fn from_connection(connection: Connection) -> Result<Self, LqsError> {
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
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = transaction.execute(
            "INSERT INTO queues(name, queue_type, visibility_timeout_ms, content_based_deduplication, deduplication_window_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![name, queue_type.as_db_value(), ms(options.visibility_timeout_ms), i64::from(options.content_based_deduplication), ms(options.deduplication_window_ms)],
        );
        match result {
            Ok(_) => {
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
        let config = self.queue_config(queue_name)?;
        let SendRequest {
            body,
            message_group_id,
            deduplication_id,
        } = request;
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
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
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
                "SELECT message_id FROM deduplication_keys WHERE queue_name = ?1 AND deduplication_id = ?2",
                params![queue_name, key], |row| row.get(0),
            ).optional()? {
                transaction.commit()?;
                return Ok(SendResult { message_id, deduplicated: true });
            }
            Some(key)
        } else {
            None
        };
        transaction.execute(
            "INSERT INTO messages(message_id, queue_name, body, group_id, created_at_ms) VALUES (NULL, ?1, ?2, ?3, ?4)",
            params![queue_name, body, group_id, ms(now_ms)],
        )?;
        let sequence = transaction.last_insert_rowid();
        let message_id = format!("msg-{sequence:016x}");
        transaction.execute(
            "UPDATE messages SET message_id = ?1 WHERE sequence = ?2",
            params![message_id, sequence],
        )?;
        if let Some(key) = deduplication_id {
            transaction.execute(
                "INSERT INTO deduplication_keys(queue_name, deduplication_id, message_id, seen_at_ms) VALUES (?1, ?2, ?3, ?4)",
                params![queue_name, key, message_id, ms(now_ms)],
            )?;
        }
        transaction.commit()?;
        Ok(SendResult {
            message_id,
            deduplicated: false,
        })
    }

    pub fn receive(
        &mut self,
        queue_name: &str,
        max_messages: usize,
        now_ms: u64,
    ) -> Result<Vec<ReceivedMessage>, LqsError> {
        let config = self.queue_config(queue_name)?;
        let now = ms(now_ms);
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut received = Vec::with_capacity(max_messages);
        let policy = read_redrive_policy(&transaction, queue_name)?;
        while received.len() < max_messages {
            let Some(candidate) =
                next_receivable(&transaction, queue_name, config.queue_type, now)?
            else {
                break;
            };
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
                "UPDATE messages SET receipt_handle = ?1, invisible_until_ms = ?2, receive_count = ?3 WHERE sequence = ?4",
                params![receipt_handle, now.saturating_add(ms(config.visibility_timeout_ms)), receive_count, candidate.sequence],
            )?;
            received.push(ReceivedMessage {
                message_id: candidate.message_id,
                receipt_handle,
                body: candidate.body,
                message_group_id: candidate.group_id,
                receive_count: receive_count as u32,
            });
        }
        transaction.commit()?;
        Ok(received)
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
        self.queue_config(queue_name)?;
        let count = self.connection.execute(
            "UPDATE messages SET invisible_until_ms = ?1 WHERE queue_name = ?2 AND receipt_handle = ?3",
            params![ms(now_ms).saturating_add(ms(timeout_ms)), queue_name, receipt_handle],
        )?;
        if count == 1 {
            Ok(())
        } else {
            Err(LqsError::InvalidReceiptHandle(receipt_handle.to_owned()))
        }
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
        while moved < max_messages {
            let candidate: Option<i64> = transaction.query_row(
                "SELECT m.sequence FROM messages m JOIN dead_letter_origins o ON o.sequence = m.sequence
                 WHERE m.queue_name = ?1 AND o.source_queue = ?2
                 AND (m.invisible_until_ms IS NULL OR m.invisible_until_ms <= ?3)
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
        let row = self.connection.query_row(
            "SELECT queue_type, visibility_timeout_ms, content_based_deduplication, deduplication_window_ms FROM queues WHERE name = ?1",
            params![queue_name],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?, row.get::<_, i64>(3)?)),
        ).optional()?.ok_or_else(|| LqsError::QueueNotFound(queue_name.to_owned()))?;
        Ok(QueueConfig {
            queue_type: QueueType::from_db_value(&row.0)?,
            visibility_timeout_ms: row.1 as u64,
            content_based_deduplication: row.2 != 0,
            deduplication_window_ms: row.3 as u64,
            redrive_policy: read_redrive_policy(&self.connection, queue_name)?,
        })
    }
}
impl Default for Lqs {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QueueConfig {
    pub(crate) queue_type: QueueType,
    pub(crate) visibility_timeout_ms: u64,
    pub(crate) content_based_deduplication: bool,
    pub(crate) deduplication_window_ms: u64,
    pub(crate) redrive_policy: Option<RedrivePolicy>,
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
    let (message_id, body, group_id, created_at): (String, String, Option<String>, i64) =
        transaction.query_row(
            "SELECT message_id, body, group_id, created_at_ms FROM messages WHERE sequence = ?1",
            [sequence],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
    transaction.execute("DELETE FROM messages WHERE sequence = ?1", [sequence])?;
    transaction.execute(
        "INSERT INTO messages(message_id, queue_name, body, group_id, created_at_ms) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![if new_identity { None } else { Some(message_id) }, destination, body, group_id, if reset_timestamp { now } else { created_at }],
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
            "SELECT sequence, message_id, body, group_id, receive_count FROM messages WHERE queue_name = ?1 AND (invisible_until_ms IS NULL OR invisible_until_ms <= ?2) ORDER BY sequence LIMIT 1"
        }
        QueueType::Fifo => {
            "SELECT message.sequence, message.message_id, message.body, message.group_id, message.receive_count FROM messages AS message WHERE message.queue_name = ?1 AND (message.invisible_until_ms IS NULL OR message.invisible_until_ms <= ?2) AND NOT EXISTS (SELECT 1 FROM messages AS earlier WHERE earlier.queue_name = message.queue_name AND earlier.group_id = message.group_id AND earlier.sequence < message.sequence) ORDER BY message.sequence LIMIT 1"
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
fn stable_content_id(body: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in body.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("content-{hash:016x}")
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
