use super::*;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DeduplicationScope {
    #[default]
    Queue,
    MessageGroup,
}
impl DeduplicationScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queue => "queue",
            Self::MessageGroup => "messageGroup",
        }
    }
}
impl FromStr for DeduplicationScope {
    type Err = LqsError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queue" => Ok(Self::Queue),
            "messageGroup" => Ok(Self::MessageGroup),
            _ => Err(invalid("DeduplicationScope must be queue or messageGroup")),
        }
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FifoThroughputLimit {
    #[default]
    PerQueue,
    PerMessageGroupId,
}
impl FifoThroughputLimit {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PerQueue => "perQueue",
            Self::PerMessageGroupId => "perMessageGroupId",
        }
    }
}
impl FromStr for FifoThroughputLimit {
    type Err = LqsError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "perQueue" => Ok(Self::PerQueue),
            "perMessageGroupId" => Ok(Self::PerMessageGroupId),
            _ => Err(invalid(
                "FifoThroughputLimit must be perQueue or perMessageGroupId",
            )),
        }
    }
}

fn invalid(reason: &str) -> LqsError {
    LqsError::InvalidDeliveryOptions(reason.into())
}

pub(super) fn validate_fifo(
    kind: QueueType,
    content: bool,
    scope: DeduplicationScope,
    throughput: FifoThroughputLimit,
) -> Result<(), LqsError> {
    if kind == QueueType::Standard
        && (content
            || scope != DeduplicationScope::Queue
            || throughput != FifoThroughputLimit::PerQueue)
    {
        return Err(invalid("FIFO attributes require a FIFO queue"));
    }
    if throughput == FifoThroughputLimit::PerMessageGroupId
        && scope != DeduplicationScope::MessageGroup
    {
        return Err(invalid(
            "perMessageGroupId requires messageGroup deduplication scope",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub struct ReceiveOptions {
    pub receive_request_attempt_id: Option<String>,
    pub visibility_timeout_ms: Option<u64>,
}
impl ReceiveOptions {
    pub(super) fn validate(&self, kind: QueueType) -> Result<(), LqsError> {
        if self
            .visibility_timeout_ms
            .is_some_and(|value| value > 43_200_000)
        {
            return Err(invalid("visibility timeout must be 0-43200000ms"));
        }
        if let Some(id) = &self.receive_request_attempt_id {
            if kind != QueueType::Fifo {
                return Err(invalid("ReceiveRequestAttemptId requires a FIFO queue"));
            }
            if !(1..=128).contains(&id.len()) || !id.bytes().all(|b| b.is_ascii_graphic()) {
                return Err(LqsError::InvalidMessageIdentifier(
                    "ReceiveRequestAttemptId",
                ));
            }
        }
        Ok(())
    }
    fn fingerprint(&self, maximum: usize) -> String {
        format!("{maximum}:{:?}", self.visibility_timeout_ms)
    }
}

pub(super) fn migrate(tx: &rusqlite::Transaction<'_>) -> Result<(), LqsError> {
    for (table, column, definition) in [
        (
            "queues",
            "deduplication_scope",
            "TEXT NOT NULL DEFAULT 'queue' CHECK(deduplication_scope IN ('queue', 'messageGroup'))",
        ),
        (
            "queues",
            "fifo_throughput_limit",
            "TEXT NOT NULL DEFAULT 'perQueue' CHECK(fifo_throughput_limit IN ('perQueue', 'perMessageGroupId'))",
        ),
        (
            "messages",
            "visibility_revision",
            "INTEGER NOT NULL DEFAULT 0",
        ),
    ] {
        let columns = tx
            .prepare(&format!("PRAGMA table_info({table})"))?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        if !columns.iter().any(|name| name == column) {
            tx.execute_batch(&format!(
                "ALTER TABLE {table} ADD COLUMN {column} {definition}"
            ))?;
        }
    }
    let columns = tx
        .prepare("PRAGMA table_info(deduplication_keys)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if !columns.iter().any(|name| name == "group_id") {
        tx.execute_batch("ALTER TABLE deduplication_keys RENAME TO legacy_deduplication_keys;
            CREATE TABLE deduplication_keys (
                queue_name TEXT NOT NULL REFERENCES queues(name) ON DELETE CASCADE,
                deduplication_id TEXT NOT NULL, message_id TEXT NOT NULL, seen_at_ms INTEGER NOT NULL,
                group_id TEXT NOT NULL, PRIMARY KEY(queue_name, deduplication_id, group_id));
            INSERT INTO deduplication_keys SELECT queue_name, deduplication_id, message_id, seen_at_ms,
                COALESCE((SELECT group_id FROM messages WHERE messages.message_id = legacy_deduplication_keys.message_id), '') FROM legacy_deduplication_keys;
            DROP TABLE legacy_deduplication_keys;")?;
    }
    tx.execute_batch("UPDATE queues SET deduplication_window_ms = 300000 WHERE deduplication_window_ms != 300000;
        CREATE INDEX IF NOT EXISTS deduplication_expiry ON deduplication_keys(queue_name, seen_at_ms);
        CREATE TABLE IF NOT EXISTS receive_attempts (
            queue_name TEXT NOT NULL REFERENCES queues(name) ON DELETE CASCADE,
            attempt_id TEXT NOT NULL, created_at_ms INTEGER NOT NULL, fingerprint TEXT NOT NULL,
            visibility_ms INTEGER NOT NULL, response TEXT NOT NULL, valid INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY(queue_name, attempt_id));
        CREATE INDEX IF NOT EXISTS receive_attempt_expiry ON receive_attempts(queue_name, created_at_ms);
        CREATE TABLE IF NOT EXISTS receive_attempt_members (
            queue_name TEXT NOT NULL, attempt_id TEXT NOT NULL, receipt_handle TEXT NOT NULL,
            FOREIGN KEY(queue_name, attempt_id) REFERENCES receive_attempts(queue_name, attempt_id) ON DELETE CASCADE,
            PRIMARY KEY(queue_name, attempt_id, receipt_handle));
        CREATE INDEX IF NOT EXISTS receive_attempt_receipts ON receive_attempt_members(receipt_handle);
        CREATE TRIGGER IF NOT EXISTS invalidate_receive_delete AFTER DELETE ON messages BEGIN
            UPDATE receive_attempts SET valid = 0 WHERE (queue_name, attempt_id) IN
                (SELECT queue_name, attempt_id FROM receive_attempt_members WHERE receipt_handle = OLD.receipt_handle);
        END;
        CREATE TRIGGER IF NOT EXISTS invalidate_receive_update AFTER UPDATE OF receipt_handle, visibility_revision ON messages BEGIN
            UPDATE receive_attempts SET valid = 0 WHERE (queue_name, attempt_id) IN
                (SELECT queue_name, attempt_id FROM receive_attempt_members WHERE receipt_handle = OLD.receipt_handle);
        END;")?;
    Ok(())
}

pub(super) fn replay_attempt(
    tx: &rusqlite::Transaction<'_>,
    queue: &str,
    options: &ReceiveOptions,
    maximum: usize,
    now: i64,
) -> Result<Option<Vec<ReceivedMessage>>, LqsError> {
    tx.execute(
        "DELETE FROM receive_attempts WHERE queue_name = ?1 AND created_at_ms <= ?2 - 300000",
        params![queue, now],
    )?;
    let Some(id) = &options.receive_request_attempt_id else {
        return Ok(None);
    };
    let record = tx.query_row("SELECT fingerprint, visibility_ms, response, valid FROM receive_attempts WHERE queue_name = ?1 AND attempt_id = ?2", params![queue, id], |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?, row.get::<_, String>(2)?, row.get::<_, bool>(3)?))).optional()?;
    let Some((fingerprint, visibility, response, valid)) = record else {
        return Ok(None);
    };
    if !valid {
        return Err(invalid(
            "ReceiveRequestAttemptId is no longer replayable because a message was modified",
        ));
    }
    if fingerprint != options.fingerprint(maximum) {
        return Err(invalid(
            "ReceiveRequestAttemptId parameters differ from the original request",
        ));
    }
    let messages: Vec<ReceivedMessage> =
        serde_json::from_str(&response).map_err(|e| LqsError::Database(e.to_string()))?;
    // Replays do not increment receive counters or change receipt handles.
    let extra: usize = tx.query_row("SELECT COUNT(*) FROM messages WHERE queue_name = ?1 AND invisible_until_ms <= ?2 AND receipt_handle IN (SELECT receipt_handle FROM receive_attempt_members WHERE queue_name = ?1 AND attempt_id = ?3)", params![queue, now, id], |row| row.get(0))?;
    if visibility > 0
        && extra > 0
        && count_in_flight(tx, queue, now)?.saturating_add(extra)
            > read_queue_config(tx, queue)?.max_in_flight
    {
        return Err(LqsError::OverLimit);
    }
    for message in &messages {
        tx.execute("UPDATE messages SET invisible_until_ms = ?1 WHERE queue_name = ?2 AND receipt_handle = ?3", params![now.saturating_add(ms(visibility)), queue, message.receipt_handle])?;
    }
    Ok(Some(messages))
}

pub(super) fn save_attempt(
    tx: &rusqlite::Transaction<'_>,
    queue: &str,
    options: &ReceiveOptions,
    maximum: usize,
    visibility: u64,
    now: i64,
    messages: &[ReceivedMessage],
) -> Result<(), LqsError> {
    let Some(id) = &options.receive_request_attempt_id else {
        return Ok(());
    };
    let response =
        serde_json::to_string(messages).map_err(|e| LqsError::Database(e.to_string()))?;
    tx.execute("INSERT INTO receive_attempts(queue_name, attempt_id, created_at_ms, fingerprint, visibility_ms, response) VALUES (?1, ?2, ?3, ?4, ?5, ?6)", params![queue, id, now, options.fingerprint(maximum), ms(visibility), response])?;
    for message in messages {
        tx.execute("INSERT INTO receive_attempt_members(queue_name, attempt_id, receipt_handle) VALUES (?1, ?2, ?3)", params![queue, id, message.receipt_handle])?;
    }
    Ok(())
}
