use super::*;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use std::collections::BTreeMap;

pub type QueueTags = BTreeMap<String, String>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueuePage {
    pub queue_names: Vec<String>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueMetrics {
    pub visible: usize,
    pub not_visible: usize,
    pub delayed: usize,
    pub created_at_ms: u64,
    pub modified_at_ms: u64,
}

pub(super) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn invalid(message: &str) -> LqsError {
    LqsError::InvalidQueueManagement(message.into())
}

pub(super) fn migrate(tx: &rusqlite::Transaction<'_>) -> Result<(), LqsError> {
    let schema: String = tx.query_row(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'queues'",
        [],
        |row| row.get(0),
    )?;
    if schema.contains("CHECK(visibility_timeout_ms > 0)") {
        let auxiliaries: Vec<String> = tx.prepare("SELECT sql FROM sqlite_master WHERE tbl_name = 'queues' AND type IN ('index', 'trigger') AND sql IS NOT NULL")?.query_map([], |row| row.get(0))?.collect::<Result<_, _>>()?;
        let replacement = schema.replacen("queues", "queues_v9", 1).replace(
            "CHECK(visibility_timeout_ms > 0)",
            "CHECK(visibility_timeout_ms BETWEEN 0 AND 43200000)",
        );
        tx.execute_batch(&replacement)?;
        tx.execute_batch("INSERT INTO queues_v9 SELECT * FROM queues; DROP TABLE queues; ALTER TABLE queues_v9 RENAME TO queues;")?;
        for sql in auxiliaries {
            tx.execute_batch(&sql)?;
        }
    }
    let columns = tx
        .prepare("PRAGMA table_info(queues)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    for (column, definition) in [
        ("created_at_ms", "INTEGER NOT NULL DEFAULT 0"),
        ("modified_at_ms", "INTEGER NOT NULL DEFAULT 0"),
        ("last_purge_ms", "INTEGER"),
    ] {
        if !columns.iter().any(|name| name == column) {
            tx.execute_batch(&format!(
                "ALTER TABLE queues ADD COLUMN {column} {definition}"
            ))?;
        }
    }
    tx.execute_batch("CREATE TABLE IF NOT EXISTS queue_tags (
        queue_name TEXT NOT NULL REFERENCES queues(name) ON DELETE CASCADE,
        key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(queue_name, key));
        CREATE TABLE IF NOT EXISTS deleted_queues (name TEXT PRIMARY KEY NOT NULL, deleted_at_ms INTEGER NOT NULL);")?;
    Ok(())
}

fn validate_tag_component(value: &str, key: bool) -> Result<(), LqsError> {
    if (key && value.is_empty())
        || value.chars().count() > if key { 128 } else { 256 }
        || value.to_ascii_lowercase().starts_with("aws:")
        || !value.chars().all(|ch| {
            (!ch.is_control() || matches!(ch, '\t' | '\n' | '\r'))
                && (ch.is_alphanumeric() || ch.is_whitespace() || "_.:/=+-@".contains(ch))
        })
    {
        return Err(invalid(
            "tag keys must be 1-128 characters, values 0-256; use Unicode letters/numbers/whitespace or _ . : / = + - @, without the aws: prefix",
        ));
    }
    Ok(())
}

pub(super) fn validate_tags(tags: &QueueTags) -> Result<(), LqsError> {
    if tags.len() > 50 {
        return Err(invalid("LQS allows at most 50 tags per queue"));
    }
    for (key, value) in tags {
        validate_tag_component(key, true)?;
        validate_tag_component(value, false)?;
    }
    Ok(())
}

pub(super) fn write_tags(
    tx: &rusqlite::Transaction<'_>,
    queue: &str,
    tags: &QueueTags,
) -> Result<(), LqsError> {
    for (key, value) in tags {
        tx.execute("INSERT INTO queue_tags(queue_name, key, value) VALUES (?1, ?2, ?3) ON CONFLICT(queue_name, key) DO UPDATE SET value = excluded.value", params![queue, key, value])?;
    }
    Ok(())
}

impl Lqs {
    /// Case-sensitive, literal prefix filtering with keyset pagination (not a snapshot).
    pub fn list_queues(
        &self,
        prefix: &str,
        maximum: Option<usize>,
        next_cursor: Option<&str>,
    ) -> Result<QueuePage, LqsError> {
        let limit = maximum.unwrap_or(1000);
        if !(1..=1000).contains(&limit) {
            return Err(invalid("MaxResults must be 1-1000"));
        }
        let last = if let Some(encoded) = next_cursor {
            if encoded.len() > 2048 {
                return Err(invalid("invalid ListQueues cursor"));
            }
            let bytes = URL_SAFE_NO_PAD
                .decode(encoded)
                .map_err(|_| invalid("invalid ListQueues cursor"))?;
            let (version, original_prefix, last): (u8, String, String) =
                serde_json::from_slice(&bytes).map_err(|_| invalid("invalid ListQueues cursor"))?;
            if version != 1
                || original_prefix != prefix
                || !last.starts_with(prefix)
                || last.is_empty()
            {
                return Err(invalid("ListQueues cursor does not match the prefix"));
            }
            last
        } else {
            String::new()
        };
        let mut names = self.connection.prepare("SELECT name FROM queues WHERE substr(name, 1, length(?1)) = ?1 AND name > ?2 ORDER BY name LIMIT ?3")?.query_map(params![prefix, last, limit + 1], |row| row.get::<_, String>(0))?.collect::<Result<Vec<_>, _>>()?;
        let has_more = names.len() > limit;
        names.truncate(limit);
        let next_cursor = if has_more && maximum.is_some() {
            Some(
                URL_SAFE_NO_PAD.encode(
                    serde_json::to_vec(&(1u8, prefix, names.last().unwrap()))
                        .map_err(|e| LqsError::Database(e.to_string()))?,
                ),
            )
        } else {
            None
        };
        Ok(QueuePage {
            queue_names: names,
            next_cursor,
        })
    }

    pub fn queue_exists(&self, name: &str) -> Result<bool, LqsError> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM queues WHERE name = ?1)",
            [name],
            |row| row.get(0),
        )?)
    }

    /// Exact local snapshot; AWS-compatible approximate metric names at the HTTP layer.
    pub fn queue_metrics(&self, queue: &str, now_ms: u64) -> Result<QueueMetrics, LqsError> {
        self.connection.query_row("SELECT
            (SELECT COUNT(*) FROM messages WHERE queue_name = q.name AND created_at_ms > ?2 - q.message_retention_ms AND available_at_ms <= ?2 AND (invisible_until_ms IS NULL OR invisible_until_ms <= ?2)),
            (SELECT COUNT(*) FROM messages WHERE queue_name = q.name AND created_at_ms > ?2 - q.message_retention_ms AND invisible_until_ms > ?2),
            (SELECT COUNT(*) FROM messages WHERE queue_name = q.name AND created_at_ms > ?2 - q.message_retention_ms AND available_at_ms > ?2 AND (invisible_until_ms IS NULL OR invisible_until_ms <= ?2)),
            created_at_ms, modified_at_ms FROM queues q WHERE name = ?1", params![queue, ms(now_ms)], |row| Ok(QueueMetrics {visible:row.get(0)?, not_visible:row.get(1)?, delayed:row.get(2)?, created_at_ms:row.get(3)?, modified_at_ms:row.get(4)?})).optional()?.ok_or_else(|| LqsError::QueueNotFound(queue.into()))
    }

    pub fn list_queue_tags(&self, queue: &str) -> Result<QueueTags, LqsError> {
        self.queue_config(queue)?;
        Ok(self
            .connection
            .prepare("SELECT key, value FROM queue_tags WHERE queue_name = ?1 ORDER BY key")?
            .query_map([queue], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<_, _>>()?)
    }

    pub fn tag_queue(&mut self, queue: &str, tags: QueueTags) -> Result<(), LqsError> {
        if tags.is_empty() {
            return Err(invalid("Tags must not be empty"));
        }
        validate_tags(&tags)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        read_queue_config(&tx, queue)?;
        write_tags(&tx, queue, &tags)?;
        let total: usize = tx.query_row(
            "SELECT COUNT(*) FROM queue_tags WHERE queue_name = ?1",
            [queue],
            |row| row.get(0),
        )?;
        if total > 50 {
            return Err(invalid("LQS allows at most 50 tags per queue"));
        }
        tx.commit()?;
        Ok(())
    }

    pub fn untag_queue(&mut self, queue: &str, keys: &[String]) -> Result<(), LqsError> {
        if keys.is_empty() || keys.len() > 50 {
            return Err(invalid("TagKeys must contain 1-50 keys"));
        }
        for key in keys {
            validate_tag_component(key, true)?;
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        read_queue_config(&tx, queue)?;
        for key in keys {
            tx.execute(
                "DELETE FROM queue_tags WHERE queue_name = ?1 AND key = ?2",
                params![queue, key],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Deletes only messages currently in this queue. Settings, tags, DLQ policy,
    /// and send deduplication history survive. The cooldown survives restarts.
    pub fn purge_queue(&mut self, queue: &str, now_ms: u64) -> Result<(), LqsError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        read_queue_config(&tx, queue)?;
        let previous: Option<i64> = tx.query_row(
            "SELECT last_purge_ms FROM queues WHERE name = ?1",
            [queue],
            |row| row.get(0),
        )?;
        if previous.is_some_and(|time| ms(now_ms) < time.saturating_add(60_000)) {
            return Err(LqsError::PurgeQueueInProgress);
        }
        tx.execute("DELETE FROM messages WHERE queue_name = ?1", [queue])?;
        tx.execute(
            "UPDATE receive_attempts SET valid = 0 WHERE queue_name = ?1",
            [queue],
        )?;
        tx.execute(
            "UPDATE queues SET last_purge_ms = ?2 WHERE name = ?1",
            params![queue, ms(now_ms)],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Deletes queue-owned data and detaches incoming DLQ policies/origins without
    /// deleting messages in other queues. The name is reserved for 60 seconds.
    pub fn delete_queue(&mut self, queue: &str, now_ms: u64) -> Result<(), LqsError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        read_queue_config(&tx, queue)?;
        tx.execute("UPDATE queues SET modified_at_ms = ?2 WHERE name IN (SELECT source_queue FROM redrive_policies WHERE dead_letter_queue = ?1)", params![queue, ms(now_ms)])?;
        tx.execute(
            "DELETE FROM redrive_policies WHERE source_queue = ?1 OR dead_letter_queue = ?1",
            [queue],
        )?;
        tx.execute("UPDATE receive_attempts SET valid = 0 WHERE (queue_name, attempt_id) IN (SELECT r.queue_name, r.attempt_id FROM receive_attempt_members r JOIN messages m ON m.receipt_handle = r.receipt_handle JOIN dead_letter_origins o ON o.sequence = m.sequence WHERE o.source_queue = ?1)", [queue])?;
        tx.execute(
            "DELETE FROM dead_letter_origins WHERE source_queue = ?1",
            [queue],
        )?;
        tx.execute("DELETE FROM queues WHERE name = ?1", [queue])?;
        tx.execute("INSERT INTO deleted_queues(name, deleted_at_ms) VALUES (?1, ?2) ON CONFLICT(name) DO UPDATE SET deleted_at_ms = excluded.deleted_at_ms", params![queue, ms(now_ms)])?;
        tx.commit()?;
        Ok(())
    }
}
