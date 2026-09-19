use super::*;

pub(super) fn migrate(tx: &rusqlite::Transaction<'_>) -> Result<(), LqsError> {
    tx.execute_batch("CREATE TABLE IF NOT EXISTS queue_security (
        queue_name TEXT PRIMARY KEY NOT NULL REFERENCES queues(name) ON DELETE CASCADE,
        policy TEXT, sqs_managed_sse_enabled INTEGER NOT NULL DEFAULT 0 CHECK(sqs_managed_sse_enabled IN (0,1)),
        kms_master_key_id TEXT, kms_reuse_seconds INTEGER NOT NULL DEFAULT 300 CHECK(kms_reuse_seconds BETWEEN 60 AND 86400),
        CHECK(sqs_managed_sse_enabled = 0 OR kms_master_key_id IS NULL));
        INSERT OR IGNORE INTO queue_security(queue_name) SELECT name FROM queues;")?;
    Ok(())
}

pub(super) fn read(connection: &Connection, queue: &str) -> Result<QueueSecurity, LqsError> {
    Ok(connection.query_row("SELECT policy, sqs_managed_sse_enabled, kms_master_key_id, kms_reuse_seconds FROM queue_security WHERE queue_name = ?1", [queue], |row| Ok(QueueSecurity {policy:row.get(0)?, sqs_managed_sse_enabled:row.get(1)?, kms_master_key_id:row.get(2)?, kms_data_key_reuse_period_seconds:row.get(3)?}))?)
}

pub(super) fn write(
    tx: &rusqlite::Transaction<'_>,
    queue: &str,
    settings: &QueueSecurity,
) -> Result<(), LqsError> {
    settings.validate()?;
    tx.execute("INSERT INTO queue_security(queue_name, policy, sqs_managed_sse_enabled, kms_master_key_id, kms_reuse_seconds) VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(queue_name) DO UPDATE SET policy = excluded.policy, sqs_managed_sse_enabled = excluded.sqs_managed_sse_enabled, kms_master_key_id = excluded.kms_master_key_id, kms_reuse_seconds = excluded.kms_reuse_seconds",
        params![queue, settings.policy, settings.sqs_managed_sse_enabled, settings.kms_master_key_id, settings.kms_data_key_reuse_period_seconds])?;
    Ok(())
}

impl Lqs {
    pub fn queue_security(&self, queue: &str) -> Result<QueueSecurity, LqsError> {
        queue_type(&self.connection, queue)?;
        read(&self.connection, queue)
    }

    pub fn authorize_queue(
        &self,
        queue: &str,
        identity: &RequestIdentity,
        action: &str,
    ) -> Result<(), LqsError> {
        self.queue_security(queue)?.authorize(
            identity,
            action,
            &format!("{}{queue}", crate::LOCAL_QUEUE_ARN_PREFIX),
        )
    }

    pub fn add_permission(
        &mut self,
        queue: &str,
        label: &str,
        accounts: &[String],
        actions: &[String],
        now_ms: u64,
    ) -> Result<(), LqsError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut security = read_queue_config(&tx, queue)?.security;
        security.policy = Some(crate::security::add_permission(
            security.policy.as_deref(),
            queue,
            label,
            accounts,
            actions,
        )?);
        write(&tx, queue, &security)?;
        tx.execute(
            "UPDATE queues SET modified_at_ms = ?2 WHERE name = ?1",
            params![queue, ms(now_ms)],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn remove_permission(
        &mut self,
        queue: &str,
        label: &str,
        now_ms: u64,
    ) -> Result<(), LqsError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut security = read_queue_config(&tx, queue)?.security;
        security.policy = crate::security::remove_permission(security.policy.as_deref(), label)?;
        write(&tx, queue, &security)?;
        tx.execute(
            "UPDATE queues SET modified_at_ms = ?2 WHERE name = ?1",
            params![queue, ms(now_ms)],
        )?;
        tx.commit()?;
        Ok(())
    }
}
