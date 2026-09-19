use std::collections::HashSet;

use crate::{Lqs, LqsError, MAX_MESSAGE_BYTES, SendRequest, SendResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchEntry<T> {
    pub id: String,
    pub value: T,
}

#[derive(Debug)]
pub struct BatchResult<T, E = LqsError> {
    pub successful: Vec<BatchEntry<T>>,
    pub failed: Vec<BatchEntry<E>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibilityChange {
    pub receipt_handle: String,
    pub timeout_ms: u64,
}

pub(crate) fn validate_batch_ids<'a>(ids: impl Iterator<Item = &'a str>) -> Result<(), LqsError> {
    let ids: Vec<_> = ids.collect();
    if ids.is_empty() {
        return Err(LqsError::InvalidBatch("EmptyBatchRequest"));
    }
    if ids.len() > 10 {
        return Err(LqsError::InvalidBatch("TooManyEntriesInBatchRequest"));
    }
    let mut seen = HashSet::new();
    for id in ids {
        if !(1..=80).contains(&id.len())
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        {
            return Err(LqsError::InvalidBatch("InvalidBatchEntryId"));
        }
        if !seen.insert(id) {
            return Err(LqsError::InvalidBatch("BatchEntryIdsNotDistinct"));
        }
    }
    Ok(())
}

pub(crate) fn validate_batch_size(sizes: impl Iterator<Item = usize>) -> Result<(), LqsError> {
    let total = sizes.fold(0usize, usize::saturating_add);
    if total > MAX_MESSAGE_BYTES {
        return Err(LqsError::InvalidBatch("BatchRequestTooLong"));
    }
    Ok(())
}

impl Lqs {
    /// Each entry commits independently, in input order. Request-level errors write nothing.
    pub fn send_batch(
        &mut self,
        queue: &str,
        entries: Vec<BatchEntry<SendRequest>>,
        now_ms: u64,
    ) -> Result<BatchResult<SendResult>, LqsError> {
        validate_batch_size(entries.iter().map(|entry| {
            crate::message_attributes::payload_size(
                &entry.value.body,
                &entry.value.message_attributes,
            )
        }))?;
        self.run_batch(queue, entries, |lqs, request| {
            lqs.send(queue, request, now_ms)
        })
    }

    pub fn delete_batch(
        &mut self,
        queue: &str,
        entries: Vec<BatchEntry<String>>,
    ) -> Result<BatchResult<()>, LqsError> {
        self.run_batch(queue, entries, |lqs, handle| lqs.delete(queue, &handle))
    }

    pub fn change_visibility_batch(
        &mut self,
        queue: &str,
        entries: Vec<BatchEntry<VisibilityChange>>,
        now_ms: u64,
    ) -> Result<BatchResult<()>, LqsError> {
        self.run_batch(queue, entries, |lqs, change| {
            lqs.change_visibility(queue, &change.receipt_handle, change.timeout_ms, now_ms)
        })
    }

    // Shared by typed library APIs and HTTP, which can also have per-entry parse errors.
    pub(crate) fn run_batch<T, R, E: From<LqsError>>(
        &mut self,
        queue: &str,
        entries: Vec<BatchEntry<T>>,
        mut operation: impl FnMut(&mut Self, T) -> Result<R, E>,
    ) -> Result<BatchResult<R, E>, E> {
        validate_batch_ids(entries.iter().map(|entry| entry.id.as_str()))?;
        self.queue_config(queue)?;
        let mut result = BatchResult {
            successful: Vec::new(),
            failed: Vec::new(),
        };
        for entry in entries {
            match operation(self, entry.value) {
                Ok(value) => result.successful.push(BatchEntry {
                    id: entry.id,
                    value,
                }),
                Err(value) => result.failed.push(BatchEntry {
                    id: entry.id,
                    value,
                }),
            }
        }
        Ok(result)
    }
}
