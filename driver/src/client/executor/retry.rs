use std::{collections::HashSet, time::Duration};

use crate::{
    bson_util::round_clamp,
    error::{Error, Result, NO_WRITES_PERFORMED, RETRYABLE_ERROR, SYSTEM_OVERLOADED_ERROR},
    operation::{Operation, Retryability},
    options::ServerAddress,
    Client,
};

/// The maximum number of retries that can be performed when the system is overloaded.
const MAX_OVERLOADED_RETRIES: u8 = 5;
/// The maximum number of retries that can be performed for retryable reads/writes.
const MAX_RW_RETRIES: u8 = 1;

pub(super) struct Retry {
    attempt: u8,
    pub(super) deprioritized_servers: HashSet<ServerAddress>,
    pub(super) last_error: Error,
    txn_number: Option<i64>,
    pub(super) overloaded: bool,
}

impl Retry {
    pub(super) fn max_retries_reached(&self) -> bool {
        if self.overloaded {
            self.attempt > MAX_OVERLOADED_RETRIES
        } else {
            self.attempt > MAX_RW_RETRIES
        }
    }

    pub(super) fn calculate_backoff(&self) -> Duration {
        const BACKOFF_INITIAL_MS: f64 = 100f64;
        const BACKOFF_MAX_MS: f64 = 10_000f64;
        const RETRY_BASE: f64 = 2f64;

        let jitter = rand::random_range(0f64..1f64);

        let computed_backoff =
            jitter * BACKOFF_INITIAL_MS * RETRY_BASE.powf(f64::from(self.attempt - 1));
        let max_backoff = jitter * BACKOFF_MAX_MS;
        let backoff: u64 = std::cmp::min(round_clamp(computed_backoff), round_clamp(max_backoff));
        Duration::from_millis(backoff)
    }
}

pub(super) fn handle_connection_establishment_failure<T: Operation>(
    retry: Option<Retry>,
    error: Error,
    op: &T,
    client: &Client,
    server: ServerAddress,
    in_transaction: bool,
) -> Result<Retry> {
    handle_failure(retry, error, op, client, server, in_transaction, None, true)
}

pub(super) fn handle_execution_failure<T: Operation>(
    retry: Option<Retry>,
    error: Error,
    op: &T,
    client: &Client,
    server: ServerAddress,
    is_transaction_op: bool,
    txn_number: Option<i64>,
) -> Result<Retry> {
    handle_failure(
        retry,
        error,
        op,
        client,
        server,
        is_transaction_op,
        txn_number,
        false,
    )
}

/// Handles a failure by either creating a new `Retry` or updating the existing `Retry` with the new
/// error. If the error cannot be retried, the appropriate error to return from the retry loop is
/// returned and no further retries should be performed.
#[allow(clippy::too_many_arguments)]
fn handle_failure<T: Operation>(
    retry: Option<Retry>,
    error: Error,
    op: &T,
    client: &Client,
    server: ServerAddress,
    is_transaction_op: bool,
    txn_number: Option<i64>,
    error_is_connection_establishment: bool,
) -> Result<Retry> {
    let can_retry = if error.contains_label(SYSTEM_OVERLOADED_ERROR)
        && error.contains_label(RETRYABLE_ERROR)
        && op.is_backpressure_retryable(client.options())
    {
        true
    } else {
        let retryability = op.retryability(client.options());
        // Pool cleared errors should be retried for reads regardless of transaction status.
        retryability == Retryability::Read && error.is_pool_cleared()
            || retryability.can_retry_error(&error) && !is_transaction_op
    };
    let overloaded = error.contains_label(SYSTEM_OVERLOADED_ERROR);
    let deprioritized = if client.is_sharded() || overloaded {
        Some(server)
    } else {
        None
    };

    match retry {
        Some(mut retry) => {
            let better_error = if (error.is_server_error()
                || error.is_read_retryable()
                || error.is_write_retryable())
                && !error_is_connection_establishment
                && !error.contains_label(NO_WRITES_PERFORMED)
            {
                error
            } else {
                retry.last_error
            };
            if !can_retry {
                return Err(better_error);
            }
            retry.attempt += 1;
            if let Some(server) = deprioritized {
                retry.deprioritized_servers.insert(server);
            }
            retry.last_error = better_error;
            if txn_number.is_some() && retry.txn_number.is_none() {
                retry.txn_number = txn_number;
            }
            retry.overloaded = overloaded;
            Ok(retry)
        }
        None => {
            if !can_retry {
                return Err(error);
            }
            Ok(Retry {
                attempt: 1,
                deprioritized_servers: deprioritized
                    .map(|server| HashSet::from([server]))
                    .unwrap_or_default(),
                last_error: error,
                txn_number,
                overloaded,
            })
        }
    }
}

pub(super) fn return_last_error(retry: &mut Option<Retry>) -> Result<()> {
    match retry.take() {
        Some(retry) => Err(retry.last_error),
        _ => Ok(()),
    }
}

pub(super) fn txn_number(retry: &Option<Retry>) -> Option<i64> {
    retry.as_ref().and_then(|r| r.txn_number)
}
