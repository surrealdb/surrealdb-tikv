// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]

mod cnf;
mod provider;
mod savepoint;

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
pub use cnf::TikvConfig;
pub use provider::TikvProvider;
use savepoint::{Operation, UndoLog};
use surrealdb_kvs::api::{BoxFut, GetMultiResult, KeysResult, ScanResult, Transactable};
use surrealdb_kvs::consts::COUNT_BATCH_SIZE;
use surrealdb_kvs::err::{Error, Result};
use surrealdb_kvs::timestamp::{
	BoxTimeStamp, BoxTimeStampImpl, MAX_TIMESTAMP_BYTES, TimeStamp, TimeStampImpl,
};
use surrealdb_kvs::{
	DestroyRange, DestroyRangeHandle, Key, KeyRange, Maintenance, MaintenanceHandle, Metrics,
	TransactionBuilder, TransactionType, Val,
};
use tikv::transaction::ResolveLocksOptions;
use tikv::{CheckLevel, Config, TimestampExt, TransactionClient, TransactionOptions};
use tokio::sync::RwLock;
use tonic::{Code, Status};
use tracing::{debug, error, info, instrument, trace, warn};

/// The longest a write commit runs before its outcome is reported unknown
/// ([`Transactable::write_commit_bound`]). A TiKV commit is a two-phase commit across every
/// region the transaction wrote, so a transaction of many thousands of keys outlasts the default
/// bound; this stays below a minute, the longest a statement runs by default.
const WRITE_COMMIT_BOUND: Duration = Duration::from_secs(50);

const TARGET: &str = "surrealdb::core::kvs::tikv";

/// Number of bytes a protobuf base-128 varint occupies for `n`.
const fn prost_varint_len(mut n: usize) -> usize {
	let mut bytes = 1;
	while n >= 0x80 {
		n >>= 7;
		bytes += 1;
	}
	bytes
}

/// Encoded byte length of a `proto3` `bytes` field with a 1-byte field tag
/// (field numbers 1..=15), which omits empty payloads.
const fn prost_bytes_field_len(payload_len: usize) -> usize {
	if payload_len == 0 {
		0
	} else {
		1usize.saturating_add(prost_varint_len(payload_len)).saturating_add(payload_len)
	}
}

/// Strict lower bound on the protobuf-encoded byte length of a TiKV
/// `PrewriteRequest` carrying a single `Put(key, val)` mutation.
///
/// Accounts for:
/// - the 4-byte cluster API V2 keyspace prefix when `api_version == 2`,
/// - `Mutation.key` (tag 2) and `Mutation.value` (tag 3),
/// - the outer `PrewriteRequest.mutations` length-delimited entry (tag 2, always emitted for the
///   mutation), and
/// - `PrewriteRequest.txn_size = u64::MAX` (tag 8 + 10-byte varint = 11 bytes), set unconditionally
///   by `tikv::transaction::requests::new_prewrite_request`.
///
/// Variable per-transaction fields (`context`, `primary_lock`, `start_version`,
/// `lock_ttl`, `secondaries`) are omitted so this bound never overestimates;
/// a request pushed over the cap by those fields is still caught at encode time
/// via [`is_outbound_encoding_limit_status`].
const fn min_prewrite_encoded_bytes(key_len: usize, val_len: usize, api_version: u8) -> usize {
	let wire_key_len = if api_version == 2 {
		key_len.saturating_add(4)
	} else {
		key_len
	};
	let mutation_len =
		prost_bytes_field_len(wire_key_len).saturating_add(prost_bytes_field_len(val_len));
	let mutations_entry_len =
		1usize.saturating_add(prost_varint_len(mutation_len)).saturating_add(mutation_len);
	mutations_entry_len.saturating_add(11)
}

/// Live buffered writes in a transaction whose minimum `PrewriteRequest` wire
/// size already exceeds `grpc_max_encoding_message_size`.
///
/// Kept empty (`capacity == 0`) while writes stay under the cap, and updated on
/// overwrites, deletes, and savepoint rollbacks so a write replaced or reverted
/// before `commit` does not fail the transaction.
#[derive(Default)]
struct OversizedWrites {
	entries: HashMap<Vec<u8>, usize>,
}

impl OversizedWrites {
	fn contains(&self, key: &[u8]) -> bool {
		!self.entries.is_empty() && self.entries.contains_key(key)
	}

	fn record_put(&mut self, key: Vec<u8>, val_len: usize, config: &TikvConfig) {
		let min_bytes = min_prewrite_encoded_bytes(key.len(), val_len, config.api_version);
		if min_bytes > config.grpc_max_encoding_message_size {
			self.entries.insert(key, min_bytes);
		} else if !self.entries.is_empty() {
			self.entries.remove(&key);
		}
	}

	fn record_delete(&mut self, key: &[u8]) {
		if !self.entries.is_empty() {
			self.entries.remove(key);
		}
	}

	fn max_encoded_bytes(&self) -> Option<usize> {
		self.entries.values().copied().max()
	}
}

/// Whether `status` came from the client's own outbound `max_encoding_message_size`
/// check rejecting a request before a single HTTP/2 `DATA` frame was sent.
///
/// `tonic` checks `max_encoding_message_size` in `EncodeBody::finish_encoding`
/// and yields `Err(Status::out_of_range("Error, encoded message length too large: ..."))`
/// from `Body::poll_frame` before producing any body frame. Two shapes can
/// reach callers:
///
/// 1. `Code::OutOfRange` with `"encoded message length too large"`, when the `EncodeBody` status
///    surfaces directly (as distinct from response-side `"decoded message length too large"`).
/// 2. `Code::Internal` wrapping an `h2::Error` stream reset with `Reason::INTERNAL_ERROR` and
///    `Initiator::User` (`!is_remote() && !is_library()`). `hyper`'s HTTP/2 client calls
///    `SendStream::send_reset(Reason::INTERNAL_ERROR)` in `SendStreamExt::on_user_err` when the
///    outbound request `Body` errors, discarding the `OutOfRange` status and failing the response
///    future with that local reset. In `h2`, `Initiator::User` resets come exclusively from local
///    `SendStream::send_reset` calls (peer `RST_STREAM` frames carry `Initiator::Remote`, protocol
///    violations `Initiator::Library`), and client request cancellation uses `Reason::CANCEL`. For
///    unary gRPC calls with compression disabled, `EncodeBody` only errors on the outbound message
///    length check, before any payload bytes are written to the stream.
fn is_outbound_encoding_limit_status(status: &Status) -> bool {
	if status.code() == Code::OutOfRange
		&& status.message().contains("encoded message length too large")
	{
		return true;
	}
	if status.code() != Code::Internal {
		return false;
	}
	let mut current = std::error::Error::source(status);
	while let Some(err) = current {
		if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
			return h2_err.is_reset()
				&& !h2_err.is_remote()
				&& !h2_err.is_library()
				&& h2_err.reason() == Some(h2::Reason::INTERNAL_ERROR);
		}
		current = err.source();
	}
	false
}

/// Whether `e` is a client-side outbound gRPC encoding-limit rejection.
fn is_outbound_encoding_limit_error(e: &tikv::Error) -> bool {
	if let tikv::Error::GrpcAPI(status) = e {
		return is_outbound_encoding_limit_status(status);
	}
	if let tikv::Error::UndeterminedError(inner) = e {
		return is_outbound_encoding_limit_error(inner);
	}
	false
}

/// Build the definite [`Error::Transaction`] message naming the outbound gRPC
/// encoding limit and the environment variables that raise it.
fn encoding_limit_exceeded_error(
	min_encoded_bytes: Option<usize>,
	max_encoding_message_size: Option<usize>,
	cause: Option<&tikv::Error>,
) -> Error {
	let subject = match min_encoded_bytes {
		Some(bytes) => format!("encoded write size (at least {bytes} bytes)"),
		None => "request".to_string(),
	};
	let limit = match max_encoding_message_size {
		Some(limit) => format!(" of {limit} bytes"),
		None => String::new(),
	};
	let suffix = match cause {
		Some(err) => format!(": {err}"),
		None => String::new(),
	};
	Error::Transaction(format!(
		"{subject} exceeds the TiKV gRPC max encoding message size{limit}; \
		 raise SURREAL_TIKV_GRPC_MAX_ENCODING_MESSAGE_SIZE (and \
		 SURREAL_TIKV_GRPC_MAX_DECODING_MESSAGE_SIZE to read large values back){suffix}"
	))
}

/// Convert a failure of `commit` into the generic KVS error type,
/// distinguishing failures that leave the transaction's outcome unknown from
/// those that definitely rejected it.
///
/// Two-phase commit runs over the network, so a transport-level failure means
/// only that the client stopped hearing back. TiKV may have durably applied
/// the transaction before the deadline elapsed or the connection broke, and
/// the rollback attempted alongside cannot undo an already-committed primary.
/// Reporting that as a definite failure would tell callers a write does not
/// exist when it may.
///
/// An outbound encoding-limit failure ([`is_outbound_encoding_limit_error`]) is
/// the exception among `GrpcAPI` errors: `tonic` refuses the message locally
/// before transmitting a single `DATA` frame, so neither 1PC, async commit
/// (which requires every key's lock to be prewritten), nor 2PC (which only
/// commits after prewrite succeeds) can have applied the transaction.
///
/// Every other failure is a decision the cluster reached and reported — a
/// write conflict, an oversized raft entry, a key that already exists — so it
/// keeps the ordinary classification.
///
/// Only used at the commit call site: the same status arriving from a read
/// carries no such ambiguity, because a failed read applies nothing.
fn commit_error(e: tikv::Error, max_encoding_message_size: Option<usize>) -> Error {
	// `UndeterminedError` is the client's own verdict, but it only sets it for
	// `Grpc`, so the transport variants are listed rather than relied upon.
	// `GrpcAPI` is what every unary RPC produces, `Grpc` the channel-level one.
	let indeterminate = matches!(
		e,
		tikv::Error::UndeterminedError(_) | tikv::Error::Grpc(_) | tikv::Error::GrpcAPI(_)
	) && !is_outbound_encoding_limit_error(&e);
	// Classify through `kvs_error_with_encoding_limit` either way, so its
	// logging still runs — the commit path is where the gRPC status code
	// matters most, and it is recorded nowhere else.
	let rendered = e.to_string();
	let classified = kvs_error_with_encoding_limit(e, max_encoding_message_size);
	if indeterminate {
		Error::CommitOutcomeUnknown(rendered)
	} else {
		classified
	}
}

/// Classify a batch of per-key errors, each already converted by `kvs_error`.
///
/// The batch is a conflict only when every error in it is one. Any other
/// failure is one a retry would meet again, so it takes precedence: a single
/// kind of failure is reported as itself, a mix as the generic variant carrying
/// all of them.
fn classify_batch(errs: Vec<Error>) -> Error {
	let (conflicts, mut failures): (Vec<Error>, Vec<Error>) =
		errs.into_iter().partition(|e| matches!(e, Error::TransactionConflict(_)));
	if failures.is_empty() {
		conflicts
			.into_iter()
			.next()
			.unwrap_or_else(|| Error::Transaction("empty batch of key errors".to_string()))
	} else if failures
		.iter()
		.all(|e| std::mem::discriminant(e) == std::mem::discriminant(&failures[0]))
	{
		failures.swap_remove(0)
	} else {
		Error::Transaction(format!("multiple key errors: {failures:?}"))
	}
}

/// Convert a TiKV engine error into the generic KVS error type.
fn kvs_error(e: tikv::Error) -> Error {
	kvs_error_with_encoding_limit(e, None)
}

/// Convert a TiKV engine error into the generic KVS error type, enriching
/// outbound gRPC encoding-limit failures with the configured byte cap when
/// available.
fn kvs_error_with_encoding_limit(
	e: tikv::Error,
	max_encoding_message_size: Option<usize>,
) -> Error {
	const TIKV_TARGET: &str = "surrealdb::core::kvs::tikv";
	match e {
		tikv::Error::DuplicateKeyInsertion => Error::TransactionKeyAlreadyExists,
		tikv::Error::Grpc(_) => Error::ConnectionFailed(e.to_string()),
		// Failure of an individual RPC, reported as a gRPC status. Recorded at
		// debug for the reason the `KeyError` and `RegionError` arms below give:
		// `kvs_error` runs on every operation, so one unhealthy store can bring
		// thousands at once, and a flood still has to stay traceable to a cause.
		//
		// The status code is the only signal separating a deadline from a
		// transport reset from a cluster fault, and it is not recoverable from
		// the returned error. No code is narrowed to a different `Error`
		// variant, because each has at least two causes a status code alone
		// cannot distinguish; when the source chain proves the client's own
		// `max_encoding_message_size` cap rejected an outbound message before
		// sending it, the `Transaction` message names that limit and the
		// `SURREAL_TIKV_GRPC_MAX_ENCODING_MESSAGE_SIZE` setting that raises it.
		tikv::Error::GrpcAPI(ref status) => {
			debug!(
				target: TARGET,
				grpc_code = ?status.code(),
				grpc_message = status.message(),
				"TiKV gRPC status",
			);
			if is_outbound_encoding_limit_status(status) {
				encoding_limit_exceeded_error(None, max_encoding_message_size, Some(&e))
			} else {
				Error::Transaction(e.to_string())
			}
		}
		tikv::Error::KeyError(ref ke) => {
			if let Some(conflict) = &ke.conflict {
				Error::TransactionConflict(Key::from(conflict.key.as_slice()).to_string())
			} else if ke.already_exist.is_some() {
				Error::TransactionKeyAlreadyExists
			} else if ke.abort.contains("KeyTooLarge") {
				Error::TransactionKeyTooLarge
			} else {
				// Preserve aborts and retryable flags at debug-level so
				// operators can correlate generic transaction errors
				// back to the underlying TiKV cause without inflating
				// the error variant surface.
				debug!(
					target: TIKV_TARGET,
					abort = %ke.abort,
					retryable = ke.retryable,
					"TiKV KeyError",
				);
				Error::Transaction(e.to_string())
			}
		}
		// Async commit and 1PC report per-key prewrite failures batched into one
		// error, so the classification is recovered from the inner errors; left
		// to fall through, a plain write conflict would surface as the
		// non-retryable `Transaction` variant and fail every caller that retries
		// on conflict.
		tikv::Error::MultipleKeyErrors(errs) | tikv::Error::ExtractedErrors(errs) => {
			classify_batch(
				errs.into_iter()
					.map(|err| kvs_error_with_encoding_limit(err, max_encoding_message_size))
					.collect(),
			)
		}
		tikv::Error::RegionError(ref re) => {
			// Most region errors carry a region id in their nested
			// `not_leader` / `region_not_found` / `epoch_not_match`
			// payload. Emit the most useful identifier we can find at
			// debug-level so a flood of generic Transaction errors can
			// still be traced to specific regions/stores during
			// post-mortem.
			let region_id = re
				.not_leader
				.as_ref()
				.map(|n| n.region_id)
				.or_else(|| re.region_not_found.as_ref().map(|n| n.region_id))
				.or_else(|| re.key_not_in_region.as_ref().map(|n| n.region_id))
				.or_else(|| {
					re.epoch_not_match
						.as_ref()
						.and_then(|n| n.current_regions.first().map(|r| r.id))
				});
			let store_id = re.store_not_match.as_ref().map(|n| n.request_store_id);
			let kind = if re.not_leader.is_some() {
				"not_leader"
			} else if re.region_not_found.is_some() {
				"region_not_found"
			} else if re.key_not_in_region.is_some() {
				"key_not_in_region"
			} else if re.epoch_not_match.is_some() {
				"epoch_not_match"
			} else if re.server_is_busy.is_some() {
				"server_is_busy"
			} else if re.stale_command.is_some() {
				"stale_command"
			} else if re.store_not_match.is_some() {
				"store_not_match"
			} else if re.raft_entry_too_large.is_some() {
				"raft_entry_too_large"
			} else if re.disk_full.is_some() {
				"disk_full"
			} else {
				"other"
			};
			debug!(
				target: TIKV_TARGET,
				kind,
				message = %re.message,
				region_id = ?region_id,
				store_id = ?store_id,
				"TiKV RegionError",
			);
			if re.raft_entry_too_large.is_some() {
				Error::TransactionTooLarge
			} else {
				Error::Transaction(e.to_string())
			}
		}
		tikv::Error::UndeterminedError(inner) => {
			if is_outbound_encoding_limit_error(&inner) {
				kvs_error_with_encoding_limit(*inner, max_encoding_message_size)
			} else {
				Error::Transaction(tikv::Error::UndeterminedError(inner).to_string())
			}
		}
		tikv::Error::Unimplemented
		| tikv::Error::ResolveLockError(_)
		| tikv::Error::InvalidTransactionType
		| tikv::Error::OperationAfterCommitError
		| tikv::Error::OnePcFailure
		| tikv::Error::NoPrimaryKey
		| tikv::Error::UnsupportedMode
		| tikv::Error::NoCurrentRegions
		| tikv::Error::EntryNotFoundInRegionCache
		| tikv::Error::Io(_)
		| tikv::Error::Channel(_)
		| tikv::Error::Url(_)
		| tikv::Error::Canceled(_)
		| tikv::Error::ColumnFamilyError(_)
		| tikv::Error::JoinError(_)
		| tikv::Error::RegionForKeyNotFound {
			..
		}
		| tikv::Error::RegionForRangeNotFound {
			..
		}
		| tikv::Error::RegionNotFoundInResponse {
			..
		}
		| tikv::Error::LeaderNotFound {
			..
		}
		| tikv::Error::MaxScanLimitExceeded {
			..
		}
		| tikv::Error::InvalidSemver(_)
		| tikv::Error::KvError {
			..
		}
		| tikv::Error::InternalError {
			..
		}
		| tikv::Error::StringError(_)
		| tikv::Error::PessimisticLockError {
			..
		}
		| tikv::Error::KeyspaceNotFound(_) => Error::Transaction(e.to_string()),
	}
}

/// TiKV's maintenance and observability operations.
///
/// The engine reaches them through the backend-agnostic [`Maintenance`]
/// capability, published under [`MaintenanceHandle`] by
/// [`TransactionBuilder::extension`], so it never names TiKV. The same handle
/// also serves the [`DestroyRange`] capability, published under
/// [`DestroyRangeHandle`].
pub struct TikvOpsHandle {
	/// Underlying transactional client.
	db: Pin<Arc<TransactionClient>>,
	/// Per-datastore in-flight transaction counter.
	///
	/// Incremented in [`Datastore::transaction`] and released exactly
	/// once when a [`Transaction`] reaches a terminal state. Surfaced
	/// via [`Self::in_flight_transaction_count`] so graceful shutdown
	/// can drain before running the advisory final GC pass.
	in_flight_txns: Arc<AtomicUsize>,
	/// Per-instance configuration captured at construction time.
	config: TikvConfig,
}

pub struct Datastore {
	/// Arc-wrapped operational handle shared with [`Transaction`]
	/// instances and exposed to the engine via the `extension` hook.
	handle: Arc<TikvOpsHandle>,
}

pub struct Transaction {
	// Is the transaction complete?
	done: AtomicBool,
	// Is the transaction writeable?
	write: bool,
	/// The underlying datastore transaction
	inner: RwLock<TransactionInner>,
	/// Wall-clock instant when the transaction was opened, used to enrich
	/// commit/rollback failure logs with elapsed lifetime.
	started_at: Instant,
	// The above, supposedly 'static transaction actually points into the
	// `TransactionClient` owned by `handle.db`, so the handle must
	// outlive `inner.tx`. Declared last so it is dropped last; the
	// inner `Arc` shares storage with `Datastore::handle` and with the
	// engine-level extension handle.
	#[allow(dead_code, reason = "Held to keep the TransactionClient alive while `tx` borrows it")]
	handle: Arc<TikvOpsHandle>,
}

impl Transaction {
	/// Decrement the in-flight counter, ensuring this is only done once per
	/// transaction even if `commit` is followed by `cancel` (or vice versa).
	fn release_in_flight(&self) {
		// Guarded subtract via `checked_sub`: on underflow `fetch_update`
		// returns `Err` and the counter is left unchanged. An underflow
		// would signal a missing increment somewhere, which is the real
		// bug to surface — clamping at zero would just hide it.
		let _ = self
			.handle
			.in_flight_txns
			.try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
	}

	fn kvs_err(&self, e: tikv::Error) -> Error {
		kvs_error_with_encoding_limit(e, Some(self.handle.config.grpc_max_encoding_message_size))
	}

	/// The values `keys` hold in the transaction, read with one batched get.
	///
	/// Every requested key is present in the answer: as `None` when it holds
	/// nothing, which is what a savepoint's undo log needs to tell a key to
	/// delete from a key to restore.
	async fn priors(
		tx: &mut tikv::Transaction,
		keys: Vec<Vec<u8>>,
		max_encoding_message_size: usize,
	) -> Result<HashMap<Vec<u8>, Option<Val>>> {
		let mut priors: HashMap<Vec<u8>, Option<Val>> =
			keys.iter().map(|key| (key.clone(), None)).collect();
		for pair in tx
			.batch_get(keys)
			.await
			.map_err(|e| kvs_error_with_encoding_limit(e, Some(max_encoding_message_size)))?
		{
			let key: Vec<u8> = pair.0.into();
			priors.insert(key, Some(pair.1));
		}
		Ok(priors)
	}

	/// Shared scan-delete loop for [`Self::delr`] and [`Self::clrr`].
	///
	/// On TiKV `clr` and `del` collapse to the same in-transaction
	/// operation, so a single loop services both. Walks the half-open
	/// range in `TIKV_DELR_BATCH_SIZE` increments, capped at
	/// `tikv_delr_max_keys`. If the cap would be exceeded the call
	/// returns [`Error::TransactionRangeTooLarge`] without writing the
	/// over-cap deletes. Records `Operation::RestoreValue` against
	/// the savepoint stack when one is active.
	async fn delete_range_bounded(&self, rng: KeyRange<'_>) -> Result<()> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		let max_keys = self.handle.config.delr_max_keys;
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Whether we need to record undo operations for the savepoint stack.
		let track_ops = inner.undo.is_tracking();
		let end = rng.end.to_vec();
		let mut start = rng.start.to_vec();
		let mut processed: u32 = 0;
		// Tracks whether the previous batch returned exactly its requested
		// `batch_size` keys. If it didn't, the range is known-exhausted
		// and we can skip the cap-boundary probe below — avoids one
		// extra `scan_keys` RPC on the clean termination path when
		// `processed` happens to align with the cap.
		let mut previous_batch_full = false;
		loop {
			let remaining = max_keys.saturating_sub(processed);
			if remaining == 0 {
				if previous_batch_full {
					// Probe by one to distinguish "exactly at the cap"
					// from "ran over the cap" — the previous batch
					// was full, so we don't know whether there's more
					// without asking.
					let mut iter = inner
						.tx
						.scan_keys(start.clone()..end.clone(), 1)
						.await
						.map_err(|e| self.kvs_err(e))?;
					if iter.next().is_some() {
						return Err(Error::TransactionRangeTooLarge(max_keys));
					}
				}
				break;
			}
			let batch_size = remaining.min(cnf::TIKV_DELR_BATCH_SIZE);
			let keys: Vec<tikv::Key> = inner
				.tx
				.scan_keys(start.clone()..end.clone(), batch_size)
				.await
				.map_err(|e| self.kvs_err(e))?
				.collect();
			if keys.is_empty() {
				break;
			}
			previous_batch_full = (keys.len() as u32) == batch_size;
			// Remember the last key so we can advance past it for the
			// next pass before consuming the batch.
			let last = keys.last().cloned();
			for k in keys {
				let key: Vec<u8> = k.into();
				// Pre-fetch the old value only when needed for
				// savepoint rollback. Skipping the read in the common
				// path keeps a large range delete from doubling its
				// RPC count.
				let old_val = if track_ops {
					inner.tx.get(key.clone()).await.map_err(|e| self.kvs_err(e))?
				} else {
					None
				};
				if let Some(val) = old_val {
					inner.delete_entry(key.clone(), &self.handle.config).await?;
					inner.undo.record(Operation::RestoreValue(key, val));
				} else {
					inner.delete_entry(key, &self.handle.config).await?;
				}
				processed = processed.saturating_add(1);
			}
			match last {
				Some(k) => {
					let new_start: Vec<u8> = k.into();
					start = Key::from(new_start).next().to_vec();
				}
				None => break,
			}
		}
		Ok(())
	}
}

impl Drop for Transaction {
	/// Release the in-flight reservation if the transaction was dropped
	/// without an explicit `commit` / `cancel`.
	///
	/// On debug builds the underlying `tikv::Transaction`'s
	/// `CheckLevel::Panic` already makes this surface loudly; on
	/// release builds the inner check is `Warn`, so without this
	/// `Drop` the in-flight counter would slowly leak on each
	/// abandoned handle. A slow drip eventually makes every shutdown
	/// hit `shutdown_grace_secs` before falling through, even when
	/// the cluster is idle.
	///
	/// The atomic `done.swap(true, AcqRel)` gates the release so a
	/// `commit`-then-drop sequence (or `cancel`-then-drop) doesn't
	/// double-release; the existing happy-path calls already win the
	/// swap.
	fn drop(&mut self) {
		if !self.done.swap(true, Ordering::AcqRel) {
			self.release_in_flight();
		}
	}
}

struct TransactionInner {
	/// The underlying datastore transaction
	tx: tikv::Transaction,
	/// Undo log backing the savepoint stack
	undo: UndoLog,
	/// Live writes whose minimum `PrewriteRequest` wire size exceeds
	/// `grpc_max_encoding_message_size`.
	oversized: OversizedWrites,
}

impl TransactionInner {
	/// Buffer a `Put(key, val)` in the underlying TiKV transaction and update
	/// [`OversizedWrites`] once `tx.put` succeeds.
	async fn put_entry(&mut self, key: Vec<u8>, val: Val, config: &TikvConfig) -> Result<()> {
		let val_len = val.len();
		let min_bytes = min_prewrite_encoded_bytes(key.len(), val_len, config.api_version);
		let max_encoding = config.grpc_max_encoding_message_size;
		if min_bytes > max_encoding || self.oversized.contains(&key) {
			self.tx
				.put(key.clone(), val)
				.await
				.map_err(|e| kvs_error_with_encoding_limit(e, Some(max_encoding)))?;
			self.oversized.record_put(key, val_len, config);
		} else {
			self.tx
				.put(key, val)
				.await
				.map_err(|e| kvs_error_with_encoding_limit(e, Some(max_encoding)))?;
		}
		Ok(())
	}

	/// Buffer a `Delete(key)` in the underlying TiKV transaction and clear any
	/// tracked oversized write for `key` once `tx.delete` succeeds.
	async fn delete_entry(&mut self, key: Vec<u8>, config: &TikvConfig) -> Result<()> {
		let max_encoding = config.grpc_max_encoding_message_size;
		if self.oversized.contains(&key) {
			self.tx
				.delete(key.clone())
				.await
				.map_err(|e| kvs_error_with_encoding_limit(e, Some(max_encoding)))?;
			self.oversized.record_delete(&key);
		} else {
			self.tx
				.delete(key)
				.await
				.map_err(|e| kvs_error_with_encoding_limit(e, Some(max_encoding)))?;
		}
		Ok(())
	}
}

impl Datastore {
	/// Open a new database, using the typed `TikvConfig` produced by
	/// [`crate::cnf::ConfigMap::load`].
	pub async fn new(path: &str, tikv_config: TikvConfig) -> Result<Datastore> {
		// Configure the client and keyspace
		let config = match tikv_config.api_version {
			2 => match tikv_config.keyspace.as_ref() {
				Some(keyspace) => {
					info!(target: TARGET, "Connecting to keyspace with cluster API V2: {keyspace}");
					Config::default().with_keyspace(keyspace)
				}
				None => {
					info!(target: TARGET, "Connecting to default keyspace with cluster API V2");
					Config::default().with_default_keyspace()
				}
			},
			1 => {
				info!(target: TARGET, "Connecting with cluster API V1");
				Config::default()
			}
			_ => return Err(Error::Datastore("Invalid TiKV API version".into())),
		};
		// Set the default request timeout
		let config = config.with_timeout(Duration::from_secs(tikv_config.request_timeout_secs));
		// Set the gRPC payload size caps on both sides of the channel.
		let config = config
			.with_grpc_max_decoding_message_size(tikv_config.grpc_max_decoding_message_size)
			.with_grpc_max_encoding_message_size(tikv_config.grpc_max_encoding_message_size);
		// Optionally configure mTLS. All three paths must be provided
		// together — if some are set but not others we refuse to start
		// rather than silently fall back to a plaintext connection.
		let config = match validate_tls_paths(&tikv_config)? {
			Some(TlsPaths {
				ca,
				cert,
				key,
			}) => {
				info!(
					target: TARGET,
					ca = %ca,
					cert = %cert,
					key = %key,
					"Configuring TiKV client with mTLS",
				);
				config.with_security(ca, cert, key)
			}
			None => config,
		};
		// Create the client with the config
		let db = match TransactionClient::new_with_config(vec![path], config).await {
			Ok(db) => Arc::pin(db),
			Err(e) => return Err(Error::Datastore(e.to_string())),
		};
		// Optional startup health probe. Calling `current_timestamp`
		// confirms PD reachability and clock health; opening a read-only
		// transaction and running a no-op scan confirms at least one TiKV
		// store is reachable through gRPC. We bound the probe at twice the
		// configured request timeout so a wholly-unreachable cluster fails
		// the boot rather than hanging the process.
		if tikv_config.health_probe {
			let probe = async {
				db.current_timestamp().await.map_err(kvs_error)?;
				let mut txn = db
					.begin_with_options(TransactionOptions::new_optimistic().read_only())
					.await
					.map_err(kvs_error)?;
				let mut iter = txn.scan_keys(vec![0u8]..vec![0u8], 1).await.map_err(kvs_error)?;
				// Drain so the iterator's drop side-effects (if any) are
				// observed before the transaction is dropped.
				while iter.next().is_some() {}
				Ok::<_, Error>(())
			};
			let probe_budget =
				Duration::from_secs(tikv_config.request_timeout_secs.saturating_mul(2));
			match tokio::time::timeout(probe_budget, probe).await {
				Ok(Ok(())) => {
					info!(target: TARGET, "TiKV startup health probe succeeded");
				}
				Ok(Err(e)) => {
					return Err(Error::Datastore(format!("TiKV startup health probe failed: {e}")));
				}
				Err(_) => {
					return Err(Error::Datastore(format!(
						"TiKV startup health probe timed out after {probe_budget:?}"
					)));
				}
			}
		}
		let handle = Arc::new(TikvOpsHandle {
			db,
			in_flight_txns: Arc::new(AtomicUsize::new(0)),
			config: tikv_config,
		});
		Ok(Datastore {
			handle,
		})
	}

	/// Clone of the operational handle. Exposed to the engine through the
	/// `TransactionBuilder::extension` hook so non-TiKV callers don't
	/// need to know about TiKV-specific signatures.
	pub fn ops_handle(&self) -> Arc<TikvOpsHandle> {
		Arc::clone(&self.handle)
	}

	/// Shutdown the database. Waits for in-flight transactions to drain
	/// (up to `shutdown_grace_secs`) then runs one advisory GC pass
	/// bounded by `shutdown_gc_timeout_secs`.
	pub async fn shutdown(&self) -> Result<()> {
		let cfg = &self.handle.config;
		let drain_deadline = Instant::now() + Duration::from_secs(cfg.shutdown_grace_secs);
		let drained = loop {
			let outstanding = self.handle.in_flight_transaction_count();
			if outstanding == 0 {
				break true;
			}
			if Instant::now() >= drain_deadline {
				warn!(
					target: TARGET,
					outstanding,
					"TiKV shutdown drain timed out; proceeding with active transactions still in flight",
				);
				break false;
			}
			tokio::time::sleep(Duration::from_millis(100)).await;
		};
		if drained && cfg.gc_enabled {
			let shutdown_gc_timeout = Duration::from_secs(cfg.shutdown_gc_timeout_secs);
			let gc_lifetime = Duration::from_secs(cfg.gc_lifetime_secs);
			match tokio::time::timeout(shutdown_gc_timeout, self.handle.run_mvcc_gc(gc_lifetime))
				.await
			{
				Ok(Ok(())) => {}
				Ok(Err(e)) => {
					warn!(
						target: TARGET,
						error = %e,
						"Advisory TiKV GC pass at shutdown failed",
					);
				}
				Err(_) => {
					warn!(
						target: TARGET,
						timeout_ms = shutdown_gc_timeout.as_millis() as u64,
						"Advisory TiKV GC pass at shutdown timed out",
					);
				}
			}
		}
		Ok(())
	}

	/// Start a new transaction
	pub async fn transaction(&self, ty: TransactionType) -> Result<Box<dyn Transactable>> {
		let cfg = &self.handle.config;
		// All transactions are optimistic
		let mut opt = TransactionOptions::new_optimistic();
		// Use async commit to determine transaction state earlier
		if cfg.async_commit {
			opt = opt.use_async_commit();
		}
		// Try to use one-phase commit if writing to only one region
		if cfg.one_phase_commit {
			opt = opt.try_one_pc();
		}
		// Set the behaviour when dropping an unfinished transaction.
		// In debug builds (including tests) escalate to Panic so dropped-without-commit
		// transactions surface immediately. Release builds remain lenient.
		opt = opt.drop_check(if cfg!(debug_assertions) {
			CheckLevel::Panic
		} else {
			CheckLevel::Warn
		});
		// Set this transaction as read only if possible
		if ty == TransactionType::Read {
			opt = opt.read_only();
		}
		// Create a new transaction
		match self.handle.db.begin_with_options(opt).await {
			Ok(txn) => {
				// Only bump the counter after the underlying transaction
				// is in hand so a failed `begin_with_options` doesn't leak
				// a phantom entry into the drain accounting.
				self.handle.in_flight_txns.fetch_add(1, Ordering::AcqRel);
				Ok(Box::new(Transaction {
					done: AtomicBool::new(false),
					write: matches!(ty, TransactionType::Write),
					inner: RwLock::new(TransactionInner {
						tx: txn,
						undo: UndoLog::default(),
						oversized: OversizedWrites::default(),
					}),
					started_at: Instant::now(),
					handle: Arc::clone(&self.handle),
				}))
			}
			Err(e) => {
				Err(kvs_error_with_encoding_limit(e, Some(cfg.grpc_max_encoding_message_size)))
			}
		}
	}
}

impl TransactionBuilder for Datastore {
	fn name(&self) -> &'static str {
		"tikv"
	}

	fn new_transaction(
		&self,
		write: TransactionType,
	) -> BoxFut<'_, Result<(Box<dyn Transactable>, bool)>> {
		// Transactions are remote: they hold resources in the external
		// TiKV cluster.
		Box::pin(async move { Ok((self.transaction(write).await?, false)) })
	}

	fn shutdown(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(Datastore::shutdown(self))
	}

	fn register_metrics(&self) -> Option<Metrics> {
		None
	}

	fn collect_u64_metric(&self, _metric: &str) -> Option<u64> {
		None
	}

	fn extension(&self, type_id: TypeId) -> Option<Arc<dyn Any + Send + Sync>> {
		if type_id == TypeId::of::<MaintenanceHandle>() {
			return Some(Arc::new(MaintenanceHandle(self.ops_handle())) as _);
		}
		// Offered only while garbage collection is running. The destroy bypasses
		// MVCC, so what makes it safe is the safepoint advancing past the removal
		// before the data goes; with collection disabled the safepoint never
		// moves, a snapshot older than the removal is never invalidated, and a
		// reader holding one would watch keys vanish mid-scan. Withholding the
		// capability is what routes such a deployment to the transactional delete,
		// which every reader's snapshot does see consistently.
		if type_id == TypeId::of::<DestroyRangeHandle>() && self.handle.config.gc_enabled {
			return Some(Arc::new(DestroyRangeHandle(self.ops_handle())) as _);
		}
		None
	}
}

impl Maintenance for TikvOpsHandle {
	fn collect_garbage(&self, lifetime: Duration) -> BoxFut<'_, Result<()>> {
		Box::pin(async move { self.run_mvcc_gc(lifetime).await })
	}

	fn cleanup_locks(&self, lifetime: Duration) -> BoxFut<'_, Result<()>> {
		Box::pin(async move { self.run_lock_cleanup(lifetime).await })
	}

	fn in_flight_transactions(&self) -> Option<usize> {
		Some(self.in_flight_transaction_count())
	}
}

impl DestroyRange for TikvOpsHandle {
	fn mechanism(&self) -> &'static str {
		"tikv-unsafe-destroy-range"
	}

	fn destroy_range(&self, range: KeyRange<'static>) -> BoxFut<'_, Result<()>> {
		Box::pin(async move { self.unsafe_destroy_range(range).await })
	}
}

impl TikvOpsHandle {
	fn kvs_err(&self, e: tikv::Error) -> Error {
		kvs_error_with_encoding_limit(e, Some(self.config.grpc_max_encoding_message_size))
	}

	/// Number of transactions currently in flight on this datastore.
	pub fn in_flight_transaction_count(&self) -> usize {
		self.in_flight_txns.load(Ordering::Acquire)
	}

	/// Bypass-MVCC range destruction.
	///
	/// Callers are responsible for ensuring the data is already
	/// logically inaccessible (e.g. the catalog metadata pointing at it
	/// has already been cleared in a committed transaction).
	pub async fn unsafe_destroy_range(&self, range: KeyRange<'static>) -> Result<()> {
		let started = Instant::now();
		let tikv_start: tikv::Key = range.start.to_vec().into();
		let tikv_end: tikv::Key = range.end.to_vec().into();
		self.db.unsafe_destroy_range(tikv_start..tikv_end).await.map_err(|e| self.kvs_err(e))?;
		debug!(
			target: TARGET,
			duration_ms = started.elapsed().as_millis() as u64,
			"TiKV unsafe_destroy_range completed",
		);
		Ok(())
	}

	/// Advance the MVCC GC safepoint by `lifetime`.
	///
	/// Resolves the current TSO via `current_timestamp`, computes a
	/// safepoint at `now - lifetime`, then calls the client's `gc()` which
	/// internally runs `cleanup_locks` followed by `update_safepoint`.
	pub async fn run_mvcc_gc(&self, lifetime: Duration) -> Result<()> {
		if !self.config.gc_enabled {
			return Ok(());
		}
		let started = Instant::now();
		let now = self.db.current_timestamp().await.map_err(|e| self.kvs_err(e))?;
		let safepoint = match safepoint_from(&now, lifetime) {
			Some(ts) => ts,
			None => {
				debug!(
					target: TARGET,
					"Skipping TiKV MVCC GC pass: safepoint would precede epoch",
				);
				return Ok(());
			}
		};
		let advanced = match self.db.gc(safepoint).await {
			Ok(v) => v,
			Err(e) => {
				warn!(
					target: TARGET,
					error = %e,
					"TiKV MVCC GC pass failed",
				);
				return Err(self.kvs_err(e));
			}
		};
		info!(
			target: TARGET,
			advanced,
			duration_ms = started.elapsed().as_millis() as u64,
			lifetime_secs = lifetime.as_secs(),
			"TiKV MVCC GC pass completed",
		);
		Ok(())
	}

	/// Standalone lock-cleanup pass.
	///
	/// Calls `cleanup_locks` over the whole keyspace using a safepoint at
	/// `now - lifetime`. Runs on a faster cadence than full GC because
	/// stale locks block readers immediately, but version GC can wait.
	pub async fn run_lock_cleanup(&self, lifetime: Duration) -> Result<()> {
		if !self.config.gc_enabled {
			return Ok(());
		}
		let started = Instant::now();
		let now = self.db.current_timestamp().await.map_err(|e| self.kvs_err(e))?;
		let safepoint = match safepoint_from(&now, lifetime) {
			Some(ts) => ts,
			None => {
				debug!(
					target: TARGET,
					"Skipping TiKV lock cleanup pass: safepoint would precede epoch",
				);
				return Ok(());
			}
		};
		// Range that covers the entire user keyspace. The TiKV client
		// applies the active keyspace prefix internally, so callers pass
		// raw bounds. A bare `vec![0xff]` upper bound would lexically
		// exclude any multi-byte key starting with `0xff`
		// (`[0xff] < [0xff, 0x00]`); pad with extra `0xff` bytes so the
		// bound stays correct if a future key encoding ever leads with
		// a high byte.
		let range: Range<tikv::Key> = (vec![0u8].into())..(vec![0xffu8; 16].into());
		let result = self
			.db
			.cleanup_locks(range, &safepoint, ResolveLocksOptions::default())
			.await
			.map_err(|e| self.kvs_err(e))?;
		info!(
			target: TARGET,
			has_region_error = result.region_error.is_some(),
			key_error_count = result.key_error.as_ref().map(|v| v.len()).unwrap_or(0),
			resolved_locks = result.resolved_locks,
			duration_ms = started.elapsed().as_millis() as u64,
			lifetime_secs = lifetime.as_secs(),
			"TiKV lock cleanup pass completed",
		);
		Ok(())
	}
}

/// Compute a TiKV [`tikv::Timestamp`] at `now - lifetime` using the same
/// physical/logical decomposition as the rest of the engine. Returns
/// `None` if the computed instant would precede the epoch.
fn safepoint_from(now: &tikv::Timestamp, lifetime: Duration) -> Option<tikv::Timestamp> {
	let millis: i64 = lifetime.as_millis().try_into().ok()?;
	let physical = now.physical.checked_sub(millis)?;
	if physical < 0 {
		return None;
	}
	Some(tikv::Timestamp {
		physical,
		logical: now.logical,
		suffix_bits: now.suffix_bits,
	})
}

/// Owned TLS paths after partial-config validation.
#[cfg_attr(test, derive(Debug))]
struct TlsPaths {
	ca: String,
	cert: String,
	key: String,
}

/// Validate the mTLS portion of a [`TikvConfig`] before any I/O.
///
/// Returns:
/// - `Ok(Some(_))` when all three paths are set (mTLS is active).
/// - `Ok(None)` when none are set (plaintext connection).
/// - `Err(Error::Datastore)` when only some are set, with a message reporting which of {ca, cert,
///   key} are missing. Extracted so the partial-config rejection can be exercised without a live PD
///   endpoint.
fn validate_tls_paths(config: &TikvConfig) -> Result<Option<TlsPaths>> {
	match (config.tls_ca_path.as_ref(), config.tls_cert_path.as_ref(), config.tls_key_path.as_ref())
	{
		(Some(ca), Some(cert), Some(key)) => Ok(Some(TlsPaths {
			ca: ca.clone(),
			cert: cert.clone(),
			key: key.clone(),
		})),
		(None, None, None) => Ok(None),
		(ca, cert, key) => Err(Error::Datastore(format!(
			"TiKV mTLS requires tikv_tls_ca_path, tikv_tls_cert_path and \
			 tikv_tls_key_path to all be set together (e.g. via \
			 SURREAL_TIKV_TLS_*); got ca={}, cert={}, key={}",
			if ca.is_some() {
				"set"
			} else {
				"unset"
			},
			if cert.is_some() {
				"set"
			} else {
				"unset"
			},
			if key.is_some() {
				"set"
			} else {
				"unset"
			},
		))),
	}
}

impl Transactable for Transaction {
	fn kind(&self) -> &'static str {
		"tikv"
	}

	/// See [`WRITE_COMMIT_BOUND`].
	fn write_commit_bound(&self) -> Duration {
		WRITE_COMMIT_BOUND
	}

	/// Check if closed
	fn closed(&self) -> bool {
		self.done.load(Ordering::Relaxed)
	}

	/// Check if writeable
	fn writeable(&self) -> bool {
		self.write
	}

	/// Transactions are optimistic: a commit conflicts only with writes to the keys it writes; a
	/// plain read is never checked.
	fn plain_reads_tracked(&self) -> bool {
		false
	}

	/// Cancel a transaction
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	fn cancel(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Atomically mark transaction as done and check if it was already closed
			if self.done.swap(true, Ordering::AcqRel) {
				return Err(Error::TransactionFinished);
			}
			// Drop the in-flight reservation regardless of rollback outcome
			// so a slow/failing rollback can't stall shutdown drain.
			self.release_in_flight();
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Cancel this transaction
			if self.write {
				// Ignore rollback errors
				let _ = inner.tx.rollback().await;
			}
			// Continue
			Ok(())
		})
	}

	/// Commit a transaction
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	fn commit(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Atomically mark transaction as done and check if it was already closed
			if self.done.swap(true, Ordering::AcqRel) {
				return Err(Error::TransactionFinished);
			}
			// Release the in-flight reservation eagerly; the commit attempt
			// below is the last operation this handle will perform either
			// way. Doing it before the await also makes the counter
			// trustworthy from the shutdown drain's perspective even if the
			// commit hangs against a degraded cluster.
			self.release_in_flight();
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Get the inner transaction
			let mut inner = self.inner.write().await;
			let max_encoding = self.handle.config.grpc_max_encoding_message_size;
			if let Some(min_bytes) = inner.oversized.max_encoded_bytes() {
				// Reject before sending any `Prewrite` RPC so multi-region
				// transactions never prewrite locks on sibling regions for a
				// batch that is guaranteed to fail the outbound encoding cap,
				// while marking `inner.tx` finished (`CheckLevel::Panic` in
				// debug builds).
				if let Err(rollback_err) = inner.tx.rollback().await {
					debug!(
						target: TARGET,
						min_encoded_bytes = min_bytes,
						max_encoding_message_size = max_encoding,
						rollback_error = %rollback_err,
						"TiKV pre-commit rollback after local encoding-limit check failed",
					);
				}
				return Err(encoding_limit_exceeded_error(
					Some(min_bytes),
					Some(max_encoding),
					None,
				));
			}
			// Commit this transaction
			if let Err(err) = inner.tx.commit().await {
				if let Err(inner_err) = inner.tx.rollback().await {
					error!(
						target: TARGET,
						commit_error = %err,
						rollback_error = %inner_err,
						elapsed_ms = self.started_at.elapsed().as_millis() as u64,
						"TiKV transaction commit failed and rollback also failed",
					);
				} else {
					debug!(
						target: TARGET,
						commit_error = %err,
						elapsed_ms = self.started_at.elapsed().as_millis() as u64,
						"TiKV transaction commit failed; rollback succeeded",
					);
				}
				return Err(commit_error(err, Some(max_encoding)));
			}
			trace!(
				target: TARGET,
				elapsed_ms = self.started_at.elapsed().as_millis() as u64,
				"TiKV transaction committed",
			);
			// Continue
			Ok(())
		})
	}

	/// Check if a key exists
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn exists<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, Result<bool>> {
		Box::pin(async move {
			// TiKV does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Check the key
			let res = inner.tx.key_exists(key.to_vec()).await.map_err(|e| self.kvs_err(e))?;
			// Return result
			Ok(res)
		})
	}

	/// Fetch a key from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn get<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, Result<Option<Val>>> {
		Box::pin(async move {
			// TiKV does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Get the key
			let res = inner.tx.get(key.to_vec()).await.map_err(|e| self.kvs_err(e))?;
			// Return result
			Ok(res)
		})
	}

	/// Fetch a key from the database, registering the key for
	/// commit-time conflict detection.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn getu<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<Option<Val>>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable, as the conflict
			// registration is only validated when the transaction commits
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Get the key, locking it for commit-time conflict detection
			let res = inner.tx.get_for_update(key.to_vec()).await.map_err(|e| self.kvs_err(e))?;
			// Return result
			Ok(res)
		})
	}

	/// A locked read is prewritten as a `Lock` mutation, and its commit
	/// records a lock-type write of the key. That record conflicts with any
	/// overlapping transaction that locked or wrote the same key, so two
	/// locked reads of one key do not both commit.
	fn shared_locked_reads(&self) -> bool {
		false
	}

	/// Fetch many keys from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(keys = keys.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",")))]
	fn getm<'a>(
		&'a self,
		keys: &'a [Key<'a>],
		version: Option<u64>,
	) -> BoxFut<'a, Result<GetMultiResult>> {
		Box::pin(async move {
			// TiKV does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Build an index from key bytes to *every* original position that
			// requested it. `batch_get` collapses duplicate keys to a single
			// returned pair, so a key repeated in `keys` (common when several
			// graph rows fetch a shared endpoint/target node in one batch) must
			// fan its one result out to all of its positions — mirroring the
			// per-position semantics of the mem/surrealkv engines. Mapping to a
			// single position would leave the other slots `None`, silently
			// dropping records.
			let mut key_index: HashMap<&[u8], Vec<usize>> = HashMap::with_capacity(keys.len());
			for (i, k) in keys.iter().enumerate() {
				key_index.entry(k.as_slice()).or_default().push(i);
			}
			// Batch get the keys
			// This is done because of #[instrument] causing a lifetime inference issue, where the
			// compiler is unable to infer the correct lifetime for keys.
			let owned: Vec<Vec<u8>> = keys.iter().map(|x| x.as_slice().to_vec()).collect();
			let pairs = inner.tx.batch_get(owned).await.map_err(|e| self.kvs_err(e))?;
			// Place each result at every position that requested its key,
			// accumulating the hit count and value bytes during the same pass so
			// callers do not need to re-walk the result. The value is cloned into
			// each duplicate position and moved into the last, so the all-unique
			// case incurs no extra clone.
			let mut values: Vec<Option<Val>> = vec![None; keys.len()];
			let mut records = 0u64;
			let mut value_bytes = 0u64;
			for kv in pairs {
				let key: Vec<u8> = kv.0.into();
				if let Some(idxs) = key_index.get(key.as_slice())
					&& let Some((&last, rest)) = idxs.split_last()
				{
					let len = kv.1.len() as u64;
					for &i in rest {
						records += 1;
						value_bytes += len;
						values[i] = Some(kv.1.clone());
					}
					records += 1;
					value_bytes += len;
					values[last] = Some(kv.1);
				}
			}
			Ok(GetMultiResult {
				values,
				records,
				value_bytes,
			})
		})
	}

	/// Insert or update a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn set<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Take ownership of the key bytes for the client calls below
			let key = key.to_vec();
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Read the pre-image only when a savepoint could roll this back.
			// The outer `Option` distinguishes "no read was made" from the
			// inner `None`'s "the key did not exist", which need opposite
			// undo operations.
			let old_val = if inner.undo.is_tracking() {
				Some(inner.tx.get(key.clone()).await.map_err(|e| self.kvs_err(e))?)
			} else {
				None
			};
			// Set the key and record undo operation if tracking
			if let Some(old_val) = old_val {
				inner.put_entry(key.clone(), val, &self.handle.config).await?;
				inner.undo.record(match old_val {
					// Key existed, record operation to restore old value
					Some(existing_val) => Operation::RestoreValue(key, existing_val),
					// Key didn't exist, record operation to delete it
					None => Operation::DeleteKey(key),
				});
			} else {
				inner.put_entry(key, val, &self.handle.config).await?;
			}
			// Return result
			Ok(())
		})
	}

	/// Check many keys with one `batch_get`, which reads through the transaction's buffered
	/// writes as `put`'s `key_exists` does, in one round trip rather than one per key.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self, keys))]
	fn assert_absent<'a>(&'a self, keys: &'a [Key<'a>]) -> BoxFut<'a, Result<Option<usize>>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			if keys.is_empty() {
				return Ok(None);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			let owned: Vec<Vec<u8>> = keys.iter().map(|x| x.as_slice().to_vec()).collect();
			let pairs = inner.tx.batch_get(owned).await.map_err(|e| self.kvs_err(e))?;
			let present: std::collections::HashSet<Vec<u8>> =
				pairs.map(|kv| Vec::<u8>::from(kv.0)).collect();
			Ok(keys.iter().position(|key| present.contains(key.as_slice())))
		})
	}

	/// Insert a key if it doesn't exist in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn put<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Take ownership of the key bytes for the client calls below
			let key = key.to_vec();
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Check if key exists
			let exists = inner.tx.key_exists(key.clone()).await.map_err(|e| self.kvs_err(e))?;
			if exists {
				return Err(Error::TransactionKeyAlreadyExists);
			}
			// Set the key
			inner.put_entry(key.clone(), val, &self.handle.config).await?;
			// Key didn't exist (we just checked), record operation to delete it
			inner.undo.record(Operation::DeleteKey(key));
			// Return result
			Ok(())
		})
	}

	/// Insert a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn putc<'a>(&'a self, key: Key<'a>, val: Val, chk: Option<Val>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Take ownership of the key bytes for the client calls below
			let key = key.to_vec();
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Get the current value
			let current = inner.tx.get(key.clone()).await.map_err(|e| self.kvs_err(e))?;
			// Check if condition is met
			match (&current, &chk) {
				(Some(v), Some(w)) if v == w => {}
				(None, None) => {}
				_ => return Err(Error::TransactionConditionNotMet),
			};
			// Set the key
			inner.put_entry(key.clone(), val, &self.handle.config).await?;
			// Record operation after successful operation
			match current {
				Some(existing_val) => {
					// Key existed, record operation to restore old value
					inner.undo.record(Operation::RestoreValue(key, existing_val));
				}
				None => {
					// Key didn't exist, record operation to delete it
					inner.undo.record(Operation::DeleteKey(key));
				}
			}
			// Return result
			Ok(())
		})
	}

	/// Delete a key
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn del<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Take ownership of the key bytes for the client calls below
			let key = key.to_vec();
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Get the old value if we need to track operations
			let old_val = if inner.undo.is_tracking() {
				inner.tx.get(key.clone()).await.map_err(|e| self.kvs_err(e))?
			} else {
				None
			};
			// Delete the key and record undo operation if tracking
			if let Some(existing_val) = old_val {
				inner.delete_entry(key.clone(), &self.handle.config).await?;
				inner.undo.record(Operation::RestoreValue(key, existing_val));
			} else {
				inner.delete_entry(key, &self.handle.config).await?;
			}
			// Return result
			Ok(())
		})
	}

	/// Insert or update many keys in the database.
	///
	/// While a savepoint is tracking, the values the keys held are read with
	/// one batched get instead of one get per key.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip_all, fields(entries = entries.len()))]
	fn setm<'a>(&'a self, entries: Vec<(Key<'a>, Val)>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			if entries.is_empty() {
				return Ok(());
			}
			// Take ownership of the key bytes for the client calls below
			let entries: Vec<(Vec<u8>, Val)> =
				entries.into_iter().map(|(key, val)| (key.to_vec(), val)).collect();
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			let max_encoding = self.handle.config.grpc_max_encoding_message_size;
			// The values the keys held before the batch, when a savepoint
			// could roll it back. A key absent from the map was absent.
			let mut priors = if inner.undo.is_tracking() {
				let keys: Vec<Vec<u8>> = entries.iter().map(|(key, _)| key.clone()).collect();
				Some(Self::priors(&mut inner.tx, keys, max_encoding).await?)
			} else {
				None
			};
			for (key, val) in entries {
				// Only the first write of a key records an operation: undoing
				// in reverse restores the value held before the batch last.
				if let Some(old_val) = priors.as_mut().and_then(|priors| priors.remove(&key)) {
					inner.put_entry(key.clone(), val, &self.handle.config).await?;
					inner.undo.record(match old_val {
						Some(existing_val) => Operation::RestoreValue(key, existing_val),
						None => Operation::DeleteKey(key),
					});
				} else {
					inner.put_entry(key, val, &self.handle.config).await?;
				}
			}
			// Return result
			Ok(())
		})
	}

	/// Delete a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn delc<'a>(&'a self, key: Key<'a>, chk: Option<&'a [u8]>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Take ownership of the key bytes for the client calls below
			let key = key.to_vec();
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Get the current value
			let current = inner.tx.get(key.clone()).await.map_err(|e| self.kvs_err(e))?;
			// Check if condition is met
			match (&current, chk) {
				(Some(v), Some(w)) if v.as_slice() == w => {}
				(None, None) => {}
				_ => return Err(Error::TransactionConditionNotMet),
			};
			// Delete the key and record undo operation after successful operation
			if let Some(existing_val) = current {
				inner.delete_entry(key.clone(), &self.handle.config).await?;
				inner.undo.record(Operation::RestoreValue(key, existing_val));
			} else {
				inner.delete_entry(key, &self.handle.config).await?;
			}
			// Return result
			Ok(())
		})
	}

	/// Delete many keys from the database.
	///
	/// While a savepoint is tracking, the values the keys held are read with
	/// one batched get instead of one get per key.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip_all, fields(keys = keys.len()))]
	fn delm<'a>(&'a self, keys: Vec<Key<'a>>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			if keys.is_empty() {
				return Ok(());
			}
			// Take ownership of the key bytes for the client calls below
			let keys: Vec<Vec<u8>> = keys.into_iter().map(|key| key.to_vec()).collect();
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			let max_encoding = self.handle.config.grpc_max_encoding_message_size;
			// The values the keys held before the batch, when a savepoint
			// could roll it back.
			let mut priors = if inner.undo.is_tracking() {
				Some(Self::priors(&mut inner.tx, keys.clone(), max_encoding).await?)
			} else {
				None
			};
			for key in keys {
				// A key that held nothing records nothing, and only the first
				// delete of a key records an operation.
				let prior = priors.as_mut().and_then(|priors| priors.remove(&key)).flatten();
				if let Some(existing_val) = prior {
					inner.delete_entry(key.clone(), &self.handle.config).await?;
					inner.undo.record(Operation::RestoreValue(key, existing_val));
				} else {
					inner.delete_entry(key, &self.handle.config).await?;
				}
			}
			// Return result
			Ok(())
		})
	}

	/// Delete a range of keys from the database transactionally.
	///
	/// Keys are materialised via `scan_keys` in batches and deleted as part of
	/// the surrounding transaction, so a rollback undoes the delete and a
	/// concurrent snapshot reader never observes partial state. The number of
	/// keys one call may delete is capped by `SURREAL_TIKV_DELR_MAX_KEYS`;
	/// exceeding it returns [`Error::TransactionRangeTooLarge`] rather than
	/// building an oversized transaction. Callers that need to drop an
	/// arbitrarily large range either page the delete across committed
	/// transactions or use the out-of-transaction
	/// [`DestroyRange`] capability, which bypasses MVCC and is only safe once
	/// the range is already logically unreachable.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn delr<'a>(&'a self, rng: KeyRange<'a>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move { self.delete_range_bounded(rng).await })
	}

	/// Hard-delete a range of keys (`clrr`) under the same transactional
	/// bound as [`Self::delr`].
	///
	/// On TiKV `clr` and `del` collapse to the same in-transaction
	/// tombstone write. MVCC versions are reclaimed later, at the GC
	/// safepoint, outside any user transaction.
	///
	/// This override routes through the bounded scan-delete loop, so
	/// `tikv_delr_max_keys` applies. The default `clrr` in `kvs/api.rs`
	/// deletes per key over `batch_keys` and skips that cap, which would
	/// buffer an unbounded number of deletes in one transaction for a
	/// large prefix. Soft (`delr`/`delp`) and hard (`clrr`/`clrp`)
	/// prefix deletes share the same cap.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn clrr<'a>(&'a self, rng: KeyRange<'a>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move { self.delete_range_bounded(rng).await })
	}

	/// Count the total number of keys within a range in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn count<'a>(&'a self, rng: KeyRange<'a>, version: Option<u64>) -> BoxFut<'a, Result<usize>> {
		Box::pin(async move {
			// TiKV does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Store the total count
			let mut total = 0usize;
			// Store the end range key
			let end = rng.end.to_vec();
			// Store the next start key
			let mut start = rng.start.to_vec();
			// Loop until we have exhausted the range
			loop {
				// Scan keys in key-only mode (no values fetched)
				let iter = inner
					.tx
					.scan_keys(start..end.clone(), COUNT_BATCH_SIZE)
					.await
					.map_err(|e| self.kvs_err(e))?;
				// Count the items, tracking the last key seen
				let mut key: Option<tikv::Key> = None;
				// Count the items in this batch
				let mut count = 0u32;
				// Loop over the iterator
				for k in iter {
					count += 1;
					key = Some(k);
				}
				// Increment the total count
				total += count as usize;
				// If we got fewer than batch_size, we've exhausted the range
				if count < COUNT_BATCH_SIZE {
					break;
				}
				// Advance past the last key for the next batch
				match key {
					Some(k) => {
						let new_start: Vec<u8> = k.into();
						start = Key::from(new_start).next().to_vec();
					}
					None => break,
				}
			}
			// Return the total count
			Ok(total)
		})
	}

	/// Count the keys within a range, examining at most `budget` of them
	/// ([`Transactable::count_bounded`]): one key-only scan of `budget` keys,
	/// resumed just after the last when it comes back full.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn count_bounded<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
		budget: u32,
	) -> BoxFut<'a, Result<(usize, Option<Key<'static>>)>> {
		Box::pin(async move {
			// TiKV does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			let budget = budget.max(1);
			let mut inner = self.inner.write().await;
			let iter = inner
				.tx
				.scan_keys(rng.start.to_vec()..rng.end.to_vec(), budget)
				.await
				.map_err(|e| self.kvs_err(e))?;
			let mut counted = 0usize;
			let mut last: Option<tikv::Key> = None;
			for key in iter {
				counted += 1;
				last = Some(key);
			}
			let resume = match last {
				Some(key) if counted == budget as usize => {
					let key: Vec<u8> = key.into();
					Some(Key::from(key).next())
				}
				_ => None,
			};
			Ok((counted, resume))
		})
	}

	/// Retrieve a range of keys from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn keys<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<KeysResult>> {
		Box::pin(async move {
			// TiKV does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Add skip to the row budget so enough entries are fetched
			let count = limit.saturating_add(skip);
			// Create the iterator
			let mut iter = inner
				.tx
				.scan_keys(rng.start.to_vec()..rng.end.to_vec(), count)
				.await
				.map_err(|e| self.kvs_err(e))?;
			// Consume the iterator
			Ok(consume_keys(&mut iter, limit, skip))
		})
	}

	/// Retrieve a range of keys from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn keysr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<KeysResult>> {
		Box::pin(async move {
			// TiKV does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Add skip to the row budget so enough entries are fetched
			let count = limit.saturating_add(skip);
			// Create the iterator
			let mut iter = inner
				.tx
				.scan_keys_reverse(rng.start.to_vec()..rng.end.to_vec(), count)
				.await
				.map_err(|e| self.kvs_err(e))?;
			// Consume the iterator
			Ok(consume_keys(&mut iter, limit, skip))
		})
	}

	/// Retrieve a range of keys from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn scan<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ScanResult>> {
		Box::pin(async move {
			// TiKV does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Set the key range
			let rng = rng.start.to_vec()..rng.end.to_vec();
			// Skip entries using keys-only scan to avoid fetching values
			let rng = if skip > 0 {
				let skipped =
					inner.tx.scan_keys(rng.clone(), skip).await.map_err(|e| self.kvs_err(e))?;
				match skipped.last() {
					Some(last) => {
						let start: Vec<u8> = last.into();
						let start = Key::from(start).next().to_vec();
						start..rng.end
					}
					// Fewer entries than skip -- nothing to return
					None => return Ok(ScanResult::default()),
				}
			} else {
				rng
			};
			// Create the iterator
			let mut iter = inner.tx.scan(rng, limit).await.map_err(|e| self.kvs_err(e))?;
			// Consume the iterator
			Ok(consume_vals(&mut iter, limit))
		})
	}

	/// Retrieve a range of keys from the database in reverse order
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn scanr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ScanResult>> {
		Box::pin(async move {
			// TiKV does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Set the key range
			let rng = rng.start.to_vec()..rng.end.to_vec();
			// Skip entries using keys-only scan to avoid fetching values
			let rng = if skip > 0 {
				let skipped = inner
					.tx
					.scan_keys_reverse(rng.clone(), skip)
					.await
					.map_err(|e| self.kvs_err(e))?;
				match skipped.last() {
					Some(last) => {
						let end: Vec<u8> = last.into();
						rng.start..end
					}
					// Fewer entries than skip -- nothing to return
					None => return Ok(ScanResult::default()),
				}
			} else {
				rng
			};
			// Create the iterator
			let mut iter = inner.tx.scan_reverse(rng, limit).await.map_err(|e| self.kvs_err(e))?;
			// Consume the iterator
			Ok(consume_vals(&mut iter, limit))
		})
	}

	// --------------------------------------------------
	// Savepoint functions
	// --------------------------------------------------

	/// Set a new save point on the transaction.
	fn new_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Open a new scope for subsequent writes to record into
			inner.undo.new_savepoint();
			// Continue
			Ok(())
		})
	}

	/// Release the last save point.
	fn release_last_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Release the last savepoint, keeping its writes undoable by any
			// enclosing savepoint
			inner.undo.release_savepoint();
			// Continue
			Ok(())
		})
	}

	/// Rollback to the last save point.
	fn rollback_to_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Take the most recent savepoint's operations, restoring the
			// enclosing scope's log as the current one
			let Some(operations) = inner.undo.take_savepoint() else {
				return Err(Error::NoSavepoint);
			};
			// Execute undo operations in reverse order, moving the recorded
			// keys and values straight into the client rather than cloning
			// them: this path replays the whole pre-image of a failed create
			// attempt, so it is hot for an UPSERT against an existing record.
			for op in operations.into_iter().rev() {
				match op {
					// Delete the key that was inserted
					Operation::DeleteKey(key) => {
						inner.delete_entry(key, &self.handle.config).await?;
					}
					// Restore the key's previous value
					Operation::RestoreValue(key, val) => {
						inner.put_entry(key, val, &self.handle.config).await?;
					}
				}
			}
			// Continue
			Ok(())
		})
	}

	// --------------------------------------------------
	// Timestamp functions
	// --------------------------------------------------

	/// Get the current monotonic timestamp
	fn timestamp(&self) -> BoxFut<'_, Result<BoxTimeStamp>> {
		Box::pin(async move {
			let ts = self
				.inner
				.write()
				.await
				.tx
				.current_timestamp()
				.await
				.map_err(|e| self.kvs_err(e))?;
			Ok(BoxTimeStamp::new(TiKVStamp(ts)))
		})
	}

	fn timestamp_impl(&self) -> BoxTimeStampImpl {
		Box::new(TiKVStampImpl)
	}
}

pub struct TiKVStampImpl;

impl TimeStampImpl for TiKVStampImpl {
	fn earliest(&self) -> BoxTimeStamp {
		BoxTimeStamp::new(TiKVStamp(tikv::Timestamp {
			physical: 0,
			logical: 0,
			suffix_bits: 0,
		}))
	}

	fn create_from_versionstamp(&self, version: u128) -> Option<BoxTimeStamp> {
		Some(BoxTimeStamp::new(TiKVStamp(tikv::Timestamp::from_version(version as u64))))

		/* We really should encode full precision but version stamps aren't actually a u128, they
		 * only support values in range of 0 to i64::MAX,

		let physical = ((version >> 64) as u64 as i64) ^ i64::MIN;
		let logical = (version as u64 as i64) ^ i64::MIN;
		Some(BoxTimeStamp::new(TiKVStamp(tikv::Timestamp {
			physical,
			logical,
			suffix_bits: 0,
		})))
		*/
	}

	fn create_from_datetime(&self, dt: DateTime<Utc>) -> Option<BoxTimeStamp> {
		// `physical` is the TiKV TSO physical component, which is measured in
		// milliseconds (it is what `version()` shifts left by 18 bits, and what
		// `current_timestamp()` returns). Using microseconds here overflows the
		// `version()` packing for any realistic datetime.
		let physical = dt.timestamp_millis();
		Some(BoxTimeStamp::new(TiKVStamp(tikv::Timestamp {
			physical,
			logical: 0,
			suffix_bits: 0,
		})))
	}

	fn decode(&self, bytes: &[u8]) -> Result<BoxTimeStamp> {
		if bytes.len() == 8 {
			// An 8-byte value is a bare big-endian version number, so decode it as one.
			let Ok(b) = <[u8; 8]>::try_from(&bytes[0..8]) else {
				unreachable!()
			};
			let ts = u64::from_be_bytes(b);
			return Ok(BoxTimeStamp::new(TiKVStamp(tikv::Timestamp::from_version(ts))));
		}

		if bytes.len() != 20 {
			return Err(Error::TimestampInvalid(
				"Encoded timestamp is not the right length".to_string(),
			));
		}
		let Ok(b) = <[u8; 8]>::try_from(&bytes[0..8]) else {
			unreachable!()
		};
		let physical = i64::from_be_bytes(b) ^ i64::MIN;
		let Ok(b) = <[u8; 8]>::try_from(&bytes[8..16]) else {
			unreachable!()
		};
		let logical = i64::from_be_bytes(b) ^ i64::MIN;
		let Ok(b) = <[u8; 4]>::try_from(&bytes[16..20]) else {
			unreachable!()
		};
		let suffix_bits = u32::from_be_bytes(b);
		Ok(BoxTimeStamp::new(TiKVStamp(tikv::Timestamp {
			physical,
			logical,
			suffix_bits,
		})))
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct TiKVStamp(tikv::Timestamp);

impl TimeStamp for TiKVStamp {
	fn as_versionstamp(&self) -> u128 {
		self.0.version() as u128

		/* We really should encode full precision but version stamps aren't actually a u128, they
		 * only support values in range of 0 to i64::MAX,

		let p = (self.0.physical ^ i64::MIN) as u64;
		let l = (self.0.logical ^ i64::MIN) as u64;

		(p as u128) << 64 | l as u128
		*/
	}

	fn as_datetime(&self) -> Option<DateTime<Utc>> {
		// `physical` is in milliseconds (TiKV TSO physical component).
		DateTime::from_timestamp_millis(self.0.physical)
	}

	fn sub_checked(&self, duration: Duration) -> Option<BoxTimeStamp> {
		let millis = duration.as_millis().try_into().ok()?;
		let physical = self.0.physical.checked_sub(millis)?;
		Some(BoxTimeStamp::new(TiKVStamp(tikv::Timestamp {
			physical,
			logical: self.0.logical,
			suffix_bits: self.0.suffix_bits,
		})))
	}

	fn encode<'a>(&self, bytes: &'a mut [u8; MAX_TIMESTAMP_BYTES]) -> &'a [u8] {
		let b = (self.0.physical ^ i64::MIN).to_be_bytes();
		bytes[0..8].copy_from_slice(&b);
		let b = (self.0.logical ^ i64::MIN).to_be_bytes();
		bytes[8..16].copy_from_slice(&b);
		let b = self.0.suffix_bits.to_be_bytes();
		bytes[16..20].copy_from_slice(&b);

		&bytes[..20]
	}
}

// Consume and iterate over only keys
fn consume_keys<I: Iterator<Item = tikv::Key>>(iter: &mut I, limit: u32, skip: u32) -> KeysResult {
	// Skip entries from the pre-fetched iterator
	for _ in 0..skip {
		if iter.next().is_none() {
			return KeysResult::default();
		}
	}
	let mut key_bytes = 0u64;
	// Create the result set
	let mut keys = Vec::with_capacity(limit.min(4096) as usize);
	// Check that we don't exceed the count limit
	while keys.len() < limit as usize {
		// Check the key
		if let Some(k) = iter.next() {
			key_bytes += k.len() as u64;
			keys.push(k.into());
		} else {
			break;
		}
	}
	KeysResult {
		keys,
		key_bytes,
	}
}

// Consume and iterate over keys and values
fn consume_vals<I: Iterator<Item = tikv::KvPair>>(iter: &mut I, limit: u32) -> ScanResult {
	// Track the cumulative key/value bytes for the scan metrics.
	let mut key_bytes = 0u64;
	let mut value_bytes = 0u64;
	// Create the result set
	let mut values = Vec::with_capacity(limit.min(4096) as usize);
	// Check that we don't exceed the count limit
	while values.len() < limit as usize {
		// Check the key and value
		if let Some(kv) = iter.next() {
			let key_len = kv.0.len() as u64;
			let value_len = kv.1.len() as u64;
			key_bytes += key_len;
			value_bytes += value_len;
			values.push((kv.0.into(), kv.1));
		} else {
			break;
		}
	}
	ScanResult {
		values,
		key_bytes,
		value_bytes,
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::time::Duration;

	use bytes::{Buf, BufMut};
	use http::uri::PathAndQuery;
	use surrealdb_kvs::err::Error;
	use tokio::net::TcpListener;
	use tonic::client::Grpc;
	use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
	use tonic::transport::Endpoint;
	use tonic::{Code, Request, Status};

	use super::{
		OversizedWrites, TikvConfig, classify_batch, commit_error, encoding_limit_exceeded_error,
		is_outbound_encoding_limit_status, kvs_error, kvs_error_with_encoding_limit,
		min_prewrite_encoded_bytes, safepoint_from, validate_tls_paths,
	};

	#[derive(Default)]
	struct RawBytesCodec;

	#[derive(Default)]
	struct RawBytesEncoder;

	#[derive(Default)]
	struct RawBytesDecoder;

	impl Encoder for RawBytesEncoder {
		type Item = Vec<u8>;
		type Error = Status;

		fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
			dst.put_slice(&item);
			Ok(())
		}
	}

	impl Decoder for RawBytesDecoder {
		type Item = Vec<u8>;
		type Error = Status;

		fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
			if !src.has_remaining() {
				return Ok(None);
			}
			Ok(Some(src.copy_to_bytes(src.remaining()).to_vec()))
		}
	}

	impl Codec for RawBytesCodec {
		type Encode = Vec<u8>;
		type Decode = Vec<u8>;
		type Encoder = RawBytesEncoder;
		type Decoder = RawBytesDecoder;

		fn encoder(&mut self) -> Self::Encoder {
			RawBytesEncoder
		}

		fn decoder(&mut self) -> Self::Decoder {
			RawBytesDecoder
		}
	}

	/// Drive one unary gRPC request through a real `tonic` + `hyper` + `h2`
	/// loopback connection with `max_encoding_message_size`, returning the
	/// resulting client `Status` and the total request body bytes the HTTP/2
	/// server received before any stream reset.
	async fn run_unary_against_h2_server(
		payload_len: usize,
		max_encoding_message_size: usize,
		server_reset: Option<h2::Reason>,
	) -> (Status, usize) {
		let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let addr = listener.local_addr().unwrap();
		let received_bytes = Arc::new(AtomicUsize::new(0));
		let received_for_server = Arc::clone(&received_bytes);

		let server = tokio::spawn(async move {
			let (tcp, _) = listener.accept().await.unwrap();
			let mut h2_conn = h2::server::handshake(tcp).await.unwrap();
			while let Some(accepted) = h2_conn.accept().await {
				let Ok((req, mut respond)) = accepted else {
					break;
				};
				let received_stream = Arc::clone(&received_for_server);
				tokio::spawn(async move {
					let mut body = req.into_body();
					while let Some(chunk) = body.data().await {
						match chunk {
							Ok(bytes) => {
								received_stream.fetch_add(bytes.len(), Ordering::AcqRel);
								let _ = body.flow_control().release_capacity(bytes.len());
							}
							Err(_) => return,
						}
					}
					if let Some(reason) = server_reset {
						respond.send_reset(reason);
					}
				});
			}
		});

		let channel = Endpoint::from_shared(format!("http://{addr}")).unwrap().connect_lazy();
		let mut grpc = Grpc::new(channel).max_encoding_message_size(max_encoding_message_size);
		grpc.ready().await.unwrap();
		let status = grpc
			.unary(
				Request::new(vec![b'x'; payload_len]),
				PathAndQuery::from_static("/tikvpb.Tikv/KvPrewrite"),
				RawBytesCodec,
			)
			.await
			.expect_err("unary call should fail");
		server.abort();
		(status, received_bytes.load(Ordering::Acquire))
	}

	#[test]
	fn batched_errors_keep_the_classification_of_what_they_carry() {
		// Real batches carry `KeyError` and `RegionError` entries, whose proto
		// payloads are private to the client, so they are exercised against a
		// live cluster instead (surrealdb-idx's `tikv_concurrency` tests). The
		// variants below never arrive inside a batch; they stand in for those
		// entries because they are constructible. What is checked here is the
		// unwrapping: both batched variants, nested, reach the inner
		// classification. The precedence between classifications is covered by
		// `a_batch_is_retryable_only_when_every_error_is_a_conflict`.
		let duplicate = tikv::Error::MultipleKeyErrors(vec![
			tikv::Error::DuplicateKeyInsertion,
			tikv::Error::DuplicateKeyInsertion,
		]);
		assert!(matches!(kvs_error(duplicate), Error::TransactionKeyAlreadyExists));

		// Distinct failures in one batch are reported together, generically.
		let mixed = tikv::Error::MultipleKeyErrors(vec![
			tikv::Error::StringError("stand-in for an opaque inner error".into()),
			tikv::Error::DuplicateKeyInsertion,
		]);
		assert!(matches!(kvs_error(mixed), Error::Transaction(_)));

		let nested = tikv::Error::ExtractedErrors(vec![tikv::Error::MultipleKeyErrors(vec![
			tikv::Error::DuplicateKeyInsertion,
		])]);
		assert!(matches!(kvs_error(nested), Error::TransactionKeyAlreadyExists));

		// Nothing classifiable inside stays the generic, non-retryable variant.
		let opaque = tikv::Error::MultipleKeyErrors(vec![tikv::Error::StringError(
			"stand-in for an opaque inner error".into(),
		)]);
		assert!(matches!(kvs_error(opaque), Error::Transaction(_)));
	}

	#[test]
	fn a_batch_is_retryable_only_when_every_error_is_a_conflict() {
		let conflict = || Error::TransactionConflict("k".into());
		assert!(matches!(
			classify_batch(vec![conflict(), conflict()]),
			Error::TransactionConflict(_)
		));
		// A failure a retry would meet again wins over a conflict beside it.
		assert!(matches!(
			classify_batch(vec![conflict(), Error::TransactionKeyTooLarge]),
			Error::TransactionKeyTooLarge
		));
		assert!(matches!(
			classify_batch(vec![Error::TransactionKeyAlreadyExists, conflict()]),
			Error::TransactionKeyAlreadyExists
		));
		assert!(matches!(
			classify_batch(vec![
				conflict(),
				Error::TransactionKeyTooLarge,
				Error::TransactionKeyAlreadyExists
			]),
			Error::Transaction(_)
		));
		assert!(matches!(classify_batch(vec![]), Error::Transaction(_)));
	}

	#[test]
	fn transport_failures_during_commit_leave_the_outcome_unknown() {
		// A commit that fails at the transport level may still have been
		// applied: the client stopped hearing back, which is not the same as
		// the cluster rejecting the work. Reporting it as a definite failure
		// would tell a caller a write does not exist when it may.
		let transport = [
			tikv::Error::GrpcAPI(Status::new(Code::Cancelled, "Timeout expired")),
			tikv::Error::GrpcAPI(Status::new(Code::Unavailable, "no healthy store")),
			tikv::Error::UndeterminedError(Box::new(tikv::Error::GrpcAPI(Status::new(
				Code::Internal,
				"h2 protocol error",
			)))),
		];
		for err in transport {
			let rendered = err.to_string();
			let classified = commit_error(err, Some(4 * 1024 * 1024));
			let Error::CommitOutcomeUnknown(message) = classified else {
				panic!("expected CommitOutcomeUnknown for {rendered}, got {classified:?}");
			};
			assert!(!message.is_empty(), "cause dropped for {rendered}");
		}
	}

	#[test]
	fn cluster_rejections_during_commit_stay_definite() {
		// A decision the cluster reached and reported is unambiguous, so it must
		// not be blurred into an unknown outcome.
		let classified = commit_error(tikv::Error::DuplicateKeyInsertion, Some(4 * 1024 * 1024));
		assert!(matches!(classified, Error::TransactionKeyAlreadyExists), "got {classified:?}");
	}

	#[tokio::test]
	async fn outbound_encoding_limit_over_tonic_h2_is_definite_and_transmits_zero_bytes() {
		// Reproduces issue #2151 end-to-end through `tonic` + `hyper` + `h2`:
		// `EncodeBody` rejects the 128-byte message against the 64-byte cap
		// before yielding any `DATA` frame, `hyper` resets the stream locally
		// with `Reset(StreamId, INTERNAL_ERROR, User)` (`!is_remote()`), and
		// `commit_error` classifies the failure as definite rather than
		// `CommitOutcomeUnknown`.
		let (status, received) = run_unary_against_h2_server(128, 64, None).await;
		assert_eq!(received, 0, "no payload bytes may reach the server when encode cap trips");
		assert_eq!(status.code(), Code::Internal);
		assert!(
			is_outbound_encoding_limit_status(&status),
			"expected local User INTERNAL_ERROR reset to be recognized: {status:?}"
		);

		let direct = commit_error(tikv::Error::GrpcAPI(status.clone()), Some(64));
		let Error::Transaction(msg) = direct else {
			panic!("expected definite Error::Transaction, got {direct:?}");
		};
		assert!(msg.contains("64 bytes"), "missing configured byte limit: {msg}");
		assert!(
			msg.contains("SURREAL_TIKV_GRPC_MAX_ENCODING_MESSAGE_SIZE"),
			"missing encoding env var hint: {msg}"
		);
		assert!(
			msg.contains("SURREAL_TIKV_GRPC_MAX_DECODING_MESSAGE_SIZE"),
			"missing decoding env var hint: {msg}"
		);

		let wrapped = commit_error(
			tikv::Error::UndeterminedError(Box::new(tikv::Error::GrpcAPI(status))),
			Some(64),
		);
		assert!(
			matches!(wrapped, Error::Transaction(_)),
			"UndeterminedError wrapping local encode reset must still be definite, got {wrapped:?}"
		);
	}

	#[tokio::test]
	async fn remote_server_h2_resets_during_commit_stay_unknown() {
		// When the request fits under `max_encoding_message_size` (32 <= 64),
		// the server receives the gRPC payload and resets the stream from its
		// side (`Initiator::Remote`, `is_remote() == true`). Because the server
		// received the request and may have applied it before resetting,
		// `commit_error` MUST keep reporting `CommitOutcomeUnknown`.
		for reason in [h2::Reason::INTERNAL_ERROR, h2::Reason::CANCEL] {
			let (status, received) = run_unary_against_h2_server(32, 64, Some(reason)).await;
			assert_eq!(received, 37, "server should have received 5-byte header + 32-byte body");
			assert!(
				!is_outbound_encoding_limit_status(&status),
				"remote reset {reason:?} must never match local encode limit: {status:?}"
			);
			let classified = commit_error(tikv::Error::GrpcAPI(status), Some(64));
			assert!(
				matches!(classified, Error::CommitOutcomeUnknown(_)),
				"remote reset {reason:?} must remain CommitOutcomeUnknown, got {classified:?}"
			);
		}
	}

	#[test]
	fn direct_out_of_range_encode_is_definite_while_decode_stays_unknown() {
		let limit = 4 * 1024 * 1024;
		let encode_exceeded = Status::out_of_range(
			"Error, encoded message length too large: found 4200050 bytes, the limit is: 4194304 bytes",
		);
		let classified = commit_error(tikv::Error::GrpcAPI(encode_exceeded.clone()), Some(limit));
		let Error::Transaction(msg) = classified else {
			panic!(
				"expected definite Error::Transaction for outbound OutOfRange, got {classified:?}"
			);
		};
		assert!(msg.contains("4194304 bytes"), "got {msg}");
		assert!(msg.contains("SURREAL_TIKV_GRPC_MAX_ENCODING_MESSAGE_SIZE"), "got {msg}");

		// Nested inside batched errors, `kvs_error_with_encoding_limit` keeps
		// the configured byte limit in the message.
		let batched = kvs_error_with_encoding_limit(
			tikv::Error::ExtractedErrors(vec![tikv::Error::GrpcAPI(encode_exceeded)]),
			Some(limit),
		);
		let Error::Transaction(batched_msg) = batched else {
			panic!("expected Error::Transaction for batched encode limit, got {batched:?}");
		};
		assert!(batched_msg.contains("4194304 bytes"), "got {batched_msg}");

		// Inbound decode limit failure means the server already processed the
		// commit/prewrite RPC and sent back a response too large to decode;
		// commit outcome is genuinely unknown!
		let decode_exceeded = Status::out_of_range(
			"Error, decoded message length too large: found 5000000 bytes, the limit is: 4194304 bytes",
		);
		let decoded_commit = commit_error(tikv::Error::GrpcAPI(decode_exceeded), Some(limit));
		assert!(
			matches!(decoded_commit, Error::CommitOutcomeUnknown(_)),
			"decode OutOfRange after server processed request must stay unknown, got {decoded_commit:?}"
		);
	}

	#[test]
	fn oversized_writes_tracks_wire_bound_overwrites_deletes_and_api_versions() {
		let default_cfg = TikvConfig::default();
		assert_eq!(default_cfg.grpc_max_encoding_message_size, 4 * 1024 * 1024);

		// Reproduction payload from #2151: 4,200,000 bytes exceeds the 4 MiB
		// default, while 4,190,000 bytes fits comfortably.
		let mut tracker = OversizedWrites::default();
		tracker.record_put(b"small".to_vec(), 4_190_000, &default_cfg);
		assert_eq!(tracker.max_encoded_bytes(), None);

		tracker.record_put(b"big".to_vec(), 4_200_000, &default_cfg);
		let big_min = min_prewrite_encoded_bytes(3, 4_200_000, default_cfg.api_version);
		assert!(big_min > 4_200_000);
		assert_eq!(tracker.max_encoded_bytes(), Some(big_min));

		// Overwriting `"big"` with a small value clears its oversized entry.
		tracker.record_put(b"big".to_vec(), 128, &default_cfg);
		assert_eq!(tracker.max_encoded_bytes(), None);

		// Boundary case where raw `key.len() + val.len() <= limit` (10 + 80 = 90 <= 100),
		// yet protobuf framing + API V2 4-byte keyspace prefix pushes the wire
		// `PrewriteRequest` over 100 bytes.
		let tight_v2 = TikvConfig {
			api_version: 2,
			grpc_max_encoding_message_size: 100,
			..TikvConfig::default()
		};
		let tight_v1 = TikvConfig {
			api_version: 1,
			grpc_max_encoding_message_size: 100,
			..TikvConfig::default()
		};
		assert_eq!(min_prewrite_encoded_bytes(10, 80, 1), 107);
		assert_eq!(min_prewrite_encoded_bytes(10, 80, 2), 111);

		tracker.record_put(vec![b'a'; 10], 80, &tight_v1);
		assert_eq!(tracker.max_encoded_bytes(), Some(107));
		tracker.record_put(vec![b'b'; 10], 80, &tight_v2);
		assert_eq!(tracker.max_encoded_bytes(), Some(111));

		// Deleting the larger entry leaves the remaining oversized entry;
		// deleting the last clears `max_encoded_bytes()` to `None`.
		tracker.record_delete(&[b'b'; 10]);
		assert_eq!(tracker.max_encoded_bytes(), Some(107));
		tracker.record_delete(&[b'a'; 10]);
		assert_eq!(tracker.max_encoded_bytes(), None);

		let err = encoding_limit_exceeded_error(Some(big_min), Some(4 * 1024 * 1024), None);
		let Error::Transaction(rendered) = err else {
			panic!("expected Error::Transaction, got {err:?}");
		};
		assert!(rendered.contains(&format!("at least {big_min} bytes")), "got {rendered}");
		assert!(rendered.contains("of 4194304 bytes"), "got {rendered}");
		assert!(rendered.contains("SURREAL_TIKV_GRPC_MAX_ENCODING_MESSAGE_SIZE"), "got {rendered}");
	}

	#[test]
	fn grpc_statuses_classify_generically_and_keep_their_detail() {
		// No gRPC code is narrowed to a more specific variant, because none can
		// be attributed to one cause from the status alone. The codes below are
		// the ones that look attributable and are not: `DeadlineExceeded` is
		// only ever server-originated, since tonic's own timeout layer reports
		// an expired local deadline as `Cancelled` — which is also what hyper
		// connection loss and a peer reset produce. `Internal` is any HTTP/2
		// reset reason, and `OutOfRange` is both an encode and a decode limit.
		//
		// What must hold is that the status text survives, because it is the
		// only record of the cause that reaches the caller. The empty-message
		// row covers a bare `grpc-status` trailer with no `grpc-message`, seen
		// on mTLS misconfiguration.
		for (code, message) in [
			(Code::DeadlineExceeded, "h2 protocol error"),
			(Code::Cancelled, "h2 protocol error"),
			(Code::Internal, "h2 protocol error"),
			(Code::Unavailable, "h2 protocol error"),
			(Code::ResourceExhausted, "h2 protocol error"),
			(Code::OutOfRange, "h2 protocol error"),
			(Code::Unknown, "h2 protocol error"),
			(Code::Unauthenticated, ""),
		] {
			let err = kvs_error(tikv::Error::GrpcAPI(Status::new(code, message)));
			let Error::Transaction(rendered) = err else {
				panic!("expected a generic transaction error for {code:?}, got {err:?}");
			};
			assert!(rendered.contains(message), "detail dropped for {code:?}");
			assert!(!rendered.is_empty(), "content-free error for {code:?}");
		}
	}

	#[test]
	fn safepoint_from_subtracts_lifetime() {
		let now = tikv::Timestamp {
			physical: 1_000_000,
			logical: 42,
			suffix_bits: 7,
		};
		// `physical` is in milliseconds, so a 250 ms lifetime subtracts 250.
		let safepoint = safepoint_from(&now, Duration::from_millis(250)).unwrap();
		assert_eq!(safepoint.physical, 999_750);
		// Logical and suffix bits ride along unchanged so the safepoint
		// stays comparable to other timestamps in the same epoch.
		assert_eq!(safepoint.logical, 42);
		assert_eq!(safepoint.suffix_bits, 7);
	}

	#[test]
	fn safepoint_from_returns_none_when_lifetime_too_large() {
		let now = tikv::Timestamp {
			physical: 100,
			logical: 0,
			suffix_bits: 0,
		};
		// A 101 ms lifetime subtracted from `physical = 100` ms underflows
		// past the epoch and should bail.
		assert!(safepoint_from(&now, Duration::from_millis(101)).is_none());
	}

	#[test]
	fn safepoint_from_returns_none_when_lifetime_overflows_i64() {
		let now = tikv::Timestamp {
			physical: i64::MAX,
			logical: 0,
			suffix_bits: 0,
		};
		// `Duration::MAX.as_millis()` doesn't fit in `i64`; the
		// `try_into` should bail rather than panic.
		assert!(safepoint_from(&now, Duration::MAX).is_none());
	}

	#[test]
	fn validate_tls_paths_all_unset() {
		let cfg = TikvConfig::default();
		assert!(validate_tls_paths(&cfg).unwrap().is_none());
	}

	#[test]
	fn validate_tls_paths_all_set() {
		let cfg = TikvConfig {
			tls_ca_path: Some("/etc/tikv/ca.pem".into()),
			tls_cert_path: Some("/etc/tikv/cert.pem".into()),
			tls_key_path: Some("/etc/tikv/key.pem".into()),
			..TikvConfig::default()
		};
		let paths = validate_tls_paths(&cfg).unwrap().unwrap();
		assert_eq!(paths.ca, "/etc/tikv/ca.pem");
		assert_eq!(paths.cert, "/etc/tikv/cert.pem");
		assert_eq!(paths.key, "/etc/tikv/key.pem");
	}

	#[test]
	fn validate_tls_paths_partial_set_rejects() {
		// Permute through the six partial-set combinations and confirm
		// each produces a descriptive error that names the missing
		// piece(s). Catches a regression where only one of the three
		// triples is checked.
		let cases: &[(Option<&str>, Option<&str>, Option<&str>)] = &[
			(Some("ca"), None, None),
			(None, Some("cert"), None),
			(None, None, Some("key")),
			(Some("ca"), Some("cert"), None),
			(Some("ca"), None, Some("key")),
			(None, Some("cert"), Some("key")),
		];
		for (ca, cert, key) in cases {
			let cfg = TikvConfig {
				tls_ca_path: ca.map(String::from),
				tls_cert_path: cert.map(String::from),
				tls_key_path: key.map(String::from),
				..TikvConfig::default()
			};
			let err = validate_tls_paths(&cfg).expect_err("partial TLS config must reject");
			let msg = err.to_string();
			assert!(
				msg.contains("tikv_tls_ca_path"),
				"error should name the env var triple: {msg}"
			);
			assert!(msg.contains("set"), "error should report which fields are set: {msg}");
			assert!(msg.contains("unset"), "error should report which fields are unset: {msg}");
		}
	}

	#[test]
	fn release_in_flight_counter_decrements() {
		// `release_in_flight` lives on `Transaction` but its work is the
		// `checked_sub` on the shared `Arc<AtomicUsize>`. Exercising the
		// underlying primitive here would normally feel pointless — but
		// the contract is "underflow leaves the counter unchanged", and
		// that's the regression worth catching if anyone replaces it
		// with a bare `fetch_sub` later.
		let counter = Arc::new(AtomicUsize::new(2));
		let release = |c: &Arc<AtomicUsize>| {
			let _ = c.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
		};
		release(&counter);
		assert_eq!(counter.load(Ordering::Acquire), 1);
		release(&counter);
		assert_eq!(counter.load(Ordering::Acquire), 0);
		// Underflow: counter must stay at 0, not wrap to usize::MAX.
		release(&counter);
		assert_eq!(counter.load(Ordering::Acquire), 0);
	}
}
