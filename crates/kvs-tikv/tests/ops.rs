//! Exercises the TiKV-specific operational surface the backend publishes to the engine: the
//! `Maintenance` and `DestroyRange` capabilities and the in-flight transaction count.
//!
//! These need a live cluster, so they are ignored by default; run them with
//! `SURREAL_TIKV_TEST_ENDPOINT=<pd endpoint> cargo test --test ops -- --ignored`.
#![expect(clippy::unwrap_used, reason = "a failed setup step should fail the test")]

use std::any::TypeId;
use std::time::Duration;

use surrealdb_cnf::ConfigMap;
use surrealdb_kvs::err::Error;
use surrealdb_kvs::{
	DestroyRangeHandle, Key, MaintenanceHandle, Transactable, TransactionBuilder, TransactionType,
};
use surrealdb_kvs_any::Backends;
use surrealdb_kvs_tikv::TikvProvider;
use tokio_util::sync::CancellationToken;

async fn builder(config: ConfigMap) -> Box<dyn TransactionBuilder> {
	let endpoint = std::env::var("SURREAL_TIKV_TEST_ENDPOINT")
		.expect("SURREAL_TIKV_TEST_ENDPOINT must name a PD endpoint");
	let mut backends = Backends::community();
	backends.register(TikvProvider);
	backends
		.new_transaction_builder(&format!("tikv:{endpoint}"), CancellationToken::new(), config)
		.await
		.unwrap()
}

async fn begin(builder: &dyn TransactionBuilder, ty: TransactionType) -> Box<dyn Transactable> {
	builder.new_transaction(ty).await.unwrap().0
}

#[tokio::test]
#[ignore = "needs a live TiKV cluster"]
async fn maintenance_runs_against_the_cluster() {
	let builder = builder(ConfigMap::empty()).await;
	let ext =
		builder.extension(TypeId::of::<MaintenanceHandle>()).expect("maintenance is published");
	let handle = ext.downcast::<MaintenanceHandle>().expect("a MaintenanceHandle");
	handle.0.collect_garbage(Duration::from_secs(600)).await.unwrap();
	handle.0.cleanup_locks(Duration::from_secs(600)).await.unwrap();
}

#[tokio::test]
#[ignore = "needs a live TiKV cluster"]
async fn in_flight_transactions_are_counted_until_they_finish() {
	let builder = builder(ConfigMap::empty()).await;
	let ext = builder.extension(TypeId::of::<MaintenanceHandle>()).unwrap();
	let handle = ext.downcast::<MaintenanceHandle>().ok().unwrap();
	let before = handle.0.in_flight_transactions().expect("TiKV tracks transactions");
	let txn = begin(builder.as_ref(), TransactionType::Read).await;
	assert_eq!(handle.0.in_flight_transactions(), Some(before + 1));
	txn.cancel().await.unwrap();
	assert_eq!(handle.0.in_flight_transactions(), Some(before));
}

#[tokio::test]
#[ignore = "needs a live TiKV cluster"]
async fn destroy_range_is_published_while_garbage_collection_runs() {
	let builder = builder(ConfigMap::empty()).await;
	assert!(builder.extension(TypeId::of::<DestroyRangeHandle>()).is_some());
}

#[tokio::test]
#[ignore = "needs a live TiKV cluster"]
async fn destroy_range_is_withheld_when_garbage_collection_is_off() {
	let config = ConfigMap::empty().with_key_value("tikv_gc_enabled", "false");
	let builder = builder(config).await;
	assert!(builder.extension(TypeId::of::<DestroyRangeHandle>()).is_none());
}

#[tokio::test]
#[ignore = "needs a live TiKV cluster"]
async fn maintenance_is_a_no_op_when_garbage_collection_is_off() {
	let config = ConfigMap::empty().with_key_value("tikv_gc_enabled", "false");
	let builder = builder(config).await;
	let ext = builder.extension(TypeId::of::<MaintenanceHandle>()).unwrap();
	let handle = ext.downcast::<MaintenanceHandle>().ok().unwrap();
	handle.0.collect_garbage(Duration::from_secs(600)).await.unwrap();
	handle.0.cleanup_locks(Duration::from_secs(600)).await.unwrap();
}

#[tokio::test]
#[ignore = "needs a live TiKV cluster"]
async fn single_write_over_grpc_encoding_limit_fails_definitely_without_applying() {
	let config = ConfigMap::empty().with_key_value("tikv_grpc_max_encoding_message_size", "65536");
	let builder = builder(config).await;
	let key: &[u8] = b"ops/encoding_limit/single";

	let txn = begin(builder.as_ref(), TransactionType::Write).await;
	txn.set(Key::from(key), vec![b'x'; 70_000]).await.unwrap();
	let err = txn.commit().await.expect_err("oversized write must fail commit");
	let Error::Transaction(msg) = err else {
		panic!("expected definite Error::Transaction, got {err:?}");
	};
	assert!(msg.contains("65536 bytes"), "missing byte cap: {msg}");
	assert!(msg.contains("SURREAL_TIKV_GRPC_MAX_ENCODING_MESSAGE_SIZE"), "missing env hint: {msg}");

	let read = begin(builder.as_ref(), TransactionType::Read).await;
	assert_eq!(read.get(Key::from(key), None).await.unwrap(), None);
	read.cancel().await.unwrap();
}

#[tokio::test]
#[ignore = "needs a live TiKV cluster"]
async fn combined_prewrite_batch_over_grpc_encoding_limit_fails_definitely_without_applying() {
	let config = ConfigMap::empty().with_key_value("tikv_grpc_max_encoding_message_size", "65536");
	let builder = builder(config).await;
	let key_a: &[u8] = b"ops/encoding_limit/batch_a";
	let key_b: &[u8] = b"ops/encoding_limit/batch_b";

	// Each 40 KiB write fits under the 64 KiB cap individually, so the
	// combined ~80 KiB `PrewriteRequest` reaches `tonic`'s `EncodeBody` check
	// and triggers the local HTTP/2 `INTERNAL_ERROR` reset path.
	let txn = begin(builder.as_ref(), TransactionType::Write).await;
	txn.set(Key::from(key_a), vec![b'a'; 40_000]).await.unwrap();
	txn.set(Key::from(key_b), vec![b'b'; 40_000]).await.unwrap();
	let err = txn.commit().await.expect_err("combined oversized prewrite batch must fail commit");
	let Error::Transaction(msg) = err else {
		panic!("expected definite Error::Transaction, got {err:?}");
	};
	assert!(msg.contains("65536 bytes"), "missing byte cap: {msg}");
	assert!(msg.contains("SURREAL_TIKV_GRPC_MAX_ENCODING_MESSAGE_SIZE"), "missing env hint: {msg}");

	let read = begin(builder.as_ref(), TransactionType::Read).await;
	assert_eq!(read.get(Key::from(key_a), None).await.unwrap(), None);
	assert_eq!(read.get(Key::from(key_b), None).await.unwrap(), None);
	read.cancel().await.unwrap();
}

#[tokio::test]
#[ignore = "needs a live TiKV cluster"]
async fn savepoint_rollback_or_delete_of_oversized_write_allows_commit() {
	let config = ConfigMap::empty().with_key_value("tikv_grpc_max_encoding_message_size", "65536");
	let builder = builder(config).await;
	let kept: &[u8] = b"ops/encoding_limit/kept";
	let reverted: &[u8] = b"ops/encoding_limit/reverted";
	let deleted: &[u8] = b"ops/encoding_limit/deleted";

	let txn = begin(builder.as_ref(), TransactionType::Write).await;
	txn.set(Key::from(kept), b"ok".to_vec()).await.unwrap();

	txn.new_save_point().await.unwrap();
	txn.set(Key::from(reverted), vec![b'r'; 70_000]).await.unwrap();
	txn.rollback_to_save_point().await.unwrap();

	txn.set(Key::from(deleted), vec![b'd'; 70_000]).await.unwrap();
	txn.del(Key::from(deleted)).await.unwrap();

	txn.commit().await.unwrap();

	let verify = begin(builder.as_ref(), TransactionType::Write).await;
	assert_eq!(verify.get(Key::from(kept), None).await.unwrap(), Some(b"ok".to_vec()));
	assert_eq!(verify.get(Key::from(reverted), None).await.unwrap(), None);
	assert_eq!(verify.get(Key::from(deleted), None).await.unwrap(), None);
	verify.del(Key::from(kept)).await.unwrap();
	verify.commit().await.unwrap();
}
