//! The [`BackendProvider`] that claims the `tikv:` scheme.

use surrealdb_kvs::TransactionBuilder;
use surrealdb_kvs::api::BoxFut;
use surrealdb_kvs::err::Result;
use surrealdb_kvs_any::{BackendProvider, ConnectContext};
use tracing::info;

use crate::Datastore;

const TARGET: &str = "surrealdb::core::kvs::ds";

/// Provider for the TiKV storage engine.
///
/// Register it on top of the community set to make `tikv://<pd endpoints>`
/// constructible and valid as a datastore path:
///
/// ```ignore
/// let mut backends = Backends::community();
/// backends.register(TikvProvider);
/// ```
pub struct TikvProvider;

impl BackendProvider for TikvProvider {
	fn schemes(&self) -> &[&'static str] {
		&["tikv"]
	}

	fn connect<'a>(
		&'a self,
		ctx: ConnectContext<'a>,
	) -> BoxFut<'a, Result<Box<dyn TransactionBuilder>>> {
		Box::pin(async move {
			// Parse TiKV-specific configuration from env vars
			// (SURREAL_TIKV_*) and query parameters.
			let tikv_config = ctx.config.load();
			let v = Datastore::new(ctx.path, tikv_config).await?;
			info!(target: TARGET, "Started tikv kvs store");
			Ok(Box::new(v) as Box<dyn TransactionBuilder>)
		})
	}
}
