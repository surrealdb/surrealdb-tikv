//! The SurrealDB server with the TiKV storage backend registered on top of the engine's
//! community backends.
#![recursion_limit = "256"]

use std::process::ExitCode;

use surrealdb_cnf::ConfigMap;
use surrealdb_kvs_any::Backends;
use surrealdb_kvs_tikv::TikvProvider;
use surrealdb_server::core::CommunityComposer;
use surrealdb_server::core::kvs::{TransactionBuilderFactory, TransactionBuilderParts};
use surrealdb_server::ntw::RouterFactory;
use surrealdb_server::observe::ObservabilityProvider;
use surrealdb_server::{Config, ConfigCheck, init};
use tokio_util::sync::CancellationToken;

/// The community composer with `tikv:` added to the accepted storage schemes.
///
/// Everything but the storage backends is the community edition's, so each other trait delegates to
/// it.
#[derive(Default)]
struct TikvComposer {
	community: CommunityComposer,
}

impl TikvComposer {
	/// The community backends plus TiKV. Construction and path validation both dispatch through
	/// this registry, so the accepted schemes cannot drift from the dispatch logic.
	fn backends() -> Backends<'static> {
		let mut backends = Backends::community();
		backends.register(TikvProvider);
		backends
	}
}

impl TransactionBuilderFactory for TikvComposer {
	type RouterState = ();

	async fn new_transaction_builder(
		&self,
		path: &str,
		canceller: CancellationToken,
		config: ConfigMap,
	) -> anyhow::Result<TransactionBuilderParts<Self::RouterState>> {
		let builder = Self::backends().new_transaction_builder(path, canceller, config).await?;
		Ok(TransactionBuilderParts::without_router_state(builder))
	}

	fn path_valid(&self, path: &str) -> anyhow::Result<String> {
		Ok(Self::backends().path_valid(path)?)
	}
}

impl RouterFactory for TikvComposer {
	fn configure_router(
		router_state: Self::RouterState,
	) -> axum::Router<std::sync::Arc<surrealdb_server::RpcState>> {
		CommunityComposer::configure_router(router_state)
	}
}

#[async_trait::async_trait]
impl ConfigCheck for TikvComposer {
	async fn check_config(&mut self, cfg: &Config) -> anyhow::Result<()> {
		self.community.check_config(cfg).await
	}
}

impl surrealdb_observe::ObservabilityProvider for TikvComposer {
	fn create_observer(&self) -> std::sync::Arc<dyn surrealdb_observe::ExecutionObserver> {
		self.community.create_observer()
	}
}

impl ObservabilityProvider for TikvComposer {}

fn main() -> ExitCode {
	init(TikvComposer::default())
}
