# surrealdb-tikv

The [TiKV](https://tikv.org) storage backend for [SurrealDB](https://surrealdb.com), and a server binary (`surreal-tikv`) that is the SurrealDB server with that backend built in.

TiKV is a distributed, transactional key-value store. With this backend, SurrealDB keeps all of its data in a TiKV cluster, so the SurrealDB nodes hold no data themselves and several of them can run against the same cluster.

## Why this is a separate repository

Earlier versions compiled TiKV support into every SurrealDB build. It now lives here, outside the engine, for these reasons:

- **Customers choose what they run.** Some customers do not want to run software with TiKV built into it. The default SurrealDB binary no longer contains the TiKV client or its dependency tree. Anyone who wants TiKV builds it from this repository.
- **The TiKV ecosystem has slowed down.** The Rust TiKV client is effectively unmaintained, with its last upstream update about a year ago. TiKV itself is in maintenance mode, while PingCAP concentrates on TiDB X, which has a different overall design. The engine's release cadence should not depend on either.
- **The client is not declared production-ready.** The README of the upstream [`tikv-client`](https://github.com/tikv/client-rust) crate says that the release is not suitable for production use, because its APIs are not yet stable and it has not been thoroughly tested in real-life use. This backend is built on our [fork of that client](https://github.com/surrealdb/tikv-client), published as `surrealdb-tikv-client`, because upstream lacks features SurrealDB needs and is no longer developed. We do not want to maintain a fork, so the backend stays tied to a client that has no active upstream.
- **It is hard to optimise for.** TiKV's design uses always-on storage nodes with spinning storage and permanent disk usage. Getting the performance and cost profile SurrealDB wants from that design is difficult, which is one of the reasons we built SurrealDS.
- **SurrealDS is the direction for distributed storage.** SurrealDS is built on object storage and scales to zero, which a TiKV cluster cannot do.
- **It is a supply-chain boundary.** The recent security incident in the TiDB ecosystem is a reminder that a large optional dependency tree should not be part of every build by default.

The engine reaches the backend through two seams, so nothing in the engine names TiKV any more:

- `BackendProvider` and the `Backends` registry in `surrealdb-kvs-any`. They map a connection-path scheme, here `tikv:`, to a storage backend.
- The `Maintenance` and `DestroyRange` capabilities in `surrealdb-kvs`. They let a backend publish MVCC garbage collection, lock cleanup and fast range removal to the engine without the engine naming the backend.

## What is in this repository

| Path | What it is |
| --- | --- |
| `crates/kvs-tikv` | The backend (`surrealdb-kvs-tikv`). It implements the engine's `TransactionBuilder`, `Transactable`, `Maintenance` and `DestroyRange` over the TiKV client, and exports `TikvProvider`. |
| `crates/server` | The `surreal-tikv` binary. It registers `TikvProvider` on top of the engine's community backends and otherwise behaves like the `surreal` binary. |

## Building

You need a Rust toolchain. `rustup` installs the toolchain version in `rust-toolchain.toml` for you. The engine crates are fetched from crates.io.

```bash
git clone https://github.com/surrealdb/surrealdb-tikv.git
cd surrealdb-tikv
cargo build --release --locked -p surrealdb-tikv
```

The binary is `target/release/surreal-tikv`. Use `--locked` so the build uses the engine crate versions recorded in `Cargo.lock`.

The default features match the community `surreal` binary. Its storage backends are in-memory, SurrealKV and TiKV. Extensions (Surrealism) are not part of this binary yet.

### Using the backend in your own binary

`TikvProvider` claims the `tikv:` scheme through the engine's `BackendProvider` registry. Register it on top of the community set wherever your binary builds its backends:

```rust
let mut backends = surrealdb_kvs_any::Backends::community();
backends.register(surrealdb_kvs_tikv::TikvProvider);
```

`crates/server/src/main.rs` is a complete example of a composer that does this.

## Running TiKV

SurrealDB needs a running TiKV cluster, which is made of Placement Driver (PD) nodes and TiKV nodes. Operating TiKV is not specific to SurrealDB, so use the TiKV documentation for it:

- The [TiKV documentation](https://tikv.org/docs/dev/) covers concepts, deployment, configuration, TLS and operations.
- For local development, the TiKV documentation describes how to start a small local cluster with TiUP, which runs one PD node and one TiKV node.
- For production, follow the TiKV deployment guidance for your platform, and run at least three PD nodes and three TiKV nodes.

SurrealDB connects to a PD endpoint, not to the TiKV nodes directly. The client discovers the rest of the cluster from that endpoint.

## Running SurrealDB on TiKV

Start the server and give it a PD endpoint as the datastore path:

```bash
surreal-tikv start --user root --pass root tikv://127.0.0.1:2379
```

Every `surreal-tikv` process pointed at the same cluster serves the same data. Every other command and flag is the same as for `surreal`. See the [SurrealDB documentation](https://surrealdb.com/docs).

### Configuration

The backend reads `SURREAL_TIKV_*` environment variables. The suffix is the upper-cased form of the key, so the `tikv_request_timeout` setting is `SURREAL_TIKV_REQUEST_TIMEOUT`.

| Variable | Default | Meaning |
| --- | --- | --- |
| `SURREAL_TIKV_API_VERSION` | `1` | TiKV cluster API version. Set to `2` to use keyspaces. |
| `SURREAL_TIKV_KEYSPACE` | none | Keyspace for data isolation under API version 2. |
| `SURREAL_TIKV_REQUEST_TIMEOUT` | `10` | Timeout in seconds for every request to PD and TiKV. |
| `SURREAL_TIKV_ASYNC_COMMIT` | `true` | Use asynchronous transaction commit. |
| `SURREAL_TIKV_ONE_PHASE_COMMIT` | `true` | Try one-phase commit when a transaction writes to a single region. |
| `SURREAL_TIKV_GRPC_MAX_DECODING_MESSAGE_SIZE` | 4 MiB | Largest response message the client accepts, in bytes. |
| `SURREAL_TIKV_GRPC_MAX_ENCODING_MESSAGE_SIZE` | 4 MiB | Largest request message the client sends, in bytes. |
| `SURREAL_TIKV_DELR_MAX_KEYS` | `1000000` | Most keys one transactional range delete may touch before it fails. |
| `SURREAL_TIKV_TLS_CA_PATH`, `SURREAL_TIKV_TLS_CERT_PATH`, `SURREAL_TIKV_TLS_KEY_PATH` | none | PEM files for mutual TLS. Set all three together. |
| `SURREAL_TIKV_GC_ENABLED` | `true` | Run MVCC garbage collection and lock cleanup. |
| `SURREAL_TIKV_GC_LIFETIME` | `600` | Seconds the garbage-collection pass at shutdown stays behind the current time. |
| `SURREAL_TIKV_HEALTH_PROBE` | `true` | Check the cluster when connecting, so a bad endpoint fails at startup. |
| `SURREAL_TIKV_SHUTDOWN_GRACE` | `30` | Seconds shutdown waits for open transactions before it continues. |
| `SURREAL_TIKV_SHUTDOWN_GC_TIMEOUT` | `10` | Seconds the final garbage-collection pass at shutdown may take. |

The server schedules garbage collection and lock cleanup with `--tikv-gc-interval`, `--tikv-gc-lifetime` and `--tikv-lock-cleanup-interval`, or the matching `SURREAL_TIKV_GC_INTERVAL`, `SURREAL_TIKV_GC_LIFETIME` and `SURREAL_TIKV_LOCK_CLEANUP_INTERVAL` variables.

## Moving from a build with TiKV built in

Data in a TiKV cluster does not change. Build `surreal-tikv` from this repository, use the same `tikv://` path and the same `SURREAL_TIKV_*` settings, and it opens the same data. Once the engine release that removes TiKV ships, the standard `surreal` binary and container images reject `tikv://` and report that the scheme is not enabled in the build.

## Contributing

Run `cargo fmt`, `cargo clippy --locked --tests` and `cargo test --locked` before you open a pull request. Changes to the engine's storage traits are made in the engine repository first, and this repository follows them.
