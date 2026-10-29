# surrealdb-tikv

The TiKV storage backend for SurrealDB, kept outside the engine so it can be built from source by those who want it.

> [!WARNING]
> This crate tracks the internal storage APIs of the SurrealDB engine, which do not adhere to SemVer. It depends on the engine's `main` branch, and `Cargo.lock` records the commit a build resolved to.

## Layout

- `crates/kvs-tikv` is the backend (`surrealdb-kvs-tikv`). It implements the engine's `TransactionBuilder`, `Transactable`, `Maintenance` (MVCC garbage collection, lock cleanup) and `DestroyRange` capabilities over the TiKV client, and exports `TikvProvider`.

## Using the backend

`TikvProvider` claims the `tikv:` scheme through the engine's `BackendProvider` registry. Register it on top of the community set wherever a binary builds its backends:

```rust
let mut backends = surrealdb_kvs_any::Backends::community();
backends.register(surrealdb_kvs_tikv::TikvProvider);
```

Configuration comes from the `SURREAL_TIKV_*` environment variables and `?key=value` parameters on the connection path, exactly as before the backend left the engine.
