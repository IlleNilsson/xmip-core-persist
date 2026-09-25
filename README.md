# xmip-core-persist

Durable runtime state behind one Xmip store contract: execution checkpoints,
Journey recovery, leases and deduplication — and, since 2026-09-25, the one
layer that encrypts everything Xmip stores of its own (ADR-0063 clause 2).

## What it is

- **The records** — `DurableJourneyState` (the whole Journey, so the chain
  limit survives a restart), `DurableExecutionCheckpoint`, `RecoveryLease`,
  `DeduplicationRecord`, keyed by the estate's identifiers.
- **`RuntimeStore`** — what the runtime writes and recovers them through.
  Every error is a `PersistError`.
- **`Engine`** — bytes under bytes: `read`, `write` (durable on return),
  `write_new` (one step, `false` when taken), `remove`. An engine sees only a
  keyed hash as the key and a sealed record as the value.
- **`EncryptedStore<E: Engine>`** — the encryption, above every engine, once.
  Each record is AES-256-GCM under a fresh nonce with its place — its kind and
  key — as associated data, so a record copied under another key is refused.
  The key it is found by is HMAC-SHA-256 of that place under a second key, so
  an engine's files hold neither what is stored nor the names it is stored
  under. Both keys are derived (HKDF-SHA-256) from one data key, created with
  the store, kept in it wrapped by the key home (`xmip-core-secret`) under a
  named key-encryption key. `EncryptedStore` is a `RuntimeStore`.
- **`fixture`**, behind the `test-support` feature — an engine in memory and
  `conformance`, the one set of checks every engine runs against itself:
  a record back through the encryption after a reopen, neither key nor kind
  nor value anywhere in the engine's files, a tampered record refused with
  its scope, the store refused under another key of the same name.

## A record that fails its tag

`EncryptedStore::get` answers `PersistError::Refused { scope, reason }` — a
failure, never a missing record. The caller audits it as a failure with that
scope and reason (ADR-0062, ADR-0063 Consequences); persist does not audit on
its own, because `xmip-core-audit`'s program-level API was not landed when
this was built (2026-09-25) and a store should not choose its caller's sink.

## The engines

Each a technology mounted beside `.src` (ADR-0049, ADR-0015 amendment
2026-09-25), each storing ciphertext only:

| engine | store | crate |
| --- | --- | --- |
| `rocksdb` | the runtime store: Messages, Journeys, checkpoints | `xmip-core-persist-rocksdb` |
| `sqlite` | the management store | `xmip-core-persist-sqlite` |

RocksDB's own encryption hook and SQLCipher are not used: each would be a
second way, for one engine (ADR-0063 clause 2). A device build leaves
`rocksdb` out and with it the C++ toolchain and libclang
(`prerequisite.toml`).

## What it is not

Not runtime orchestration, which is `xmip-core-runtime`'s. Not a range scan:
a keyed hash does not keep the order of what it hashes, so the chronological
scans `doc/record-identifier.md` describes for UUIDv7 keys are not available
through the lookup key and need an index of their own when they are wanted.
Not rotation: one data key per store until the key home designs it.

## Verification

`cargo clippy --all-targets -- -D warnings` and `cargo test`, here and in each
engine. The workflow is manual-only and calls the versioned shared workflow
at `IlleNilsson/.github@v1`.
