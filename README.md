# xmip-core-persist

Durable runtime state behind one Xmip store contract: execution checkpoints,
Journey recovery, leases and deduplication — and, since 2026-09-25, the one
layer that encrypts everything Xmip stores of its own (ADR-0063 clause 2).

## What it is

- **The records** — `DurableJourneyState` (the whole Journey, so the chain
  limit survives a restart), `DurableExecutionCheckpoint`, `RecoveryLease`,
  `DeduplicationRecord`, keyed by the estate's identifiers. A moment in a
  record — when a wait gives up, when a lease lapses — is nanoseconds since
  the Unix epoch, the unit of `xcore::Clock`, never text.
- **What a paused Subscription leaves** — `SubscriptionHold`, a
  Subscription's standing on a node (paused or not, by whom, since when,
  and the range of what it holds), and `HeldMessage`, each
  Message it held while paused, in the order held. A keyed store keeps no
  order, so the range is the index. A held Message is released once its
  Subscription picks it up; the Message has gone on its way (ADR-0013,
  amendment 2026-09-30).
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

## Xmip Storage

`storage` is Xmip Storage: the doorway to all storage, served by the nodes
declaring the Storage role and called by every other node, never a database
directly (`runtime-model.md` section 3, `deployment-model.md` sections 3, 7
and 9).

- **`XmipStorage`** — the operations, once: write and read a Stream chunk, a
  Message, a Journey; claim a Journey (set the holder where there is none or
  the last claim lapsed, time-limited, on the Storage node's clock), renew
  it, release it; hand a step on — its result, the Messages it made, the
  Journeys that follow and the claim released, as one atomic write; write an
  audit record to the runtime database; `keep_audit`, the audit keeper,
  moving each to the administration database exactly once, by its
  identifier; and the administration records — registration, membership,
  Modules, Handlers, deployment and operator state — keyed by UUIDv7. Every
  write returns once it is durable. A request asked again after a lost
  answer does nothing twice.
- **`Embedded`** — the embedded Storage node: the runtime database and the
  administration database, each an `EncryptedStore` over the engine the
  program gives it — RocksDB and SQLite for a node, RocksDB on disk and
  SQLite in memory for a Storage node under test. Every runtime write goes
  through one writer, which takes every write waiting into one batch under
  one sync: group commit, and the one place a claim's condition is decided.
- **`StorageServer`** — a Storage node serving `XmipStorage` over Xmip's
  mutual TLS (`xmip-core-library-tls`, `Identity`), the protocol agreed as
  `xmip-storage/1` by ALPN: a length and a request, a length and an answer,
  each record in its one binary form; a thread per connection, synchronous.
- **`StorageClient`** — a node reaching the Storage nodes its `[storage]
  nodes` lists, round robin, moving to the next when one does not answer and
  passing over one that did not for five seconds. A node that is itself the
  Storage node calls its `Embedded` in process instead: both are
  `XmipStorage`, so nothing above knows which it has.
- **`database`** and **`schema`** — a database server Xmip Storage is in
  front of (option A): the one reading of a connection,
  `<postgresql|sqlserver>://<login>@<host>[:<port>]/<database>`, and every
  table both databases keep there, with the scripts IT runs for each server
  (`scripts`), which `deploy/database/<server>/` holds and the estate root's
  `cargo test --test database` holds to it. The backends themselves follow
  as technologies of this crate, `xmip-core-persist-postgresql` first.

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
| `rocksdb` | the runtime database: Messages, Journeys, claims, checkpoints | `xmip-core-persist-rocksdb` |
| `sqlite` | the administration database; in memory for a Storage node under test | `xmip-core-persist-sqlite` |

`Engine::apply` writes a batch whole or not at all, durable on return: a
`WriteBatch` under one sync in RocksDB, a transaction in SQLite.

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
