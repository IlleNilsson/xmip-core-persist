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
  `write_new` (one step, `false` when taken), `remove`, `apply` a batch whole,
  and `scan` a range of keys in their byte order or the reverse. An engine
  sees only a keyed hash — or an index entry's sortable key — as the key and
  a sealed record as the value.
- **`EncryptedStore<E: Engine>`** — the encryption, above every engine, once.
  Each record is AES-256-GCM under a fresh nonce with its place — its kind and
  key — as associated data, so a record copied under another key is refused.
  The key it is found by is HMAC-SHA-256 of that place under a second key, so
  an engine's files hold neither what is stored nor the names it is stored
  under. Both keys are derived (HKDF-SHA-256) from one data key, created with
  the store, kept in it wrapped by the key home (`xmip-core-secret`) under a
  named key-encryption key. `EncryptedStore` is a `RuntimeStore`. An index
  entry (`apply_indexed`, `scan_index`; proposed 2026-10-09) is the one key
  not hashed, so it sorts: the caller builds it from its values in the
  clear; its value is the record's identifier, sealed with the whole key as
  associated data.
- **`fixture`**, behind the `test-support` feature — an engine in memory and
  `conformance`, the one set of checks every engine runs against itself:
  a record back through the encryption after a reopen, index entries found
  by a value within a time, oldest or newest first, their values in the
  clear in the files, and neither key nor kind nor value anywhere in them,
  a tampered record refused with its scope, the store refused under another
  key of the same name.

## Xmip Storage

`storage` is Xmip Storage: the doorway to all storage, served by the nodes
declaring the Storage role and called by every other node, never a database
directly (`runtime-model.md` section 3, `deployment-model.md` sections 3, 7
and 9).

- **`XmipStorage`** — the operations, once: write and read a Stream chunk, a
  Message, a Journey; `publish` a receive's Publication — its Message, the
  Journeys it opened, the ones a paused Subscription holds and the rest in
  the queue of the Send Port each leads to, the claims its node takes on
  those it sends itself (`Publication::claims`, for `lease_nanos`), its
  entry in the node's Dead Message Queue where nothing matched
  (`DeadMessage`: receive context, gate verdicts, promoted properties, every
  Subscription's decline) and its audit record — as one atomic write, once
  by its Message, answering the claims its node holds of those it asked
  for; read
  a queue — a Subscription's, a Send Port's — oldest first (`read_held`);
  read a node's Dead Message Queue (`read_dead`, oldest first, a page at a
  time; `read_dead_message`, one entry) and `replay` an entry — the
  Journeys a routing against the Subscriptions of now opened, the ones held,
  its audit record and the entry taken out, as one write, done once: asked
  again after a lost answer it is `Replayed::Before` and writes nothing. Both
  kinds of queue are numbered as written and kept by one set of mechanics
  (`queue.rs`); claim a
  Journey (set the holder where there is none or
  the last claim lapsed, time-limited, on the Storage node's clock), renew
  it, release it; hand a step on — its result, the Messages it made, the
  Journeys that follow, the queues the Journey leaves (`HandOn::leaves`)
  and the places it takes (`HandOn::queued`), the queues it moves to the
  end of keeping what its place kept (`HandOn::requeued`, an Operator's
  Retry, built 2026-10-04), and the claim released, or
  kept to a due time where the step waits (`HandOn::kept_for_nanos`, a
  retry's backoff, holding no thread), as one atomic write; write an
  audit record to the runtime database; `keep_audit`, the audit keeper,
  moving each to the administration database exactly once, by its
  identifier; and the administration records — registration, membership,
  Modules, Handlers, deployment and operator state — keyed by UUIDv7. Every
  write returns once it is durable. Every operation is all or nothing: one
  that fails half-way writes nothing of itself. A request asked again after
  a lost answer does nothing twice: a Publication is kept as written by its
  Message, with the digest of what was asked, so asked again it writes
  nothing — no Journey another node moved on since is written back or
  queued again — and answers the claims of it still held under their
  tokens, while another Publication of that Message is refused (review of
  2026-10-06; the `publication` table on a database server); and a hand-on
  asked again is `true` while one after a mere release is `false` — the
  claim keeps which of the two ended it. And `query`: the identifiers of the
  records one index of one table finds (`Query`, `Ask`, `Span`), which the
  caller reads as it reads any record.
- **Laid-out columns** (proposed 2026-10-09; the owner, the same day: *Store
  it in the clear*, *All columns shall be laid out*) — the Journey, Message,
  held, Dead Message Queue, audit and administration tables keep every
  single value of their records in columns of their own, in the clear,
  beside the sealed body; a list — entries, Sections, context, properties,
  declines — stays in the body alone, never a table of its own. The writer
  says the values, typed, beside the body (`JourneyFacts`, `MessageFacts`,
  `AuditFacts`; the runtime fills them in one place from its objects); the
  times are Xmip Storage's, on its clock. A database server keeps them as
  columns and indexes; the embedded engines, which have no columns, keep
  each index as entries of its own, their values in the clear as the
  server's, written in the record's own batch, the entries of a record it
  replaces or removes taken out with it (`storage/columns.rs`). The other tables — chunks,
  queues' places, publication, replayed, claim and the runtime database's
  audit queue — keep what they kept.
- **`Embedded`** — the embedded Storage node: the runtime database and the
  administration database, each an `EncryptedStore` over the engine the
  program gives it — RocksDB and SQLite for a node, RocksDB on disk and
  SQLite in memory for a Storage node under test. Every runtime write goes
  through one writer, which takes every write waiting into one batch under
  one sync: group commit, and the one place a claim's condition is decided.
  Each operation is decided on a stage of its own over the batch, and the
  batch takes the stage only once the whole operation has been decided.
- **`StorageServer`** — a Storage node serving `XmipStorage` over Xmip's
  mutual TLS (`xmip-core-library-tls`, `Identity`), the protocol agreed as
  `xmip-storage/1` by ALPN: a length and a request, a length and an answer,
  each record in its one binary form; a thread per connection, synchronous.
- **`StorageClient`** — a node reaching the Storage nodes its `[storage]
  nodes` lists, round robin, moving to the next when one does not answer and
  passing over one that did not for its pass-over; each connect and read is
  bounded by its timeout, both given to `StorageClient::new` from the node's
  `[tuning]` (`storage_timeout`, `storage_pass_over`; five seconds each by
  default, `storage::client::TIMEOUT` and `PASS_OVER`). `storage::statement` binds
  it to one Storage node for a whole statement — a receive cycle's chunks,
  Publication and Journeys — which fails with that node rather than moving
  on (the owner, 2026-10-03). A node that is itself the
  Storage node calls its `Embedded` in process instead: both are
  `XmipStorage`, so nothing above knows which it has.
- **`database`** and **`schema`** — a database server Xmip Storage is in
  front of (option A): the one reading of a connection,
  `<postgresql|sqlserver>://<login>@<host>[:<port>]/<database>`, and every
  table both databases keep there, its searchable columns and its indexes
  (`schema/searchable.rs`, each index numbered once for the embedded
  engines), with the scripts IT runs for each server
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
`Engine::apply_deferred` writes one whole and unsynced, durable with the next
synced write: RocksDB's write-ahead log without a sync, and any other engine
as `apply`. It is how `XmipStorage::write_chunk` writes a Stream's chunks, so
a receive cycle costs one sync, its Publication's. A chunk is its Stream and its
number in it, nothing more: a Stream ends where it has no further chunk. A
Stream is a record of its own (`StreamRecord`, `write_stream` with its last
chunk, in the same unsynced write), written once and never changed: the one
home of its length and its chunks, which every Message referring to it refers
to by its identifier (the owner, 2026-10-09).

RocksDB's own encryption hook and SQLCipher are not used: each would be a
second way, for one engine (ADR-0063 clause 2). A device build leaves
`rocksdb` out and with it the C++ toolchain and libclang
(`prerequisite.toml`).

## What it is not

Not runtime orchestration, which is `xmip-core-runtime`'s. Not a range scan
by a record's key: a keyed hash does not keep the order of what it hashes, so
the chronological scans `doc/record-identifier.md` describes for UUIDv7 keys
are the searchable columns' indexes, by a time column, not the lookup key.
Not a pattern search: a name is found by equality alone. Not rotation: one
data key per store until the key home designs it.

## Verification

`cargo clippy --all-targets -- -D warnings` and `cargo test`, here and in each
engine. The workflow is manual-only and calls the versioned shared workflow
at `IlleNilsson/.github@v1`.
