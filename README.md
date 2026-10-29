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
  and `scan` a range of keys in their byte order or the reverse, and `view`:
  one consistent view (`Reads`) every read through which sees the records as
  they stood — a `RocksDB` snapshot, `SQLite`'s one connection held, the
  engine in memory its records held — so a queue's page reads its index
  entries and the records they find in one state, never short of what is
  held mid-write (`EncryptedStore::viewed`, 2026-10-27). An engine sees only
  a keyed hash — or an index entry's sortable key — as the key and a sealed
  record as the value.
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
  a queue — a Subscription's, a Send Port's — oldest first (`read_held`),
  each place marked where its Journey failed there (`Held::failed`); read a
  Send Port queue's index of failed Journeys (`read_failed`, a page at a
  time, in the order they failed, its count one read of its places,
  `FailedQueue`; the `failed_journey` and `failed_journey_places` tables;
  open problem 34, 2026-10-16); read a node's Dead Message Queue (`read_dead`, oldest first, a page at a
  time; `read_dead_message`, one entry) and `replay` an entry — the
  Journeys a routing against the Subscriptions of now opened, the ones held,
  its audit record and the entry taken out, as one write, done once: asked
  again after a lost answer it is `Replayed::Before` and writes nothing. Every
  kind of queue is numbered as written and kept by one set of mechanics
  (`queue.rs`), and read in its order by an index on its queue and place
  (`held_place`, `dead_message_place`, `failed_journey_place`; on a server,
  the table's primary key), written and taken out with each entry: a page
  is one ordered range from its place, a gap never read, and the queue's
  first place is found by the read that asks for it, never by the writer
  walking a gap (open problem 40; ADR-0013, amendment 2026-10-19; an
  embedded store made before it lacks the index, and nothing migrates it
  before the first release); read a Send Port's queue by when each is due
  (`read_due`, `DuePart`, `DueQueue`; `storage/due.rs`): what is due now,
  what waits for a retry, or what failed, each a part of the index
  `held_due` on queue, failed, due time and place, so a scan reads only
  what is due — with how many keep a retry's due time and when the
  earliest not yet due is, which the `held_due_places` table keeps beside
  where each part's first is; the one writer finds each first again once a
  batch is decided, by one range from where it was, so no read steps over
  what `RocksDB` keeps of entries taken out (ADR-0013, amendment
  2026-10-21); a Journey marked failed waits for no retry, its due time
  taken out with the mark; claim a
  Journey (set the holder where there is none or
  the last claim lapsed, time-limited, on the Storage node's clock), renew
  it, release it; hand a step on — its result, the Messages it made, the
  Journeys that follow, the queues the Journey leaves (`HandOn::leaves`)
  and the places it takes (`HandOn::queued`), the queues it moves to the
  end of keeping what its place kept (`HandOn::requeued`, an Operator's
  Retry, built 2026-10-04), the queue it waits in Failed and why, entered
  in that queue's index of failed Journeys (`HandOn::failed`), the queues
  whose index it leaves (`HandOn::cleared`, an Operator's Retry or
  Dismiss), the queue it waits in for a retry and how long, its due time
  kept at its place there on the Storage node's clock (`HandOn::waits`,
  `Held::due_unix_nanos`; a read answers with that clock beside the
  entries, `HeldQueue::read_unix_nanos`, and `HeldQueue::due_in` says how
  long one still waits, so no two clocks are compared; open problem 43),
  and the claim released, or kept for a holder that goes on at once
  (`HandOn::kept_for_nanos`, a held Journey moved to a Port its node
  sends), as one atomic write; write an
  audit record to the runtime database; `keep_audit`, the audit keeper,
  moving each to the audit database exactly once, by its
  identifier — a record of an act on a Message, which carries the Message
  in full (`Audited`), with the bytes of every Stream its Sections are over
  copied beside it a chunk at a time, a Stream two Sections share once, and
  each Stream's own record — its length, chunks, SHA-256 digest and when it
  was written — kept as a row of `audit_stream` in the record's own write
  (ADR-0070; `read_kept_audit_stream` reads the row, `read_kept_audit_chunk`
  the bytes, `ChunkReader::audited` holds each to both and refuses a copy
  that does not match, in words). The record's body — its record and the
  Message it carries — is kept as a Stream is, in chunks of the size the
  caller hands `keep_audit`, the runtime's Stream chunk, in
  `audit_body_chunk` beside its row (`KeptAudit`), its length, chunks and
  SHA-256 taken as they pass laid out in the row, read a chunk at a time by
  `ChunkReader::audit_body` and held to them (ADR-0070, amendment
  2026-10-10). And the keeper chains each record in its writer's audit chain
  as it keeps it, once by its identifier, in the order written (`chain`;
  ADR-0070 clause 5): its number there, the digest of the record before it
  and its own over its canonical form — the kept row but its own digest and
  the keeper's time, and its Streams' records (`chain_digest`) — the head of
  each writer's chain kept in `audit_chain_head` in the record's own write,
  so it goes on after a restart; `Ask::AuditChain` finds a writer's records in their order by the
  `audit_chain` index for the walk (`xmip-core-audit`'s `audit_chain::walk`,
  called by the runtime); and
  the administration records — registration, membership,
  Modules, Handlers, deployment and operator state — keyed by UUIDv7. And
  the secrets Locations present, in a table of their own, `secret`: set by
  `write_secret`, which keeps the identifier of the one it replaces, read
  with its value by `read_secret` for the node that presents it
  (`StoredSecrets`, the `xcore::Secrets` a node is handed), and listed
  without values by `list_secrets` (ADR-0019, amendment 2026-10-29). Every
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
  declines — stays in the body alone, never a table of its own. An audit
  record holds no reference but its Journey, Message and execution: its
  artifact spelled out — kind, name, version — and its node and cluster by
  name, as its origin's location says them (the owner, 2026-10-09: *In an
  Audit you can't have references, it should be spelled out*). Each Stream
  it carries is a row of `audit_stream` — the record, the Stream, its length,
  chunks, digest and when the Ledger wrote it, in the clear, found by the
  Stream (`Ask::AuditOfStream`) — and its bytes are in `audit_stream_chunk`
  (ADR-0070; the owner, the same day: *the whole shebang goes to audit*).
  The writer
  says the values, typed, beside the body (`JourneyFacts`, `MessageFacts`,
  `AuditFacts`; the runtime fills them in one place from its objects); the
  times are Xmip Storage's, on its clock. A database server keeps them as
  columns and indexes; the embedded engines, which have no columns, keep
  each index as entries of its own, their values in the clear as the
  server's, written in the record's own batch, the entries of a record it
  replaces or removes taken out with it (`storage/columns.rs`). The other tables — chunks,
  queues' places, publication, replayed, claim, the runtime database's
  audit queue and the kept audit records' body and Stream chunks — keep what
  they kept.
- **`Embedded`** — the embedded Storage node: the runtime database, the
  administration database and the audit database, each an `EncryptedStore`
  over the engine the program gives it — RocksDB for the runtime database
  and SQLite for the other two, each a store of its own, for a node;
  RocksDB on disk and SQLite in memory for a Storage node under test. The
  audit database is a data domain of its own (ADR-0070, amendment
  2026-10-10), so it may be on other storage than the other two. Every runtime write goes
  through one writer, which takes every write waiting into one batch under
  one sync: group commit, and the one place a claim's condition is decided.
  A batch is written, answered, then synced: every write but a Publication
  is waited on until durable, and a Publication is answered with its claims
  and its durability (`storage::Durable`), so its sends are prepared while
  the log syncs (ADR-0018, amendment 2026-10-20). The sync is a thread of
  its own (`xmip-storage-sync`), handed each batch once it is written, so
  the writer decides the next batch while the last syncs and one sync makes
  every batch written meanwhile durable together; until 2026-10-21 the
  writer synced each batch itself, and a Publication waited out the
  previous Message's hand-on sync before it was decided (ADR-0018,
  amendment 2026-10-21).
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
  on (the owner, 2026-10-03). A new connection first asks what its Storage
  node is (wire operation 29): a standby is passed over, and so is a primary
  of an epoch before the latest the client met; a standby asked anyway says
  it is one, and the next is asked. `StorageClient::reached` says what it
  last came to with each — what a node publishes at `<node>/storage`. A
  node that is itself the Storage node calls its `Embedded` in process
  instead: both are `XmipStorage`, so nothing above knows which it has.
- **`standby`** — embedded Storage nodes as a primary and its standbys
  (ADR-0056, amendment 2026-10-22, drafted). `StorageNode` is one of the
  Storage nodes a cluster lists, over its three engines: it asks every one
  listed what it is (`Standing`), follows a primary that answers, and where
  none does the standby holding most of the latest epoch is promoted, the
  first listed of equals. `Copied` numbers every write an engine takes, in
  the order taken, into `Copies`, the writes kept for a standby behind; a
  standby asks to follow (wire operation 30) and is handed them in order
  (`Shipped`), or each database whole first where it cannot take them on,
  with its data key handed inside the mutual TLS and wrapped under its own
  key-encryption key (ADR-0063, amendment 2026-10-22). `StorageServer::several`
  serves one; `StorageNode::served` refuses every operation while it is not
  the primary (`PersistError::Standby`), and `StorageNode::reaching` is the
  node's own Xmip Storage, its databases while primary and the primary over
  the wire while not. A stop serves nothing more, waits for every operation
  under way, and hands on everything written before the standbys'
  connections end.
- **`database`** and **`schema`** — what each data domain's database is
  kept on, `Technology` — `rocksdb`, `sqlite`, `postgresql`, `mssql` or
  `mariadb`, as each domain's own table in the configuration names it in
  `storage` (the owner, 2026-10-10: *i would do it like runtime, storage,
  connection string*) — and a database server Xmip Storage is in front of
  (option A): the one reading of a connection, in the server's own form
  (`host=… port=… dbname=… user=…` for PostgreSQL,
  `Server=…,<port>;Database=…;User Id=…` for SQL Server,
  `Server=…;Port=…;Database=…;User=…` for MariaDB), and every
  table the three databases — runtime, administration, audit — keep there,
  its searchable columns and its indexes (`schema/searchable.rs`, each
  index numbered once for the embedded engines), with the scripts IT runs
  for each server in its dialect (`schema/dialect.rs`), per data domain
  so each may be on a server of its own (`scripts`: `01-roles.sql`, then `02-<domain>-database.sql` and
  `03-<domain>-schema.sql`), which `deploy/database/<server>/` holds and
  the estate root's `cargo test --test database` holds to it. The backends themselves follow
  as technologies of this crate, `xmip-core-persist-postgresql` first.
- **`schema::version`** and **`schema::upgrade`** — the schema version
  each database holds (open problem 41; ADR-0015, amendment 2026-10-20).
  Every database records it in a table of its own, `schema_version`, a
  row for each version it has held with when; it holds the highest. Every
  table, column and index says the version that added it (`since`), so
  the schema at a version is what was added at it or before, and
  `upgrade(server, from, to)` writes `04-<domain>-upgrade-<from>-<to>.sql`
  for each database, the difference between two releases: tables made,
  columns added, indexes made, and each version reached recorded — never
  a drop or a rename, which the definition cannot say. `schema::VERSION`
  is this build's, `1` until the first release, which records its
  fingerprint in `RELEASED` so a released schema cannot change again. The
  embedded Storage node brings each database to `VERSION` as it opens
  (`Embedded::versions`) and refuses one that holds a newer version, in
  words naming both (`PersistError::Schema`).

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
| `sqlite` | the administration database and the audit database, each a store of its own; in memory for a Storage node under test | `xmip-core-persist-sqlite` |

`Engine::apply` writes a batch whole or not at all, durable on return: a
`WriteBatch` under one sync in RocksDB, a transaction in SQLite.
`Engine::apply_deferred` writes one whole and unsynced, durable with the next
synced write: RocksDB's write-ahead log without a sync — kept in RocksDB's
own buffer of the log until a sync writes it (`manual_wal_flush`), so it
never waits while another thread syncs the log, and goes with the process
until then — and SQLite's
write-ahead log committed under `synchronous = NORMAL`, synced with its next
`FULL` commit. `Engine::sync` makes everything written before it durable on
its own: RocksDB's `flush_wal`, a full checkpoint of SQLite's log. It is how
`XmipStorage::write_chunk` writes a Stream's chunks, so
a receive cycle costs one sync, its Publication's. A chunk is its Stream and its
number in it, nothing more: a Stream ends where it has no further chunk. A
Stream is a record of its own (`StreamRecord`, `write_stream` with its last
chunk, in the same unsynced write), written once and never changed: the one
home of its length, its chunks and the SHA-256 digest its writer took of its
bytes as they passed once (`StreamDigest`; ADR-0070), which every Message
referring to it refers to by its identifier (the owner, 2026-10-09). A Stream
is read back a chunk at a time by `ChunkReader`, held to its length: as a
reader, or a whole chunk as kept at a time (`ChunkReader::next_chunk`), and
over Xmip Storage it shares (`ChunkReader::shared`) a reader outlives the
call that made it — what the runtime lends one read of a Stream to every
departure of it through (open problem 38, 2026-10-19). A Ledger Stream's
read starts at a chunk where the reader took those before it
(`ChunkReader::shared_from`, the bytes it took given, so the length is
still held): what a departure fallen behind that one read asks for, the
chunks it missed and never one it took (2026-10-20).

RocksDB's own encryption hook and SQLCipher are not used: each would be a
second way, for one engine (ADR-0063 clause 2). A device build leaves
`rocksdb` out and with it the C++ toolchain and libclang
(`prerequisite.toml`).

## What it is not

Not runtime orchestration, which is `xmip-core-runtime`'s. Not a range scan
by a record's key: a keyed hash does not keep the order of what it hashes, so
the chronological scans `doc/record-identifier.md` describes for UUIDv7 keys
are the searchable columns' indexes, by a time column, not the lookup key,
and a queue's order is its index by place.
Not a pattern search: a name is found by equality alone. Not rotation: one
data key per store until the key home designs it.

## Verification

`cargo clippy --all-targets -- -D warnings` and `cargo test`, here and in each
engine. The workflow is manual-only and calls the versioned shared workflow
at `IlleNilsson/.github@v1`.
