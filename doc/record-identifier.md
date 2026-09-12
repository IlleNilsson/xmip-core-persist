# Record identifiers

Moved here from the estate root on 2026-09-12 (ADR-0020 clause 3: the document lives where its subject lives).
The paragraph that named the generation sites of 2026-08-26 is gone with the
crates it named (`doc/planning/allocation.toml`); every store keys by `Uuid::now_v7()`.


**Every record in either database is keyed by a UUIDv7**, per RFC 9562.

RFC 9562 replaced RFC 4122 in May 2024 and defines versions 6, 7 and 8. The
highest number is not the answer: **v8 is deliberately vendor-defined and
experimental**, and v6 exists to reorder v1 for legacy migration. v7 is the one
designed for this job — 48 bits of Unix millisecond timestamp in the most
significant position, then random entropy.

**The timestamp leading is the whole point, and it is not cosmetic here.** v7
values sort chronologically as raw bytes, so records written in sequence land in
sequence. Against an LSM store that is the difference between appending and
scattering: random v4 keys distribute writes across the entire keyspace, forcing
compaction to rewrite everything, while v7 keys append. The ToDo is a work
queue written constantly and read by key, which is exactly the access pattern v7
was designed for — the engine choice in the table above and this choice are the
same decision seen twice.

It also buys two things for free. A range scan over an identifier range **is** a
range scan over a time window, so "everything this node accepted between 09:00
and 09:05" needs no secondary index. And a Journey, its Messages and its Streams
sort into creation order without a separate sequence column, which is what makes
the execution history in section 3 of `runtime-model.md` readable straight from
the store.

**The choice is the same on every target OS.** Windows, Linux, macOS, ARM and
the industrial targets make no difference — UUID generation is a library
concern, and the Rust `uuid` crate emits RFC byte order everywhere. What varies
is not the operating system but two boundaries, and both are worth knowing
before someone meets them in production.

**Boundary one: .NET's `Guid` is not RFC byte order.** `System.Guid` inherits
the Windows COM/OLE layout, where the first three fields are little-endian. The
operator surfaces are .NET 11 (ADR-0014), so any identifier crossing into them
must use `ToByteArray(bigEndian: true)` and `new Guid(bytes, bigEndian: true)`.
Use the default overloads and the bytes come back scrambled relative to what the
store holds — the values still round-trip if you are consistent, and they stop
sorting, which is the entire reason for choosing v7. `Guid.CreateVersion7()`
exists from .NET 9, so .NET 11 has it.

**Boundary two: SQL Server sorts `uniqueidentifier` in its own order.** It
compares the last six bytes first, so a v7 written there sorts essentially at
random and behaves like a v4 — page splits, fragmentation, and inserts that get
dramatically slower at volume. This does not affect Xmip's own stores: RocksDB
compares raw bytes and SQLite compares BLOBs with `memcmp`, so RFC-ordered v7
sorts correctly in both. It matters only when Xmip identifiers are written into
somebody else's SQL Server through `xmip-core-transport-mssql`, which is an
integration target rather than Xmip storage. Do not conclude from that
literature that v7 is a bad key — conclude that it is a bad key *in SQL Server*.

Two further cautions:

**v7 leaks creation time.** The millisecond is recoverable from any identifier
that leaves the estate. Inside Xmip that is a feature. On anything published to
an external party, treat the identifier as disclosing when the record was made,
and use a v4 where that matters.

**Same-millisecond ordering needs the counter.** RFC 9562 allows the `rand_a`
field to act as a monotonic counter within a millisecond. A queue that ingests
faster than 1 kHz and cares about insertion order must use it, or ordering
within a millisecond is random.


The `v4` feature stays enabled deliberately. It is the right choice for an
identifier that leaves the estate and must not disclose when it was created.

