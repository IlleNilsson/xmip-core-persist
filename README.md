# xmip-core-persist

Durable runtime state behind one Xmip store contract: execution checkpoints, Journey recovery, leases, and deduplication.

It owns the persistence contract for runtime state. Technology-specific stores belong in child implementation modules and runtime orchestration remains in `xmip-core-runtime`.

Status: planned, with the initial store contract and recovery records already present.
