# The watch keeps what none waits on within a bound — design

The verified defect, 2026-11-12. In `module/platform/persist/.src/storage/watch.rs` a wait adds its identity to `Waiting.known` (`:231`, `waiting.known.entry(position.watched).or_insert(None)`). When the last listener goes, `impl Drop for Listening` (`:279-293`) takes the waiter out of `waiting.on` but leaves the identity in `known`. Nothing bounds `known` and nothing evicts from it, so it grows with every Instance, correlation key and sequence head ever watched, not with the waits under way.

State: drafted, not agreed and not built. Every reference below was read at HEAD of `xmip-core-persist` (0176368). persist also carries another worker's uncommitted changes in `standby*`, `client.rs`, `server.rs` and `fixture/hooked.rs`. This design plans **no edit** to any of those files.

---

## 1. What `known` is for

The watch module doc, `watch.rs:42-46`:
> The watch keeps where its writer left each it watched, so a wait reads the store only for what none waited on before. Its writer tells in the order it writes, so what was told last is where each stands — a key's count goes back to none where it is let go, so the last told is kept, never the highest.

The field doc, `watch.rs:141-144`:
> Each waited on since the watch opened, where its writer last left it, or a wait read it where its writer told nothing since; `None` while it is to be read.
> `known: HashMap<Watched, Option<u64>>,`

ADR-0018, first amendment of 2026-11-12 (`doc/decision/ADR-0018-service-and-host.md:1522-1527`):
> A waiter is listed before the store is read, so a write between the two is read or told, never lost. [...] The watch keeps where its writer left each queue a node has waited on, so a wait reads the store only for a queue none waited on before, never once per queue per wake.

The second amendment of that date (`:1685-1688`):
> it tells in the order it writes, so the watch keeps what it was told last, never the highest, a key going back to none where it is let go.

So `known` is a cache of each count's last position. It saves one store read (`stands`, `watch.rs:296-304`) each time something is waited on again. The step watches (`runtime/src/queue_watch.rs:8-16`) wait on **every queue of the step at every wake**. A step's queues therefore cycle from listed to unlisted and back on every wake, and the cache is what makes that cycle cost no read.

The test that holds this behavior is `watch/tests.rs:145-168`, `a_queue_waited_on_before_is_found_where_its_writer_left_it_never_read_again`:
> `assert_eq!(counted.touched(), (0, 0, 0), "a later wait reads nothing");` [...] `// Moved while none waited on it: its writer told the watch even so.`

### Who writes and reads `known` today

| Where | What it does |
|---|---|
| `listening`, `watch.rs:224-235`, under the lock | Lists the waiter in `on` **first** (`:226-230`). Then inserts `None` where the count is absent and marks it to be read (`:231-234`). |
| `wait`, `watch.rs:200-206`, **outside** the lock | Reads each count marked to be read from the store (`stands`). The comment: *"Listed before any count is read, so a write between the two is either read or told."* |
| `standing`, `watch.rs:247-269`, under the lock | `entry(watched).or_insert(None).get_or_insert(at)`. A tell that came since the read wins (*"A write told since the read is later than it"*). Then answers each count from `known`, else from `seen`. |
| `moved`, `watch.rs:155-171`, under the lock | Called by the writer once the batch is readable: `commit.rs:272-275`, *"Readable now: every node waiting on a queue it moved woken."* It updates `known` **only where an entry already exists** (`get_mut`, `:161-163`) and tells every waiter in `on`. |
| `Listening::drop`, `watch.rs:279-293` | Removes the waiter from `on` and leaves `known` as it is. **This is the defect.** |

## 2. When an entry may go, and why letting it go is safe

**Rule: an entry may be let go only while no waiter lists it,** that is, while `on` has no entry for it.

**Why it is safe while unlisted.** The writer tells `known` even with no waiter listed (`moved` updates every present entry). An unlisted entry is therefore only a cache of where its writer left it. Removing one returns that identity to the state of something never watched before. The next wait inserts `None` in `listening`, marks it to be read, lists the waiter **before** its read, and reads the store. That is exactly the first-ever wait, which the existing protection already covers. All four interleavings of a racing write W (store from v0 to v1) are answered with v1:

1. **W applied and told before the listing.** `moved` finds no entry and changes nothing, since nobody waits. The read after the listing sees v1, so the wait is answered at once.
2. **W told after the listing and before `standing`.** `known` becomes `Some(v1)` and the waiter is told v1. Whether the read found v0 or v1, `get_or_insert` keeps v1.
3. **W applied before the read and told after `standing`.** The read finds v1, `standing` stores v1, and the tell stores v1 again and tells the waiter.
4. **W applied and told after the read.** `standing` stores `Some(v0)`. The tell then stores v1 and tells the waiter, and `Waiter::waited` raises its answer to v1 (`waiter.rs:252-260`).

The writer is one thread and tells batch B (`commit.rs:274`) before it applies batch B+1. So no read can see a later batch before an earlier batch's tell.

**Why it is unsafe while listed.** Suppose an entry is removed between `listening` and `standing`. The read finds v0, W is told (v1), and then the entry is removed. Today's `standing` would re-insert the **older** read v0 (`entry().or_insert(None).get_or_insert(v0)`). The cache would then say v0 while the store stands at v1. The next waiter with `seen = v0` would wait past W (a lost wake), and one with `seen = v1` would be answered with an older position. Listed entries must therefore never be let go. The design keeps that true in three ways:

- by construction: the eviction order holds only unlisted entries;
- by a check in the eviction loop: it removes a count only where the order's stamp still matches the count's own stamp;
- `standing` no longer inserts at all. It only fills an entry that is present (`Known::read` uses `get_mut`).

## 3. The bound and the eviction order

- **The bound is on the counts no one waits on.** Every listed count is always kept, so the active set follows the waits under way. ADR-0018's second amendment (`:1729-1730`) says *"the names a step adds are one-shot, let go of once they moved"*. The total kept is at most the listed counts plus the bound.
- **Eviction order: the longest unwaited goes first** (least recently used among the unwaited). The stamp is taken when the **last waiter lets go**. A new listing takes the count out of the order, so its next release stamps it as the newest.
  - A step's queues are re-waited at every wake, so they always sit at the newest end.
  - An Instance, key or head named once ages out first.
  - Writes do not count as use: a count written often but never waited on again still ages out.
- **An unwaited count that holds nothing is let go at once.** This happens where the count's read failed, or its writer told it as unreadable. There is nothing to keep.
- **Default: `UNWAITED = 16_384`.**
  - Each unwaited count costs about 150–220 bytes: a `HashMap` slot of `(Watched 32 B, Kept 32 B)`, and a `BTreeMap` entry of `(u64, Watched)` in leaves about half full under appends.
  - That is about 3 MB at most at the default.
  - It sits far above the queue tails and heads that a large cluster's steps re-wait at every wake.
  - Decided while designing.
- **Minimum 0** means none is kept and every wait reads the store. **Maximum 4,194,304**, about 900 MB.

## 4. Where the bound is configured

Hardware assumptions go in the Cluster TOML `[tuning]` (ADR-0031, amendment 2026-10-03, quoted at `runtime/src/tuning.rs:1-5`). persist receives tuning in two ways:

- **A constant default plus a setter after open.** This is the `storage_wait` pattern: `server/waiters.rs:16-20` declares `pub const HOLD`, `server/waiters.rs:110-113` has `hold(most)`, and `server.rs:166-171` has `StorageServer::hold_waits`. The runtime wires it at `runtime/src/storage/opened.rs:92,112` (`server.hold_waits(tuning.storage_wait);`). `Embedded::in_epoch` (`embedded/epoch.rs:13`) is the same shape of setter.
- **`standby::Timings`** (`standby.rs:102-117`), filled by `Tuning::storage()` (`tuning.rs:175-186`).

This design uses the first way. The new `[tuning] storage_unwaited` key defaults to `persist::storage::UNWAITED` and is set with `Embedded::keep_unwaited(most)` after `Embedded::open`, for the Storage node that is the only one (`runtime/src/storage/opened.rs:97-103`).

**A Storage node of several builds its primary in `standby/finding.rs:121-129`** (`Embedded::open(...)`, then `primary.in_epoch(epoch)`). Carrying the key there means touching `standby.rs` (`Timings`) and `finding.rs`, both of which hold the other worker's uncommitted change, so it is **deferred** (see §8). Until then that primary keeps the default. It is still bounded, because the defect is fixed in persist whatever the configuration.

## 5. Exact code changes

### 5.1 New `module/platform/persist/.src/storage/watch/known.rs` (about 135 lines)

```rust
//! Where the one writer left each thing a wait watches (`super`), kept so a
//! wait reads the store only for what is not kept: everything waited on
//! now, and of what none waits on now the latest let go of, at most
//! [`UNWAITED`] — the Storage node's `[tuning] storage_unwaited` — the
//! longest unwaited let go first. Until 2026-11-12 every Instance, key and
//! sequence ever waited on stayed kept as long as the node ran, so what it
//! held grew with every conversation it carried, never with its waits
//! (the review of 2026-11-12).
//!
//! **Let go only where none waits on it.** A count waited on was listed
//! before the store was read, and is kept as its writer tells it or the
//! read found it, a tell winning; let go between the two it would come back
//! as the read found it, older than a write told meanwhile. What none waits
//! on is only a cache of where it stands: let go, the next wait on it reads
//! the store as the first wait on it ever did, listed first, so a write
//! between the two is read or told.

use std::collections::{BTreeMap, HashMap};

use super::Watched;

/// How many of what no node waits on now a Storage node keeps where its
/// writer left it, where its `[tuning] storage_unwaited` does not say:
/// some 200 bytes each, about 3 MB in all, far above the queues and heads
/// a cluster's steps wait on again at every wake.
pub const UNWAITED: usize = 16_384;

/// Where its writer left each waited on now, and the latest none waits on.
#[derive(Debug)]
pub(super) struct Known {
    kept: HashMap<Watched, Kept>,
    /// What none waits on now, by when the last waiting on it let go: the
    /// first, the longest unwaited.
    unwaited: BTreeMap<u64, Watched>,
    /// The next of those.
    next: u64,
    /// The most of them kept.
    most: usize,
}

#[derive(Clone, Copy, Debug)]
struct Kept {
    /// Where its writer last left it, or a wait read it where its writer
    /// told nothing since; `None` while it is to be read.
    at: Option<u64>,
    /// When the last waiting on it let go, where none waits on it now.
    unwaited: Option<u64>,
}

impl Default for Known {
    fn default() -> Self {
        Self {
            kept: HashMap::new(),
            unwaited: BTreeMap::new(),
            next: 0,
            most: UNWAITED,
        }
    }
}

impl Known {
    /// `watched` waited on: kept as long as any waits on it; whether it is
    /// to be read.
    pub(super) fn listed(&mut self, watched: Watched) -> bool {
        let kept = self.kept.entry(watched).or_insert(Kept {
            at: None,
            unwaited: None,
        });
        if let Some(since) = kept.unwaited.take() {
            self.unwaited.remove(&since);
        }
        kept.at.is_none()
    }

    /// Where `watched` stands, where it is kept.
    pub(super) fn at(&self, watched: Watched) -> Option<u64> {
        self.kept.get(&watched).and_then(|kept| kept.at)
    }

    /// `watched` left at `at` by its writer — `None` where it could not be
    /// read, read again by the next wait on it — where it is kept.
    pub(super) fn told(&mut self, watched: Watched, at: Option<u64>) {
        if let Some(kept) = self.kept.get_mut(&watched) {
            kept.at = at;
        }
    }

    /// `watched` read at `at` by a wait listed on it: kept unless its
    /// writer told it since, which is later than the read.
    pub(super) fn read(&mut self, watched: Watched, at: u64) {
        if let Some(kept) = self.kept.get_mut(&watched) {
            kept.at.get_or_insert(at);
        }
    }

    /// None waits on `watched` now: let go at once where nothing is kept of
    /// it, else kept as the latest unwaited, the longest unwaited let go
    /// past the most.
    pub(super) fn unwaited(&mut self, watched: Watched) {
        let Some(kept) = self.kept.get_mut(&watched) else {
            return;
        };
        if kept.at.is_none() {
            self.kept.remove(&watched);
            return;
        }
        let since = self.next;
        self.next += 1;
        if let Some(earlier) = kept.unwaited.replace(since) {
            self.unwaited.remove(&earlier);
        }
        self.unwaited.insert(since, watched);
        self.trim();
    }

    /// At most `most` of what none waits on kept from now on.
    pub(super) fn keep(&mut self, most: usize) {
        self.most = most;
        self.trim();
    }

    /// The longest unwaited let go until at most the most are kept: each
    /// only where its stamp is still its own, so one waited on is never.
    fn trim(&mut self) {
        while self.unwaited.len() > self.most {
            let Some((since, watched)) = self.unwaited.pop_first() else {
                return;
            };
            if self.kept.get(&watched).is_some_and(|k| k.unwaited == Some(since)) {
                self.kept.remove(&watched);
            }
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Known {
    /// How many are kept, and of them how many none waits on.
    pub(super) fn kept(&self) -> (usize, usize) {
        (self.kept.len(), self.unwaited.len())
    }
}
```

### 5.2 New `module/platform/persist/.src/storage/watch/form.rs` (about 70 lines), for the size gate

Move the three `Form` impls from `watch.rs:335-391` verbatim (`Watched`, `Position`, `Moved`), with their imports:

```rust
//! The bytes of what a wait watches and answers (`super`), as the wire
//! carries them (`super::super::wire`).

use codec::CodecError;
use codec::cursor::Cursor;
use codec::form::Form;
use codec::writer::ByteWriter;

use super::super::due::DueQueue;
use super::{Moved, Position, Watched};
// impl Form for Watched / Position / Moved — moved unchanged
```

This is needed because `watch.rs` has 392 lines above `#[cfg(test)]` (line 393). The gate counts the lines above `#[cfg(test)]` (`doc/governance/rust-style.md:31,77`). The edits below add about 7 net lines, which would take the file to about 399, at the edge after rustfmt. With the move, `watch.rs` ends at **about 340**.

### 5.3 `watch.rs` edits

- **`:42-44` (module doc).** Replace *"The watch keeps where its writer left each it watched, so a wait reads the store only for what none waited on before."* with:
  > The watch keeps where its writer left each waited on now, and the latest of what none waits on now, at most [`UNWAITED`] (`[tuning] storage_unwaited`), the longest unwaited let go first (`known`): a wait reads the store only for what is not kept, and what is kept follows the waits under way, never every Instance, key and sequence ever waited on.

  Keep the sentences after it (*Its writer tells in the order it writes …*).
- **`:64-67`.** Delete the four `codec` imports; they moved to `form.rs`.
- **`:76-81`.** Add:

  ```rust
  mod form;
  mod known;
  use known::Known;
  pub use known::UNWAITED;
  ```
- **`:141-144`.** The field becomes:

  ```rust
  /// Where its writer left each waited on now, and the latest none waits
  /// on now (`known`).
  known: Known,
  ```

  `Waiting` keeps `#[derive(Debug, Default)]`, because `Known` implements `Default`.
- **`moved`, `:161-163`.** Becomes `waiting.known.told(*watched, *at);`.
- **`listening`, `:231-234`.** Becomes:

  ```rust
  if waiting.known.listed(position.watched) && !unknown.contains(&position.watched) {
      unknown.push(position.watched);
  }
  ```

  `listed` runs first for every position, so every listed count leaves the eviction order.
- **`standing`, `:249-267`.** Becomes:

  ```rust
  for &(watched, at) in read {
      // A write told since the read is later than it; one the read
      // saw untold yet is told to this waiter, listed before it read.
      waiting.known.read(watched, at);
  }
  seen.iter()
      .map(|seen| Position {
          watched: seen.watched,
          at: waiting.known.at(seen.watched).unwrap_or(seen.at),
      })
      .collect()
  ```
- **`Listening::drop`, `:287-289`.** Becomes:

  ```rust
  if waiters.is_empty() {
      waiting.on.remove(watched);
      waiting.known.unwaited(*watched);
  }
  ```
- **New method in `impl Watch`, after `refuse`:**

  ```rust
  /// At most `most` of what none waits on now kept where its writer left
  /// it, the longest unwaited let go first: the Storage node's `[tuning]
  /// storage_unwaited` ([`UNWAITED`] where it does not say).
  pub(crate) fn keep_unwaited(&self, most: usize) {
      self.waiting.locked().known.keep(most);
  }
  ```
- **The test-support block, `:327-333`.** Add:

  ```rust
  /// How many counts it keeps, and of them how many none waits on now.
  pub(crate) fn kept(&self) -> (usize, usize) {
      self.waiting.locked().known.kept()
  }
  ```
- **`:335-391`.** Remove; moved to `form.rs`.

### 5.4 `storage/embedded.rs` (`#[cfg(test)]` moves from line 371 to about 384)

After `refuse_waits` (`:108-113`), add:

```rust
/// At most `most` of what no node waits on now kept where its writer
/// left it (`super::watch`): its `[tuning] storage_unwaited`.
pub fn keep_unwaited(&self, most: usize) {
    self.watch.keep_unwaited(most);
}
```

In the test-support block (`:361-369`), add:

```rust
/// How many counts its watch keeps, and of them how many none waits on
/// now: what a test of its bound reads.
#[must_use]
pub fn watch_kept(&self) -> (usize, usize) {
    self.watch.kept()
}
```

### 5.5 `storage.rs:103` (one line)

`pub use watch::{Moved, Position, UNWAITED, Waiter, Watched};`

### 5.6 runtime: `module/platform/runtime/src/tuning.rs`

- Add the doc line after `:40`:
  `//! storage_unwaited = 16384                   # persist's storage::UNWAITED`
- Add the field after `:91`: `pub storage_unwaited: usize,`
- Add the read after `:162`: `storage_unwaited: count("storage_unwaited"),`
- Tests:
  - In `the_declaration_is_sound_and_its_defaults_are_the_constants`, add `assert_eq!(tuning.storage_unwaited, persist::storage::UNWAITED);` beside `:282`.
  - In `what_a_node_says_is_what_it_runs_by`, add `storage_unwaited = 64\n` to the text and `assert_eq!(tuning.storage_unwaited, 64);`.

### 5.7 runtime: `module/platform/runtime/src/tuning/declared.rs`

Add after `storage_wait` (`:237-244`):

```rust
Setting {
    name: "storage_unwaited",
    kind: Kind::Integer {
        minimum: 0,
        maximum: 4_194_304,
    },
    presence: Presence::Default(Fixed::Integer(as_integer(persist::storage::UNWAITED))),
    meaning: "How many of what no node waits on now a Storage node keeps where its writer \
              left it, so a wait on one again reads nothing; past it the longest unwaited \
              is let go and read again when next waited on.",
    applies: Applies::Both,
},
```

### 5.8 runtime: `module/platform/runtime/src/storage/opened.rs:99-103`

The Storage node that is the only one; add the last line:

```rust
let opened =
    Embedded::open(runtime, administration, audit, keys.as_ref(), &kek)
        .map_err(|problem| did_not_open(problem.to_string()))?;
opened.keep_unwaited(tuning.storage_unwaited);
```

**Surfaces.** The key is declared once in `tuning::TUNING`. A grep for the `storage_*` keys finds no surface (C#, PowerShell or GUI) that lists them on its own, so nothing else has to change.

## 6. Tests

The tests go in a new `module/platform/persist/.src/storage/watch/tests/kept.rs` (about 175 lines), declared with `mod kept;` beside `mod kinds;` (`watch/tests.rs:20`). `fn turned` in `watch/tests/kinds.rs:44` becomes `pub(super) fn turned`, so `kept.rs` can reuse it.

```rust
//! What a watch keeps (the review of 2026-11-12): every count waited on
//! now, and of those none waits on at most its bound, the longest unwaited
//! let go first and read again when next waited on — no write lost for it.

use std::sync::atomic::AtomicBool;

use super::kinds::turned;
use super::*;

fn instance(id: u128, at: u64) -> Position {
    Position { watched: Watched::Instance(id), at }
}

fn key(id: u128, at: u64) -> Position {
    Position { watched: Watched::Key(id), at }
}

/// `queue` left at `at` by its writer, as a batch that moved it tells.
fn wrote(watch: &Watch, queue: u128, at: u64) {
    watch.moved(&[Change { watched: Watched::Queue(queue), at: Some(at), own: None }]);
}
```

### 6.1 The acceptance test

Distinct Instances are made, watched and completed, and what the watch keeps settles at its bound:

```rust
/// The review of 2026-11-12: a Storage node kept the count of every
/// Instance and key any wait ever named, growing with every conversation
/// it carried rather than with the waits under way.
#[test]
fn distinct_instances_made_watched_and_ended_leave_the_watch_at_its_bound() {
    let (node, _) = node();
    let most = 8;
    node.keep_unwaited(most);
    let waiter = Arc::new(Waiter::new());
    let counted = node.runtime().engine();
    for n in 0..100u128 {
        let (id, correlation) = (1_000 + n, 5_000 + n);
        turned(&node, 2 * n + 1, (id, 0), (correlation, false));
        // Seen as before it was made: answered at once, as it stands.
        assert_eq!(
            waited(&node, &[instance(id, 0), key(correlation, 0)], LONG, &waiter),
            [instance(id, 1), key(correlation, 1)]
        );
        turned(&node, 2 * n + 2, (id, 1), (correlation, true));
        let (kept, unwaited) = node.watch_kept();
        assert_eq!(kept, unwaited, "none waits on any now");
        assert!(unwaited <= most, "{unwaited} kept after {n}");
    }
    assert_eq!(node.watch_kept(), (most, most), "settled at its bound");
    counted.touched();
    assert_eq!(
        waited(&node, &[instance(1_099, 1), key(5_099, 1)], LONG, &waiter),
        [instance(1_099, 2), key(5_099, 0)],
        "the latest kept where its writer left it as it ended"
    );
    assert_eq!(counted.touched(), (0, 0, 0), "and read no more");
    assert_eq!(
        waited(&node, &[instance(1_000, 1), key(5_000, 1)], LONG, &waiter),
        [instance(1_000, 2), key(5_000, 0)],
        "the longest unwaited let go, read as it stands"
    );
    assert_ne!(counted.touched(), (0, 0, 0), "read again");
}
```

Without the fix, `watch_kept` would read 200 counts, all of them unwaited, and the second assertion in the loop fails at n = 4.

### 6.2 Race test: let go, then waited on, then written (deterministic, all four interleavings)

```rust
/// A count let go is read again by the next wait on it, as the first wait
/// on it ever was, and a write around that read is told or read whichever
/// way the two fall: in before the read or after it, told before the
/// wait's standing or after it.
#[test]
fn a_count_let_go_is_read_again_and_no_write_around_that_read_is_lost() {
    let watch = Watch::default();
    watch.keep_unwaited(0);
    let waiter = Arc::new(Waiter::new());
    let (queue, seen) = (Watched::Queue(QUEUE), [at(QUEUE, 0)]);
    for (read, told_first) in [(1, true), (1, false), (0, true), (0, false)] {
        waiter.begin();
        let (listening, unknown) = watch.listening(&seen, &waiter).expect("listed");
        assert_eq!(unknown, [queue], "let go: to be read");
        if told_first {
            wrote(&watch, QUEUE, 1);
        }
        let now = watch.standing(&seen, &[(queue, read)]);
        if !told_first {
            wrote(&watch, QUEUE, 1);
        }
        assert_eq!(
            waiter.waited(&seen, now, Some(Instant::now())).expect("answered"),
            [at(QUEUE, 1)],
            "read at {read}, told first: {told_first}"
        );
        assert_eq!(watch.standing(&seen, &[]), [at(QUEUE, 1)], "kept as its writer left it");
        drop(listening);
        assert_eq!(watch.kept(), (0, 0), "let go as its wait ended");
    }
}
```

This test is white-box: it calls `listening`, `standing`, `moved` and `Waiter::begin`/`waited` in each exact order, which a child module of `watch` may do. It needs no engine hook, so `fixture/hooked.rs`, which another worker is changing, stays untouched.

### 6.3 A listed count is never let go

```rust
#[test]
fn a_count_waited_on_is_never_let_go_however_small_the_bound() {
    let watch = Watch::default();
    let (waiter, other) = (Arc::new(Waiter::new()), Arc::new(Waiter::new()));
    let seen = [at(QUEUE, 0)];
    let (listening, _) = watch.listening(&seen, &waiter).expect("listed");
    watch.standing(&seen, &[(Watched::Queue(QUEUE), 0)]);
    watch.keep_unwaited(0);
    for queue in 100..110 {
        let seen = [at(queue, 0)];
        let (_others, _) = watch.listening(&seen, &other).expect("listed");
        watch.standing(&seen, &[(Watched::Queue(queue), 0)]);
    }
    assert_eq!(watch.kept(), (1, 0), "the one waited on, none unwaited");
    wrote(&watch, QUEUE, 1);
    assert_eq!(watch.standing(&seen, &[]), [at(QUEUE, 1)], "where its writer left it");
    drop(listening);
    assert_eq!(watch.kept(), (0, 0), "let go once none waits on it");
}
```

### 6.4 Race test: concurrent, through the real node and its one writer, with counts let go all along

```rust
/// Waits racing writes while every other count is let go as its wait ends:
/// each wait answered with the write that raced it, and one asked after a
/// write none waited on with that write — never older than it stands.
#[test]
fn waits_racing_writes_miss_none_while_counts_are_let_go_all_along() {
    let (node, _) = node();
    let node = Arc::new(node);
    node.keep_unwaited(1);
    let churning = Arc::new(AtomicBool::new(true));
    let churn = {
        let (node, churning) = (Arc::clone(&node), Arc::clone(&churning));
        thread::spawn(move || {
            let waiter = Arc::new(Waiter::new());
            for queue in 1_000u128.. {
                if !churning.load(Ordering::SeqCst) {
                    break;
                }
                // Seen where it never stood: answered at once, then let go.
                let _ = node.wait_queues(&[at(queue, 1)], (LONG, 0), &waiter);
            }
        })
    };
    let waiter = Arc::new(Waiter::new());
    let (mut id, mut seen) = (1u128, 0u64);
    for _ in 0..200 {
        // Written while none waits on the queue, its count kept or let go.
        published(&node, id..id + 1);
        id += 1;
        assert_eq!(waited(&node, &[at(QUEUE, seen)], LONG, &waiter), [at(QUEUE, seen + 1)]);
        seen += 1;
        // Written as a wait is listed, before or after its read.
        let woken = waiting(&node, vec![at(QUEUE, seen)], LONG, &waiter);
        published(&node, id..id + 1);
        id += 1;
        assert_eq!(answered(woken), [at(QUEUE, seen + 1)]);
        seen += 1;
    }
    churning.store(false, Ordering::SeqCst);
    churn.join().expect("churned");
}
```

A lost wake, or an older count kept, waits out `LONG` and answers `[]`, so the test fails. This is the existing tests' own style (`watch/tests.rs:13-15`).

### 6.5 Unchanged and still required to pass

- Every test in `watch/tests.rs`, including the concurrent registration/write tests: `a_publication_wakes_every_waiter_on_its_queue_and_none_on_another` `:170`, `a_publication_its_holder_claims_whole_wakes_every_waiter_but_that_holders` `:192`, `a_hand_on_that_queues_a_message_wakes_its_queue_and_a_send_moves_none` `:222`, `a_queue_waited_on_before_is_found_where_its_writer_left_it_never_read_again` `:145`.
- Both tests in `watch/tests/kinds.rs`.
- `storage::client`, `storage::server` and `storage::standby` waits.
- runtime `queue_watch` and `tests/ledger/woken*.rs`.

All of these run at the default bound.

**Timing.** Run persist's timing program (`tests/measured`) alone at landing. Each listing and release adds one `BTreeMap` insert and remove under the watch's lock, a few hundred nanoseconds. The wake medians (282 µs and 185 µs, ADR-0018 `:1758-1763`) should not move.

## 7. Records to change in the same landing

- **`doc/decision/ADR-0018-service-and-host.md`**, a new amendment: *"Amendment, 2026-11-12: the watch keeps what none waits on within a bound"*. It covers:
  - the review's defect, verified that day;
  - the earlier sentence at `:1525-1527` kept for what is waited on now and bounded for the rest;
  - what is waited on now is always kept, and why (§2);
  - the latest unwaited are kept up to `[tuning] storage_unwaited` (`storage::UNWAITED`, 16,384) and the longest unwaited goes first;
  - Storage owns the bound;
  - what is tested (§6);
  - the primary of several on the default until the follow-up;
  - provenance: the defect is the review's; the bound, its order, its key and its default are the assistant's, for the owner to overrule.
- **`module/platform/persist/README.md:113-115`.** Replace *"the writer keeping where it left each waited on so a wait reads the store only for one none waited on before"* with *"the writer keeping where it left each waited on now and the latest `UNWAITED` none waits on (`[tuning] storage_unwaited`, 16,384 by default), the longest unwaited let go first, so a wait reads the store only for one not kept (`storage/watch/known.rs`)"*.
- **`module/platform/configure/doc/node-configuration.md`**:
  - Sample, after `:406`: `storage_unwaited = 16384                    # of what no node waits on, how many a Storage node keeps`.
  - A bullet after `:495-500`: **`storage_unwaited`**: on a Storage node, how many of what no node waits on now (a queue's tail or head, an Instance, a key) it keeps where its writer left it, so a wait on one again reads nothing (16,384, about 3 MB at most). Past that, the longest unwaited is let go and read from the store when next waited on, so what it keeps follows the waits under way, never every conversation it ever carried (ADR-0018, amendment 2026-11-12).
- **`doc/decision/ADR-0031-configuration-is-toml-json-is-transport.md:288-289`.** After *"(`storage_wait`, built 2026-11-12; ADR-0018, amendment 2026-11-12)"* add *", and how many of what no node waits on a Storage node keeps (`storage_unwaited`, built 2026-11-12; ADR-0018, amendment 2026-11-12)"*.
- **`module/platform/runtime/README.md`, in the `[tuning]` list.** After *"(`storage_wait`, persist's `storage::HOLD`, a minute; 2026-11-12)"* add *", and how many of what no node waits on it keeps where its writer left it (`storage_unwaited`, persist's `storage::UNWAITED`, 16,384; 2026-11-12)"*.
- **`doc/architecture/runtime-model.md` is not touched.** It does not describe the cache, and it has uncommitted changes from another session.

## 8. Deferred follow-up (standby files, after their worker lands)

These edits bring `storage_unwaited` to a Storage node of several:

- `standby.rs:104-117`: add `pub unwaited: usize` to `Timings`.
- `standby/finding.rs:128`: add `primary.keep_unwaited(self.timings.unwaited);` after `in_epoch`.
- `runtime/src/tuning.rs:179-185`: add `unwaited: self.storage_unwaited`.
- The `Timings` literals in `standby/tests.rs:19` and the root's `.src/test/measured/wire.rs:49`: add the new field.

Until then that primary keeps `UNWAITED`. It is bounded, but not configurable.

## 9. Risks

1. **A bound below the step queues re-waited at each wake** costs one store read per such queue per wake: a `places` read, a few µs on `RocksDB`. That is a performance cost, never a correctness one. Because the longest unwaited goes first, one-shot Instances, keys and heads are let go before the queues.
2. **`HashMap` capacity never shrinks.** Memory follows the peak of the listed counts plus the bound, not history. That peak is the work under way, which is acceptable.
3. **A listed count is never let go.** This is upheld three times over (§2), and §6.3 tests it.
4. **Pre-existing, unchanged, now self-healing.** A count told unreadable (`at: None`, `moved.rs:93-100`) between a first wait's listing and its read is filled by that older read in `standing`. It stays stale only until it is let go, which a bound now guarantees. It arises only for a record that fails to decode. A fix needs a "told since listed" flag. That is out of scope here; noted for the owner.
5. **`next` is a `u64`** and never wraps at any rate a writer reaches.
6. **Size gate.** Expected lines above `#[cfg(test)]`:

   | File | Lines |
   |---|---|
   | `watch.rs` | about 340 |
   | `known.rs` | about 130 |
   | `form.rs` | about 70 |
   | `embedded.rs` | about 384 |
   | `tuning.rs` | about 251 |
   | `declared.rs` | about 301 |

   `watch/tests.rs` is 373 and `kept.rs` about 175.

## 10. Spread, stated up front

| Repo | Files |
|---|---|
| persist | `watch.rs`; new `watch/known.rs`, `watch/form.rs` and `watch/tests/kept.rs`; `watch/tests.rs` (1 line); `watch/tests/kinds.rs` (1 word); `embedded.rs` (+13); `storage.rs` (1 line); `README.md` |
| runtime | `tuning.rs`, `tuning/declared.rs`, `storage/opened.rs` (1 line), `README.md` |
| configure | `doc/node-configuration.md` |
| superproject | ADR-0018 and ADR-0031 |

No standby file is touched, and no build or test was run for this design.
