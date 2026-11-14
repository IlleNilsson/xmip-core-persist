//! The cluster's members as Xmip Storage keeps them: the administration
//! database's `membership` table, a table of its own (the owner,
//! 2026-10-10: *every type of data goes into dedicated tables, dedicated
//! columns*), a row for each start of each node (the design of 2026-11-12
//! the owner approved, section 3.2; the membership the cluster
//! consolidation agreed on 2026-11-07).
//!
//! **Written by the node itself.** Each node writes its row once it
//! reaches Xmip Storage, and again as it stops in order: its scope, the
//! roles it declares and its capability's evidence, where its operate and
//! sync listeners answer, its place in `[storage] nodes` where it is a
//! Storage node, the signed save it runs, and its process, drawn new at
//! each start, which keys the row. When it started and stopped, how many
//! writes the table had taken at its last write, and whether it is its
//! node's latest row are Xmip Storage's to set, on its clock.
//!
//! **Kept, never deleted** (ADR-0040): a node started again writes a row of
//! its own, and its row before stays, no longer its node's latest. The
//! members now are each node's latest row, found by one range of an index
//! that holds only those, whatever the rows before them number.
//!
//! **Waited on** (`super::watch::Watched::Membership`): the table's count
//! is the number of its latest write, which every membership write moves,
//! so a node following the members is told as a row is written rather than
//! asking again.

use codec::cursor::Cursor;
use codec::field;
use codec::form::Form;
use codec::writer::ByteWriter;

use super::row::{Columned, Row, Value};

/// What a member's rows are kept under in the administration database.
pub(crate) const MEMBERSHIP: &str = "membership";

/// One node's start as the `membership` table keeps it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberRecord {
    /// The process it runs in, drawn at random as it starts and new at
    /// each start: the row's key.
    pub process: u128,
    /// The node: `xmip:///<cluster>/node/<name>`.
    pub node: String,
    /// The roles it declares, as `node::Capability::words` says them:
    /// `receiving,sending`, or nothing where it declares none.
    pub roles: String,
    /// Its capability's evidence, as `node::Capability::evidence` says it.
    pub evidence: String,
    /// The network address its operate listener answers at, where it has
    /// one.
    pub operate: Option<String>,
    /// The address its sync listener answers at, where it starts one
    /// (ADR-0065, ADR-0067).
    pub sync: Option<String>,
    /// Its place in `[storage] nodes`, from zero, where it is one of the
    /// Storage nodes listed: found by the process it answers as, never by
    /// a name.
    pub storage_place: Option<u32>,
    /// The save of `cluster.xmip` the signed slice it runs was sliced from
    /// (ADR-0031, amendment 2026-11-06, point 3): what its peers' is
    /// compared with.
    pub save: String,
    /// That slice's serial.
    pub serial: u64,
    /// When Xmip Storage first wrote it, in nanoseconds since the Unix
    /// epoch: Xmip Storage's to set.
    pub started_unix_nanos: u64,
    /// When it stopped in order, none while it runs: Xmip Storage's to set,
    /// as a row is written saying it stopped, whatever time it says.
    pub stopped_unix_nanos: Option<u64>,
    /// How many writes the table had taken at its last write: Xmip
    /// Storage's to set.
    pub written: u64,
    /// Whether it is its node's latest row: Xmip Storage's to set.
    pub latest: bool,
}

impl MemberRecord {
    /// Whether it runs: it never said it stopped in order.
    #[must_use]
    pub const fn runs(&self) -> bool {
        self.stopped_unix_nanos.is_none()
    }
}

fn write_text(out: &mut Vec<u8>, text: Option<&str>) {
    field::optional(out, text, field::text);
}

impl Form for MemberRecord {
    fn write(&self, out: &mut Vec<u8>) {
        out.u128_be(self.process);
        field::text(out, &self.node);
        field::text(out, &self.roles);
        field::text(out, &self.evidence);
        write_text(out, self.operate.as_deref());
        write_text(out, self.sync.as_deref());
        field::optional(out, self.storage_place, |out, place| {
            out.u32_be(place);
        });
        field::text(out, &self.save);
        out.u64_be(self.serial).u64_be(self.started_unix_nanos);
        field::optional(out, self.stopped_unix_nanos, |out, stopped| {
            out.u64_be(stopped);
        });
        out.u64_be(self.written).byte(u8::from(self.latest));
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            process: cursor.u128_be()?,
            node: field::read_text(cursor)?,
            roles: field::read_text(cursor)?,
            evidence: field::read_text(cursor)?,
            operate: field::read_optional(cursor, field::read_text)?,
            sync: field::read_optional(cursor, field::read_text)?,
            storage_place: field::read_optional(cursor, Cursor::u32_be)?,
            save: field::read_text(cursor)?,
            serial: cursor.u64_be()?,
            started_unix_nanos: cursor.u64_be()?,
            stopped_unix_nanos: field::read_optional(cursor, Cursor::u64_be)?,
            written: cursor.u64_be()?,
            latest: cursor.byte()? != 0,
        })
    }
}

impl Columned for MemberRecord {
    fn id(&self) -> u128 {
        self.process
    }

    /// Started at its first write and kept; stopped at the first write that
    /// says it stopped, and kept.
    fn stamp(&mut self, now: u64, before: Option<&Self>) {
        self.started_unix_nanos = before.map_or(now, |b| b.started_unix_nanos);
        if self.stopped_unix_nanos.is_some() {
            let earlier = before.and_then(|b| b.stopped_unix_nanos);
            self.stopped_unix_nanos = Some(earlier.unwrap_or(now));
        }
    }

    fn row(&self) -> Row {
        let text = |text: &str| Some(Value::Text(text.to_string()));
        vec![
            ("node", text(&self.node)),
            ("roles", text(&self.roles)),
            ("evidence", text(&self.evidence)),
            ("operate", self.operate.as_deref().and_then(text)),
            ("sync", self.sync.as_deref().and_then(text)),
            (
                "storage_place",
                self.storage_place.map(|p| Value::Count(p.into())),
            ),
            ("save", text(&self.save)),
            ("serial", Some(Value::Count(self.serial))),
            ("started_at", Some(Value::Time(self.started_unix_nanos))),
            ("stopped_at", self.stopped_unix_nanos.map(Value::Time)),
            ("written", Some(Value::Count(self.written))),
            ("latest", Some(Value::Flag(self.latest))),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::super::wire::{Answer, Request};
    use super::*;

    /// A member as its node writes it: Xmip Storage's own values unset.
    fn member(process: u128, node: &str) -> MemberRecord {
        MemberRecord {
            process,
            node: node.to_string(),
            roles: "receiving".to_string(),
            evidence: "declares receiving; offline".to_string(),
            operate: Some("127.0.0.1:7443".to_string()),
            sync: None,
            storage_place: None,
            save: "sha256:00".to_string(),
            serial: 3,
            started_unix_nanos: 0,
            stopped_unix_nanos: None,
            written: 0,
            latest: false,
        }
    }

    #[test]
    fn a_member_comes_back_from_its_bytes_as_it_was() {
        let node = configure::fixture::test_cluster().node_scope(0);
        let running = member(u128::MAX - 1, &node);
        let stopped = MemberRecord {
            storage_place: Some(1),
            sync: Some("127.0.0.1:7444".to_string()),
            stopped_unix_nanos: Some(9),
            written: 4,
            latest: true,
            ..running.clone()
        };
        for record in [running.clone(), stopped.clone()] {
            let back = MemberRecord::from_bytes(&record.bytes()).expect("read");
            assert_eq!(back, record);
        }

        let asked = [
            Request::WriteMember(Box::new(running.clone())),
            Request::ReadMembers,
        ];
        for request in asked {
            assert_eq!(
                Request::from_bytes(&request.bytes()).expect("crossed"),
                request
            );
        }
        assert_eq!(
            Request::WriteMember(Box::new(running.clone())).operation(),
            ("write", "write_member")
        );
        assert_eq!(Request::ReadMembers.operation(), ("read", "read_members"));
        let answered = Answer::Members(vec![running, stopped]);
        assert_eq!(
            Answer::from_bytes(&answered.bytes()).expect("crossed"),
            answered
        );
    }

    #[test]
    fn storage_sets_when_a_member_started_and_stopped_and_keeps_both() {
        let node = configure::fixture::test_cluster().node_scope(0);
        let mut first = member(7, &node);
        first.stamp(10, None);
        assert_eq!(
            (first.started_unix_nanos, first.stopped_unix_nanos),
            (10, None)
        );
        assert!(first.runs());

        let mut again = member(7, &node);
        again.stamp(20, Some(&first));
        assert_eq!(again.started_unix_nanos, 10, "started at its first write");

        let mut stopping = MemberRecord {
            stopped_unix_nanos: Some(0),
            ..member(7, &node)
        };
        stopping.stamp(30, Some(&again));
        assert_eq!(stopping.stopped_unix_nanos, Some(30), "Storage's time");
        let mut asked_again = stopping.clone();
        asked_again.stamp(40, Some(&stopping));
        assert_eq!(asked_again.stopped_unix_nanos, Some(30), "kept");
        assert!(!asked_again.runs());
    }
}
