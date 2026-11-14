//! An operator's acts, kept in the administration database's `operator_act`
//! table: a table of their own, every value in a column of its own (the
//! owner, 2026-10-13: *Acts go to Xmip Storage. Nodes are consumable:
//! pause, resume, retry, dismiss and replay are written to Storage with who
//! and when, and every node honors them*; ADR-0014, amendment 2026-11-13).
//!
//! **Asked at a door, taken by its node.** The node an operator's surface
//! reaches, its door, admits the operator by the operator's own role and
//! writes the act here, asked of the node that owns what it acts on, with
//! who asked, how the door proved it, the role it was judged by and the
//! relay it came through. The owning node is told at once, by the wait it
//! holds on Xmip Storage ([`super::Watched::Acts`]), takes it, and decides
//! it: taken, refused or failed, with its own sentence.
//!
//! **Decided once.** A decision is a conditional write that takes effect
//! only where the act is still asked, so an act is decided once, across a
//! restart of its node too; the door that waits on it is told
//! ([`super::Watched::Act`]). An act asked of a node that is not running
//! stays asked until the node runs, and never expires (the owner,
//! 2026-11-12). Kept, never deleted (ADR-0040).
//!
//! **Placed in its node's order.** Each act takes the next place among the
//! acts asked of its node, Xmip Storage's to set as it writes it, so a node
//! reads the acts asked of it after the last it saw, oldest first.

use codec::CodecError;
use codec::cursor::Cursor;
use codec::field;
use codec::form::Form;
use codec::writer::ByteWriter;

use super::hold::named;
use super::row::{Columned, Row, Value};

/// What an act's records are kept under in the administration database.
pub(crate) const ACT: &str = "operator_act";

/// The index the acts asked of a node are read by, in their order, and the
/// one that holds only those still asked.
pub(crate) const BY_NODE: (&str, &str) = (ACT, "operator_act_node");
pub(crate) const STILL_ASKED: (&str, &str) = (ACT, "operator_act_open");

/// What came of an act: asked and not taken yet, or decided by its node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ActOutcome {
    /// Written, and not taken yet by the node it is asked of.
    #[default]
    Asked,
    /// Taken by its node.
    Taken,
    /// Refused by its node, in words.
    Refused,
    /// Its node could not take it: Xmip Storage did not answer it.
    Failed,
}

impl ActOutcome {
    /// Every outcome, in the order a surface lists them.
    pub const ALL: [Self; 4] = [Self::Asked, Self::Taken, Self::Refused, Self::Failed];

    /// The word its column keeps.
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::Asked => "asked",
            Self::Taken => "taken",
            Self::Refused => "refused",
            Self::Failed => "failed",
        }
    }

    /// The outcome a word names, exactly.
    #[must_use]
    pub fn named(word: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|outcome| outcome.word() == word)
    }
}

/// One operator's act, as the `operator_act` table keeps it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ActRecord {
    /// Its `UUIDv7`, minted by the door that asked it.
    pub act: u128,
    /// The node it is asked of, which owns what it acts on:
    /// `xmip:///<cluster>/node/<name>`.
    pub node: String,
    /// Its place among the acts asked of its node, from one: Xmip
    /// Storage's to set.
    pub place: u64,
    /// What it acts on, as `observe::Noun` names it, and which one.
    pub noun: String,
    pub target: String,
    /// The act, as `observe::Act` names it.
    pub word: String,
    /// Who asked it, as the door's gate proved them.
    pub who: String,
    /// How: `mutual-tls` or `peer-credentials`.
    pub proven: String,
    /// The certificate (DER) it proved, where it proved one: what the node
    /// it is asked of reads its role from again, by the rule its door
    /// admitted it by.
    pub certificate: Option<Vec<u8>>,
    /// The role the door judged it by.
    pub role: String,
    /// The relay it came through, where one vouched for its caller.
    pub through: Option<String>,
    /// The node that admitted it.
    pub door: String,
    /// When Xmip Storage wrote it, in nanoseconds since the Unix epoch.
    pub asked_unix_nanos: u64,
    pub outcome: ActOutcome,
    /// Its node's sentence; empty while it is asked.
    pub said: String,
    /// When Xmip Storage wrote its decision; nothing while it is asked.
    pub decided_unix_nanos: u64,
}

impl ActRecord {
    /// Whether it is still asked: its node has not decided it.
    #[must_use]
    pub fn is_asked(&self) -> bool {
        self.outcome == ActOutcome::Asked
    }

    /// What a wait on the acts asked of the node at `node` names, and what
    /// they are found by: the name-based identifier of its scope
    /// ([`named`]), kept beside the scope in a column of its own.
    #[must_use]
    pub fn of_node(node: &str) -> u128 {
        named(node)
    }
}

impl Form for ActOutcome {
    fn write(&self, out: &mut Vec<u8>) {
        field::text(out, self.word());
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        let word = field::read_text(cursor)?;
        Self::named(&word).ok_or_else(|| CodecError::new(format!("no act's outcome is '{word}'")))
    }
}

impl Form for ActRecord {
    fn write(&self, out: &mut Vec<u8>) {
        out.u128_be(self.act);
        for text in [&self.node, &self.noun, &self.target, &self.word] {
            field::text(out, text);
        }
        out.u64_be(self.place);
        for text in [&self.who, &self.proven, &self.role] {
            field::text(out, text);
        }
        out.byte(u8::from(self.certificate.is_some()));
        field::counted(out, self.certificate.as_deref().unwrap_or_default());
        out.byte(u8::from(self.through.is_some()));
        field::text(out, self.through.as_deref().unwrap_or_default());
        field::text(out, &self.door);
        out.u64_be(self.asked_unix_nanos);
        self.outcome.write(out);
        field::text(out, &self.said);
        out.u64_be(self.decided_unix_nanos);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        let act = cursor.u128_be()?;
        let [node, noun, target, word] = [(); 4].map(|()| field::read_text(cursor));
        let place = cursor.u64_be()?;
        let [who, proven, role] = [(); 3].map(|()| field::read_text(cursor));
        let certified = cursor.byte()? != 0;
        let certificate = field::read_counted(cursor)?.to_vec();
        let relayed = cursor.byte()? != 0;
        let through = field::read_text(cursor)?;
        Ok(Self {
            act,
            node: node?,
            place,
            noun: noun?,
            target: target?,
            word: word?,
            who: who?,
            proven: proven?,
            certificate: certified.then_some(certificate),
            role: role?,
            through: relayed.then_some(through),
            door: field::read_text(cursor)?,
            asked_unix_nanos: cursor.u64_be()?,
            outcome: ActOutcome::read(cursor)?,
            said: field::read_text(cursor)?,
            decided_unix_nanos: cursor.u64_be()?,
        })
    }
}

impl Columned for ActRecord {
    fn id(&self) -> u128 {
        self.act
    }

    /// When it was asked kept from the first write; when it was decided set
    /// by the write that decides it.
    fn stamp(&mut self, now: u64, before: Option<&Self>) {
        self.asked_unix_nanos = before.map_or(now, |b| b.asked_unix_nanos);
        self.decided_unix_nanos = match before {
            Some(before) if !before.is_asked() => before.decided_unix_nanos,
            _ if self.is_asked() => 0,
            _ => now,
        };
    }

    fn row(&self) -> Row {
        let text = |text: &str| Some(Value::Text(text.to_string()));
        let decided = (!self.is_asked()).then_some(Value::Time(self.decided_unix_nanos));
        vec![
            ("node", text(&self.node)),
            ("node_id", Some(Value::Id(Self::of_node(&self.node)))),
            ("place", Some(Value::Count(self.place))),
            ("noun", text(&self.noun)),
            ("target", text(&self.target)),
            ("act_word", text(&self.word)),
            ("who", text(&self.who)),
            ("proven", text(&self.proven)),
            ("certificate", self.certificate.clone().map(Value::Bytes)),
            ("role", text(&self.role)),
            ("through", self.through.as_deref().and_then(text)),
            ("door", text(&self.door)),
            ("asked_at", Some(Value::Time(self.asked_unix_nanos))),
            ("outcome", text(self.outcome.word())),
            ("said", text(&self.said)),
            ("decided_at", decided),
            ("open", Some(Value::Flag(self.is_asked()))),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The node an act is asked of: the test cluster's first.
    fn owner() -> String {
        configure::fixture::test_cluster().node_scope(0)
    }

    fn asked(node: &str, act: u128) -> ActRecord {
        ActRecord {
            act,
            node: node.to_string(),
            noun: "subscription".to_string(),
            target: "Orders".to_string(),
            word: "pause".to_string(),
            who: "CN=alice".to_string(),
            proven: "mutual-tls".to_string(),
            certificate: Some(vec![0x30, 0x82, 1, 2]),
            role: "Operator".to_string(),
            through: Some("CN=web".to_string()),
            door: configure::fixture::test_cluster().node_scope(1),
            ..ActRecord::default()
        }
    }

    #[test]
    fn an_act_crosses_its_bytes_as_it_was_and_its_outcome_is_its_word() {
        let mut act = asked(&owner(), 7);
        for record in [act.clone(), {
            act.through = None;
            act.outcome = ActOutcome::Refused;
            act.said = "REFUSED: no Subscription 'Orders'".to_string();
            act.place = 3;
            act
        }] {
            assert_eq!(
                ActRecord::from_bytes(&record.bytes()).expect("read"),
                record
            );
        }
        for outcome in ActOutcome::ALL {
            assert_eq!(ActOutcome::named(outcome.word()), Some(outcome));
        }
        assert_eq!(ActOutcome::named("Taken"), None);
    }

    #[test]
    fn an_act_asked_decided_and_read_crosses_the_wire_as_it_was() {
        use super::super::watch::{Position, Watched};
        use super::super::wire::{Answer, Request};
        let act = asked(&owner(), 7);
        let requests = [
            (Request::AskAct(Box::new(act.clone())), ("write", "ask_act")),
            (
                Request::DecideAct(7, ActOutcome::Taken, "paused".to_string()),
                ("write", "decide_act"),
            ),
            (
                Request::ReadActs(act.node.clone(), 3, 64, true),
                ("read", "read_acts"),
            ),
        ];
        for (request, operation) in requests {
            assert_eq!(request.operation(), operation);
            assert_eq!(
                Request::from_bytes(&request.bytes()).expect("read"),
                request
            );
        }
        for answer in [Answer::Act(Box::new(act.clone())), Answer::Acts(vec![act])] {
            assert_eq!(Answer::from_bytes(&answer.bytes()).expect("read"), answer);
        }
        for watched in [Watched::Acts(u128::MAX), Watched::Act(5)] {
            let position = Position { watched, at: 2 };
            assert_eq!(
                Position::from_bytes(&position.bytes()).expect("read"),
                position
            );
        }
    }

    #[test]
    fn its_times_are_xmip_storages_and_a_decision_keeps_when_it_was_asked() {
        let mut act = asked(&owner(), 7);
        act.stamp(10, None);
        assert_eq!((act.asked_unix_nanos, act.decided_unix_nanos), (10, 0));
        let before = act.clone();
        act.outcome = ActOutcome::Taken;
        act.stamp(20, Some(&before));
        assert_eq!((act.asked_unix_nanos, act.decided_unix_nanos), (10, 20));
        let decided = act.clone();
        act.stamp(30, Some(&decided));
        assert_eq!(act.decided_unix_nanos, 20, "decided once");
        let row = act.row();
        let open = row.iter().find(|(name, _)| *name == "open");
        assert_eq!(open, Some(&("open", Some(Value::Flag(false)))));
    }
}
