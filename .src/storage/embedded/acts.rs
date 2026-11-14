//! The embedded Storage node's operator acts: the administration
//! database's `operator_act` table (`super::super::act`), each act written
//! by the administration database's one writer, which places it among the
//! acts asked of its node and tells every node waiting on them as it
//! writes, and decides it once, telling whoever waits on its decision.
use codec::form::Form;
use xcore::lock::Locked;

use super::super::act::{ACT, ActOutcome, ActRecord, BY_NODE, STILL_ASKED};
use super::super::record::malformed;
use super::super::row::Value;
use super::super::watch::{Change, Watched};
use super::Embedded;
use crate::{Engine, PersistError};

impl<R: Engine + 'static, A: Engine> Embedded<R, A> {
    /// `act` written asked, at the next place among the acts asked of its
    /// node, and every waiter on them told — or, asked again by its
    /// identifier, the act as kept, nothing written: `ask_act`.
    pub(super) fn act_asked(&self, act: &ActRecord) -> Result<ActRecord, PersistError> {
        let _one = self.administering.locked();
        if let Some(kept) = self.act_by_id(act.act)? {
            return Ok(kept);
        }
        let node = ActRecord::of_node(&act.node);
        let place = self.acts_stand(node)? + 1;
        let asked = ActRecord {
            place,
            outcome: ActOutcome::Asked,
            said: String::new(),
            ..act.clone()
        };
        self.act_written(&asked)?;
        // Told under the writer's lock, in the order written, so what was
        // told last is where the acts asked of the node stand.
        self.watch.moved(&[Change {
            watched: Watched::Acts(node),
            at: Some(place),
            own: None,
        }]);
        self.act_kept(act.act)
    }

    /// `act` decided `outcome`, saying `said`, where it is still asked, and
    /// every waiter on it told; as it stands where it was decided before:
    /// `decide_act`.
    pub(super) fn act_decided(
        &self,
        act: u128,
        (outcome, said): (ActOutcome, &str),
    ) -> Result<ActRecord, PersistError> {
        if outcome == ActOutcome::Asked {
            return Err(malformed("an act is decided taken, refused or failed"));
        }
        let _one = self.administering.locked();
        let kept = self.act_kept(act)?;
        if !kept.is_asked() {
            return Ok(kept);
        }
        let decided = ActRecord {
            outcome,
            said: said.to_string(),
            ..kept
        };
        self.act_written(&decided)?;
        self.watch.moved(&[Change {
            watched: Watched::Act(act),
            at: Some(1),
            own: None,
        }]);
        self.act_kept(act)
    }

    /// Up to `most` of the acts asked of `node` after the place `after`,
    /// oldest first, or of those only that are still asked: `read_acts`.
    pub(super) fn acts_read(
        &self,
        node: &str,
        (after, most): (u64, u32),
        open: bool,
    ) -> Result<Vec<ActRecord>, PersistError> {
        let index = if open { STILL_ASKED } else { BY_NODE };
        let equal = [Value::Id(ActRecord::of_node(node))];
        let from = after.saturating_add(1);
        let found = self.administration_columns.find(
            &self.administration,
            index,
            &equal,
            Some((from, u64::MAX)),
            (most, false),
        )?;
        found
            .into_iter()
            .filter_map(|id| self.act_by_id(id).transpose())
            .collect()
    }

    /// Where the acts asked of the node `node` names stand: the place of
    /// the last, none where none was asked.
    pub(super) fn acts_stand(&self, node: u128) -> Result<u64, PersistError> {
        let last = self.administration_columns.find(
            &self.administration,
            BY_NODE,
            &[Value::Id(node)],
            None,
            (1, true),
        )?;
        let Some(id) = last.first() else {
            return Ok(0);
        };
        Ok(self.act_by_id(*id)?.map_or(0, |act| act.place))
    }

    /// Where the act `act` stands: one once decided, none while asked or
    /// where it was never asked.
    pub(super) fn act_stands(&self, act: u128) -> Result<u64, PersistError> {
        Ok(u64::from(
            self.act_by_id(act)?.is_some_and(|act| !act.is_asked()),
        ))
    }

    fn act_written(&self, act: &ActRecord) -> Result<(), PersistError> {
        let tables = (&self.administration, &self.administration_columns);
        let change = (ACT, act.act.to_be_bytes().to_vec(), Some(act.bytes()));
        self.indexed(tables, vec![change])
    }

    fn act_kept(&self, act: u128) -> Result<ActRecord, PersistError> {
        self.act_by_id(act)?
            .ok_or_else(|| malformed(format!("no act {act:032x} was asked")))
    }

    fn act_by_id(&self, act: u128) -> Result<Option<ActRecord>, PersistError> {
        self.administration
            .get(ACT, &act.to_be_bytes())?
            .map(|bytes| ActRecord::from_bytes(&bytes))
            .transpose()
            .map_err(PersistError::from)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use secret::{Held, KekName};
    use xcore::SystemClock;

    use super::*;
    use crate::EncryptedStore;
    use crate::fixture::Memory;
    use crate::storage::{Position, Waiter, XmipStorage};

    fn node() -> Embedded<Memory, Memory> {
        let keys = Held::new(secret::fixture::Memory::default());
        let kek = KekName::new("storage").expect("name");
        Embedded::over(
            EncryptedStore::open(Memory::default(), &keys, &kek).expect("runtime"),
            EncryptedStore::open(Memory::default(), &keys, &kek).expect("administration"),
            EncryptedStore::open(Memory::default(), &keys, &kek).expect("audit"),
            Arc::new(SystemClock),
        )
        .expect("node")
    }

    fn scope(name: &str) -> String {
        format!(
            "{}-{name}",
            configure::fixture::test_cluster().node_scope(0)
        )
    }

    fn asked(node: &str, act: u128, word: &str) -> ActRecord {
        ActRecord {
            act,
            node: node.to_string(),
            noun: "subscription".to_string(),
            target: "Orders".to_string(),
            word: word.to_string(),
            who: "CN=alice".to_string(),
            proven: "mutual-tls".to_string(),
            role: "Operator".to_string(),
            through: None,
            door: scope("door"),
            ..ActRecord::default()
        }
    }

    #[test]
    fn acts_take_their_nodes_places_in_order_and_asked_again_write_nothing() {
        let storage = node();
        let (owner, other) = (scope("owner"), scope("other"));
        let first = storage.ask_act(&asked(&owner, 11, "pause")).expect("asked");
        let elsewhere = storage.ask_act(&asked(&other, 12, "pause")).expect("asked");
        let second = storage
            .ask_act(&asked(&owner, 13, "resume"))
            .expect("asked");
        assert_eq!((first.place, elsewhere.place, second.place), (1, 1, 2));
        assert!(first.asked_unix_nanos > 0, "Xmip Storage's time");
        assert_eq!(first.outcome, ActOutcome::Asked);

        let again = storage
            .ask_act(&asked(&owner, 11, "resume"))
            .expect("asked again");
        assert_eq!(again, first, "asked again after a lost answer");
        let read = storage.read_acts(&owner, (0, 64), false).expect("read");
        let words: Vec<&str> = read.iter().map(|act| act.word.as_str()).collect();
        assert_eq!(words, ["pause", "resume"]);
        let after = storage.read_acts(&owner, (1, 64), false).expect("read");
        assert_eq!(after, [second]);
        let others = storage.read_acts(&other, (0, 64), false).expect("read");
        assert_eq!(others, [elsewhere], "each node's own");
        assert_eq!(storage.act_by_id(99).expect("read"), None);
    }

    #[test]
    fn an_act_is_decided_once_and_no_longer_listed_as_asked() {
        let storage = node();
        let owner = scope("decided");
        storage.ask_act(&asked(&owner, 21, "pause")).expect("asked");
        storage
            .ask_act(&asked(&owner, 22, "resume"))
            .expect("asked");
        let taken = storage
            .decide_act(21, (ActOutcome::Taken, "paused"))
            .expect("decided");
        assert_eq!(
            (taken.outcome, taken.said.as_str()),
            (ActOutcome::Taken, "paused")
        );
        assert!(taken.decided_unix_nanos >= taken.asked_unix_nanos);
        let twice = storage
            .decide_act(21, (ActOutcome::Refused, "REFUSED: again"))
            .expect("as it stands");
        assert_eq!(twice, taken, "a second decision is refused");
        let open = storage.read_acts(&owner, (0, 64), true).expect("read");
        let open: Vec<u128> = open.iter().map(|act| act.act).collect();
        assert_eq!(open, [22], "only what is still asked");
        assert!(
            storage.decide_act(21, (ActOutcome::Asked, "")).is_err(),
            "asked is no decision"
        );
        assert!(storage.decide_act(99, (ActOutcome::Taken, "")).is_err());
        assert_eq!(storage.act_stands(21).expect("read"), 1);
        assert_eq!(storage.act_stands(22).expect("read"), 0);
        let node = ActRecord::of_node(&owner);
        assert_eq!(storage.acts_stand(node).expect("read"), 2);
    }

    /// `watched` waited on from where it stood at nothing, on a thread,
    /// listed before this returns.
    fn waiting(
        storage: &Arc<Embedded<Memory, Memory>>,
        watched: Watched,
    ) -> std::thread::JoinHandle<Vec<crate::storage::Moved>> {
        let (held, waiter) = (Arc::clone(storage), Arc::new(Waiter::new()));
        let seen = [Position { watched, at: 0 }];
        let waiting = std::thread::spawn(move || {
            held.wait_queues(&seen, (Duration::from_secs(30), 0), &waiter)
                .expect("answered")
        });
        let deadline = Instant::now() + Duration::from_secs(30);
        while storage.waiting_on(watched) == 0 {
            assert!(Instant::now() < deadline, "never listed");
            std::thread::yield_now();
        }
        waiting
    }

    #[test]
    fn an_act_asked_wakes_its_nodes_watch_and_its_decision_wakes_its_door() {
        let storage = Arc::new(node());
        let owner = scope("woken");
        let acts = Watched::Acts(ActRecord::of_node(&owner));
        let watch = waiting(&storage, acts);
        let other = waiting(&storage, Watched::Acts(ActRecord::of_node(&scope("not"))));
        storage.ask_act(&asked(&owner, 31, "pause")).expect("asked");
        let moved = watch.join().expect("woken");
        assert_eq!(moved.len(), 1);
        assert_eq!((moved[0].position.watched, moved[0].position.at), (acts, 1));

        let door = waiting(&storage, Watched::Act(31));
        storage
            .decide_act(31, (ActOutcome::Taken, "paused"))
            .expect("decided");
        let moved = door.join().expect("woken");
        assert_eq!(moved[0].position.at, 1, "decided");
        // A wait on a node no act was asked of is still waiting.
        assert!(!other.is_finished());
        storage
            .ask_act(&asked(&scope("not"), 32, "pause"))
            .expect("asked");
        assert_eq!(other.join().expect("woken")[0].position.at, 1);
    }
}
