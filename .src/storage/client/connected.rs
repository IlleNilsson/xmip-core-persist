//! A request asked of one Storage node on a connection to it (`super`):
//! one kept from before where it is still good, or one opened afresh, each
//! held to what its Storage node is — a standby is not asked, nor a primary
//! of an epoch before the latest one met — and kept again once it answered.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use xcore::lock::Locked;

use super::super::wire::{Answer, Request};
use super::node::Node;
use super::{Asked, Connection, Exchange, StorageClient, exchange};

impl StorageClient {
    pub(super) fn ask_node(&self, node: &Node, request: &Request) -> Result<Answer, Asked> {
        self.ask_node_by(node, request, &exchange)
    }

    /// `request` asked of `node` by `exchanged`: on a connection kept from
    /// before, or on one opened afresh where that fails. A connection is kept
    /// again only once it answered, so none whose read failed or timed out
    /// is asked anything again.
    pub(super) fn ask_node_by(
        &self,
        node: &Node,
        request: &Request,
        exchanged: Exchange<'_>,
    ) -> Result<Answer, Asked> {
        let (answer, connection) = self.ask_node_held(node, request, exchanged)?;
        node.idle.locked().push(connection);
        standby(answer)
    }

    /// [`StorageClient::ask_node_by`]'s answer, and the connection it came
    /// on, kept by none yet: what a claim asked ahead holds for its second
    /// frame (`ahead`). One kept from a Storage node that last said it is a
    /// standby is asked what it is first, as a new one is, and a standby's
    /// is kept whichever it is: promoted, it serves on it at once, held to
    /// its epoch, where a new connection's handshake cost some 5 to 8 ms on
    /// Windows of every write's wait across its primary's stop in order
    /// (ADR-0056, amendment 2026-11-23).
    pub(super) fn ask_node_held(
        &self,
        node: &Node,
        request: &Request,
        exchanged: Exchange<'_>,
    ) -> Result<(Answer, Connection), Asked> {
        let kept = node.idle.locked().pop();
        let _asking = node.asking();
        if let Some(mut connection) = kept {
            let stands = if node.standby.load(Ordering::Relaxed) {
                self.standing(&mut connection)
            } else {
                Ok(())
            };
            match node.held(connection, stands) {
                Ok(mut connection) => {
                    if let Ok(answer) = exchanged(&mut connection, request) {
                        return Ok((answer, connection));
                    }
                }
                Err(Asked::Failed(_)) => {}
                Err(standby) => return Err(standby),
            }
        }
        let (connection, stands) = self.opened(node)?;
        let mut connection = node.held(connection, stands)?;
        let answer = exchanged(&mut connection, request)
            .map_err(|failure| Asked::Failed(failure.to_string()))?;
        Ok((answer, connection))
    }

    /// A new connection to `node`, and what it said it is
    /// ([`StorageClient::standing`]).
    ///
    /// # Errors
    ///
    /// Where it could not be reached.
    pub(super) fn opened(&self, node: &Node) -> Result<(Connection, Result<(), Asked>), Asked> {
        let failed = |failure: String| Asked::Failed(failure);
        let tcp = net::connect(node.address.as_str(), Some(self.timeout))
            .map_err(|failure| failed(failure.to_string()))?;
        let mut connection = tls::client_with(&node.host, tcp, Arc::clone(&self.config))
            .map_err(|failure| failed(failure.message))?;
        let stands = self.standing(&mut connection);
        Ok((connection, stands))
    }

    /// A new connection's Storage node held to what it is: a standby is
    /// not asked, nor a primary of an epoch before the latest one met.
    pub(super) fn standing(&self, connection: &mut Connection) -> Result<(), Asked> {
        let answer = exchange(connection, &Request::Standing)
            .map_err(|failure| Asked::Failed(failure.to_string()))?;
        let Answer::Standing(standing) = answer else {
            return Err(Asked::Failed(format!("answered {answer:?} to what it is")));
        };
        if !standing.primary {
            return Err(Asked::Standby(format!(
                "a standby Storage node, epoch {}, {}",
                standing.epoch,
                standing.bootstrap.said(standing.next)
            )));
        }
        let latest = self.epoch.fetch_max(standing.epoch, Ordering::SeqCst);
        if standing.epoch < latest {
            return Err(Asked::Failed(format!(
                "a primary of epoch {}, before the epoch {latest} met: it is not asked",
                standing.epoch
            )));
        }
        Ok(())
    }
}

/// An answer that says its Storage node is a standby is no answer.
pub(super) fn standby(answer: Answer) -> Result<Answer, Asked> {
    match answer {
        Answer::Standby(said) => Err(Asked::Standby(said)),
        answer => Ok(answer),
    }
}
