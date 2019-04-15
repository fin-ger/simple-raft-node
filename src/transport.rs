use raft::eraftpb::Message;
use crate::proposals::Proposal;

pub enum TransportItem {
    Proposal(Proposal),
    Message(Message),
}

/* TODO:
 *  - add transport trait
 */
