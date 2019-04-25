use std::sync::mpsc::{Sender, Receiver};

use serde::{Serialize, de::DeserializeOwned};
use failure::Fail;

use crate::{Proposal, Answer, AnswerResult, ChangeResult, RetrievalResult};

#[derive(Debug, Fail)]
pub enum MachineError {
    #[fail(display = "The proposal channels are not available")]
    ChannelsUnavailable,
    #[fail(display = "The proposed state change failed")]
    StateChange,
    #[fail(display = "The proposed state retrieval failed")]
    StateRetrieval,
}

pub struct ProposalChannel<M: MachineCore> {
    tx: Sender<Proposal<M>>,
    rx: Receiver<Answer<M>>,
}

// TODO: make apply non-blocking, so that more than 1 proposal can be processed per tick (100ms)
impl<M: MachineCore> ProposalChannel<M> {
    pub fn new(tx: Sender<Proposal<M>>, rx: Receiver<Answer<M>>) -> Self {
        Self {
            tx,
            rx,
        }
    }

    pub fn apply(&mut self, state_change: M::StateChange) -> Result<(), MachineError> {
        // FIXME: as state changes are synchronous for now, we can use a
        //        constant proposal id. This will need management when
        //        answer id's get evaluated properly in propose.
        let proposal = Proposal::state_change(42, state_change);
        let id = proposal.id();
        self.tx.send(proposal).map_err(|_| MachineError::ChannelsUnavailable)?;
        let answer = self.rx.recv().map_err(|_| MachineError::ChannelsUnavailable)?;

        if answer.id != id {
            log::error!("Proposal id not identical to answer id!");
            // FIXME: answer can still be in a later message, as this whole thing here
            //        might be async
            return Err(MachineError::StateChange);
        }

        match answer.result {
            AnswerResult::Change(ChangeResult::Successful) => Ok(()),
            _ => Err(MachineError::StateChange),
        }
    }

    pub fn retrieve(
        &mut self,
        state_identifier: M::StateIdentifier,
    ) -> Result<RetrievalResult<M>, MachineError> {
        let proposal = Proposal::state_retrieval(42, state_identifier);
        let id = proposal.id();
        self.tx.send(proposal).map_err(|_| MachineError::ChannelsUnavailable)?;
        let answer = self.rx.recv().map_err(|_| MachineError::ChannelsUnavailable)?;

        if answer.id != id {
            log::error!("Proposal id not identical to answer id!");
            // FIXME: answer can still be in a later message, as this whole thing here
            //        might be async
            return Err(MachineError::StateRetrieval);
        }

        match answer.result {
            AnswerResult::Retrieval(value) => Ok(value),
            _ => Err(MachineError::StateRetrieval),
        }
    }
}

// TODO: add init from storage
pub trait Machine: Send {
    type Core: MachineCore;

    fn init(&mut self, proposal_channel: ProposalChannel<Self::Core>);
    fn core(&self) -> Self::Core;
}

pub trait MachineCore: Send {
    type StateChange: Serialize + DeserializeOwned + Send;
    type StateIdentifier: Serialize + DeserializeOwned + Send;
    type StateValue: Serialize + DeserializeOwned + Clone + Send;

    fn apply(&mut self, state_change: Self::StateChange);
    fn retrieve(
        &self,
        state_identifier: &Self::StateIdentifier,
    ) -> Result<&Self::StateValue, MachineError>;
}
