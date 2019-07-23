use serde::{Serialize, de::DeserializeOwned};
use failure::{Fail, Backtrace};

use crate::{
    RequestManager,
    RequestKind,
    Response,
    GeneralResult,
    StateRetrievalResult,
    RequestResult,
    RequestError,
};

#[derive(Debug, Fail)]
pub enum MachineCoreError {
    #[fail(display = "Deserialization of machine core failed")]
    Deserialization,
    #[fail(display = "Serialization of machine core failed")]
    Serialization,
}

pub trait Machine: Send + Clone + std::fmt::Debug {
    type Core: MachineCore;

    fn init(&mut self, request_manager: RequestManager<Self::Core>);
    fn core(&self) -> Self::Core;
}

pub async fn apply<M: MachineCore>(
    request_manager: &RequestManager<M>,
    state_change: M::StateChange,
) -> RequestResult<()> {
    await!(request_manager.request(
        RequestKind::StateChange(state_change),
        |result| {
            match result {
                Response::StateChange(GeneralResult::Success) => Ok(()),
                _ => Err(RequestError::StateChange(Backtrace::new())),
            }
        },
    ))
}

pub async fn retrieve<M: MachineCore>(
    request_manager: &RequestManager<M>,
    state_identifier: M::StateIdentifier,
) -> RequestResult<M::StateValue> {
    await!(request_manager.request(
        RequestKind::StateRetrieval(state_identifier),
        |result| {
            match result {
                Response::StateRetrieval(StateRetrievalResult::Found(value)) => Ok(value),
                _ => Err(RequestError::StateRetrieval(Backtrace::new())),
            }
        },
    ))
}

pub async fn broadcast<M: MachineCore>(
    request_manager: &RequestManager<M>,
    data: Vec<u8>,
) -> RequestResult<()> {
    await!(request_manager.request(
        RequestKind::Broadcast(data),
        |_| Ok(()),
    ))
}

pub trait MachineItem = std::fmt::Debug + Clone + Send + Unpin;

// the core has to own all its data
pub trait MachineCore: std::fmt::Debug + Clone + Send +'static {
    type StateChange: MachineItem + Serialize + DeserializeOwned;
    type StateIdentifier: MachineItem;
    type StateValue: MachineItem;

    fn deserialize(&mut self, data: Vec<u8>) -> Result<(), MachineCoreError>;
    fn serialize(&self) -> Result<Vec<u8>, MachineCoreError>;
    fn apply(&mut self, state_change: Self::StateChange);
    fn retrieve(
        &self,
        state_identifier: Self::StateIdentifier,
    ) -> Result<Self::StateValue, RequestError>;
    fn broadcast(&self, _data: Vec<u8>) {
        // default to ignore
    }
}
