use std::collections::HashMap;

use failure::Backtrace;
use serde::{Serialize, Deserialize, de::DeserializeOwned};

use crate::{
    machine,
    Machine,
    MachineCore,
    RequestResult,
    RequestError,
    MachineCoreError,
    RequestManager,
};

pub trait Key =
    std::fmt::Debug
    + std::hash::Hash
    + std::cmp::Eq
    + Clone
    + Send
    + Unpin
    + Default
    + 'static;

pub trait Value =
    std::fmt::Debug
    + Clone
    + Send
    + Unpin
    + Default
    + 'static;

#[derive(Debug, Clone, Default)]
pub struct
    HashMapMachine<K: Key + Serialize + DeserializeOwned, V: Value + Serialize + DeserializeOwned>
{
    mngr: Option<RequestManager<HashMapMachineCore<K, V>>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HashMapMachineCore<K: Key, V: Value> {
    hash_map: HashMap<K, V>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HashMapStateChange<K, V> {
    Put(K, V),
    Delete(K),
}

impl<K: Key + Serialize + DeserializeOwned, V: Value + Serialize + DeserializeOwned>
    HashMapMachine<K, V>
{
    pub fn new() -> Self {
        Default::default()
    }

    pub async fn put(&self, key: K, value: V) -> RequestResult<()> {
        if let Some(ref mngr) = self.mngr {
            log::trace!("applying put({:?}, {:?}) to machine...", key, value);
            return await!(machine::apply(mngr, HashMapStateChange::Put(key, value)));
        }

        log::error!("machine was not initialized while put({:?}, {:?}) was requested", key, value);

        Err(RequestError::NotInitialized(Backtrace::new()))
    }

    pub async fn delete(&self, key: K) -> RequestResult<()> {
        if let Some(ref mngr) = self.mngr {
            log::trace!("applying delete({:?}) to machine...", key);
            return await!(machine::apply(mngr, HashMapStateChange::Delete(key)));
        }

        log::error!("machine was not initialized while delete({:?}) was requested", key);

        Err(RequestError::NotInitialized(Backtrace::new()))
    }

    pub async fn get(&self, key: K) -> RequestResult<V> {
        if let Some(ref mngr) = self.mngr {
            log::trace!("retrieving get({:?}) on machine...", key);
            return await!(machine::retrieve(mngr, key));
        }

        log::error!("machine was not initialized while get({:?}) was requested", key);

        Err(RequestError::NotInitialized(Backtrace::new()))
    }
}

impl<K: Key + Serialize + DeserializeOwned, V: Value + Serialize + DeserializeOwned>
    Machine for HashMapMachine<K, V>
{
    type Core = HashMapMachineCore<K, V>;

    fn init(&mut self, mngr: RequestManager<Self::Core>) {
        log::debug!("initializing machine with request manager");
        self.mngr = Some(mngr);
    }

    fn core(&self) -> Self::Core {
        log::debug!("core requested on machine");
        Default::default()
    }
}

impl<K: Key + Serialize + DeserializeOwned, V: Value + Serialize + DeserializeOwned>
    MachineCore for HashMapMachineCore<K, V>
{
    type StateChange = HashMapStateChange<K, V>;
    type StateIdentifier = K;
    type StateValue = V;

    // TODO: replace this with serde traits
    fn deserialize(&mut self, data: Vec<u8>) -> Result<(), MachineCoreError> {
        *self = bincode::deserialize(&data[..]).map_err(|_| MachineCoreError::Deserialization)?;
        Ok(())
    }

    fn serialize(&self) -> Result<Vec<u8>, MachineCoreError> {
        bincode::serialize(self).map_err(|_| MachineCoreError::Serialization)
    }

    fn apply(&mut self, state_change: HashMapStateChange<K, V>) {
        match state_change {
            HashMapStateChange::Put(key, value) => {
                log::trace!("state change put({:?}, {:?}) is applied on machine", key, value);
                self.hash_map.insert(key, value);
            },
            HashMapStateChange::Delete(key) => {
                log::trace!("state change delete({:?}) is applied on machine", key);
                self.hash_map.remove(&key);
            }
        }
    }

    fn retrieve(&self, state_identifier: &K) -> Result<&V, RequestError> {
        let value = self.hash_map.get(state_identifier)
            .ok_or(RequestError::StateRetrieval(Backtrace::new()));

        if value.is_err() {
            log::error!("could not retrieve {:?} from machine", state_identifier);
        } else {
            log::trace!("{:?} is retrieved from machine", state_identifier);
        }

        value
    }
}
