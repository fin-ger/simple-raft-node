use std::collections::HashMap;

use failure::Backtrace;
use serde::{Serialize, Deserialize, de::DeserializeOwned};

use crate::{
    utils,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HashMapStateIdentifier<K> {
    Value(K),
    Values,
    Keys,
    Entries,
    Size,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HashMapStateValue<K, V> {
    Value(V),
    Values(Vec<V>),
    Keys(Vec<K>),
    Entries(Vec<(K, V)>),
    Size(usize),
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
            return await!(machine::retrieve(mngr, HashMapStateIdentifier::Value(key)))
                .and_then(|res| match res {
                    HashMapStateValue::Value(v) => Ok(v),
                    _ => Err(RequestError::StateRetrieval(Backtrace::new())),
                });
        }

        log::error!("machine was not initialized while get({:?}) was requested", key);

        Err(RequestError::NotInitialized(Backtrace::new()))
    }

    pub async fn keys(&self) -> RequestResult<Vec<K>> {
        if let Some(ref mngr) = self.mngr {
            log::trace!("retrieving keys() on machine...");
            return await!(machine::retrieve(mngr, HashMapStateIdentifier::Keys))
                .and_then(|res| match res {
                    HashMapStateValue::Keys(keys) => Ok(keys),
                    _ => Err(RequestError::StateRetrieval(Backtrace::new())),
                });
        }

        log::error!("machine was not initialized while keys() was requested");

        Err(RequestError::NotInitialized(Backtrace::new()))
    }

    pub async fn values(&self) -> RequestResult<Vec<V>> {
        if let Some(ref mngr) = self.mngr {
            log::trace!("retrieving values() on machine...");
            return await!(machine::retrieve(mngr, HashMapStateIdentifier::Values))
                .and_then(|res| match res {
                    HashMapStateValue::Values(values) => Ok(values),
                    _ => Err(RequestError::StateRetrieval(Backtrace::new())),
                });
        }

        log::error!("machine was not initialized while values() was requested");

        Err(RequestError::NotInitialized(Backtrace::new()))
    }

    pub async fn entries(&self) -> RequestResult<Vec<(K, V)>> {
        if let Some(ref mngr) = self.mngr {
            log::trace!("retrieving entries() on machine...");
            return await!(machine::retrieve(mngr, HashMapStateIdentifier::Entries))
                .and_then(|res| match res {
                    HashMapStateValue::Entries(entries) => Ok(entries),
                    _ => Err(RequestError::StateRetrieval(Backtrace::new())),
                });
        }

        log::error!("machine was not initialized while entries() was requested");

        Err(RequestError::NotInitialized(Backtrace::new()))
    }

    pub async fn size(&self) -> RequestResult<usize> {
        if let Some(ref mngr) = self.mngr {
            log::trace!("retrieving size() on machine...");
            return await!(machine::retrieve(mngr, HashMapStateIdentifier::Size))
                .and_then(|res| match res {
                    HashMapStateValue::Size(size) => Ok(size),
                    _ => Err(RequestError::StateRetrieval(Backtrace::new())),
                });
        }

        log::error!("machine was not initialized while size() was requested");

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
    type StateIdentifier = HashMapStateIdentifier<K>;
    type StateValue = HashMapStateValue<K, V>;

    // TODO: replace this with serde traits
    fn deserialize(&mut self, data: Vec<u8>) -> Result<(), MachineCoreError> {
        *self = utils::deserialize(&data[..]).map_err(|_| MachineCoreError::Deserialization)?;
        Ok(())
    }

    fn serialize(&self) -> Result<Vec<u8>, MachineCoreError> {
        utils::serialize(self).map_err(|_| MachineCoreError::Serialization)
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

    fn retrieve(&self, state_identifier: HashMapStateIdentifier<K>) -> Result<HashMapStateValue<K, V>, RequestError> {
        Ok(match state_identifier {
            HashMapStateIdentifier::Value(key) => {
                let value = self.hash_map.get(&key)
                    .ok_or(RequestError::StateRetrieval(Backtrace::new()))?;
                HashMapStateValue::Value(value.clone())
            },
            HashMapStateIdentifier::Keys => {
                let keys = self.hash_map.keys().map(|k| k.clone()).collect::<Vec<_>>();
                HashMapStateValue::Keys(keys)
            },
            HashMapStateIdentifier::Values => {
                let values = self.hash_map.values().map(|v| v.clone()).collect::<Vec<_>>();
                HashMapStateValue::Values(values)
            },
            HashMapStateIdentifier::Entries => {
                let entries = self.hash_map.iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<Vec<(_, _)>>();
                HashMapStateValue::Entries(entries)
            },
            HashMapStateIdentifier::Size => {
                let size = self.hash_map.len();
                HashMapStateValue::Size(size)
            },
        })
    }
}
