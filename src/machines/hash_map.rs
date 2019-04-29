use std::collections::HashMap;

use serde::{Serialize, Deserialize, de::DeserializeOwned};

use crate::{
    Machine,
    MachineCore,
    MachineResult,
    MachineError,
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
    pub async fn put(&self, key: K, value: V) -> MachineResult<()> {
        if let Some(ref mngr) = self.mngr {
            return await!(mngr.apply(HashMapStateChange::Put(key, value)));
        }

        Err(MachineError::ChannelsUnavailable)
    }

    pub async fn delete(&self, key: K) -> MachineResult<()> {
        if let Some(ref mngr) = self.mngr {
            return await!(mngr.apply(HashMapStateChange::Delete(key)));
        }

        Err(MachineError::ChannelsUnavailable)
    }

    pub async fn get(&self, key: K) -> MachineResult<V> {
        if let Some(ref mngr) = self.mngr {
            return await!(mngr.retrieve(key));
        }

        Err(MachineError::ChannelsUnavailable)
    }
}

impl<K: Key + Serialize + DeserializeOwned, V: Value + Serialize + DeserializeOwned>
    Machine for HashMapMachine<K, V>
{
    type Core = HashMapMachineCore<K, V>;

    fn init(&mut self, mngr: RequestManager<Self::Core>) {
        self.mngr = Some(mngr);
    }

    fn core(&self) -> Self::Core {
        Default::default()
    }
}

impl<K: Key + Serialize + DeserializeOwned, V: Value + Serialize + DeserializeOwned>
    MachineCore for HashMapMachineCore<K, V>
{
    type StateChange = HashMapStateChange<K, V>;
    type StateIdentifier = K;
    type StateValue = V;

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
                self.hash_map.insert(key, value);
            },
            HashMapStateChange::Delete(key) => {
                self.hash_map.remove(&key);
            }
        }
    }

    fn retrieve(&self, state_identifier: &K) -> Result<&V, MachineError> {
        self.hash_map.get(state_identifier)
            .ok_or(MachineError::StateRetrieval)
    }
}
