use std::collections::HashMap;

use serde::{Serialize, Deserialize, de::DeserializeOwned};

use crate::{Machine, MachineCore, MachineError, ProposalChannel};

pub trait Key =
    std::fmt::Debug
    + std::hash::Hash
    + std::cmp::Eq
    + Default
    + Serialize
    + DeserializeOwned
    + Send;

pub trait Value =
    std::fmt::Debug
    + Default
    + Serialize
    + DeserializeOwned
    + Clone
    + Send;

#[derive(Default)]
pub struct HashMapMachine<K: Key, V: Value> {
    channel: Option<ProposalChannel<HashMapMachineCore<K, V>>>,
}

#[derive(Default)]
pub struct HashMapMachineCore<K: Key, V: Value> {
    hash_map: HashMap<K, V>,
}

#[derive(Serialize, Deserialize)]
pub enum HashMapStateChange<K, V> {
    Put(K, V),
    Delete(K),
}

impl<K: Key, V: Value> HashMapMachine<K, V> {
    pub fn put(&mut self, key: K, value: V) -> Result<(), MachineError> {
        if let Some(ref mut channel) = self.channel {
            return channel.apply(HashMapStateChange::Put(key, value));
        }

        Err(MachineError::ChannelsUnavailable)
    }

    pub fn delete(&mut self, key: K) -> Result<(), MachineError> {
        if let Some(ref mut channel) = self.channel {
            return channel.apply(HashMapStateChange::Delete(key));
        }

        Err(MachineError::ChannelsUnavailable)
    }
}

impl<K: Key, V: Value> Machine for HashMapMachine<K, V> {
    type Core = HashMapMachineCore<K, V>;

    fn init(&mut self, proposal_channel: ProposalChannel<Self::Core>) {
        self.channel = Some(proposal_channel);
    }

    fn core(&self) -> Self::Core {
        Default::default()
    }
}

impl<K: Key, V: Value> MachineCore for HashMapMachineCore<K, V> {
    type StateChange = HashMapStateChange<K, V>;
    type StateIdentifier = K;
    type StateValue = V;

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

// TODO: remove this when debugging is done
impl<K: Key, V: Value> Drop for HashMapMachineCore<K, V> {
    fn drop(&mut self) {
        log::info!("{:#?}", self.hash_map);
    }
}
