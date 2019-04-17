use std::collections::HashMap;

use serde::{Serialize, Deserialize, de::DeserializeOwned};

use crate::Machine;

pub trait Key =
    std::fmt::Debug
    + std::hash::Hash
    + std::cmp::Eq
    + Default
    + Serialize
    + DeserializeOwned + Send;

pub trait Value =
    std::fmt::Debug
    + Default
    + Serialize
    + DeserializeOwned
    + Send;

#[derive(Default, Debug)]
pub struct HashMapMachine<K: Key, V: Value> {
    name: String,
    hash_map: HashMap<K, V>,
}

#[derive(Serialize, Deserialize)]
pub struct HashMapStateChange<K, V> {
    pub key: K,
    pub value: V,
}

impl<K: Key, V: Value> Machine for HashMapMachine<K, V> {
    type StateChange = HashMapStateChange<K, V>;

    fn initialize<StringLike: Into<String>>(&mut self, name: StringLike) {
        self.name = name.into();
    }

    fn apply(&mut self, state_change: HashMapStateChange<K, V>) {
        self.hash_map.insert(state_change.key, state_change.value);
    }
}

// TODO: remove this when debugging is done
impl<K: Key, V: Value> Drop for HashMapMachine<K, V> {
    fn drop(&mut self) {
        log::info!("{}: {:#?}", self.name, self.hash_map);
    }
}
