use serde::{Serialize, de::DeserializeOwned};

// TODO: maybe this should have a serialization API for saving applied data to disk
pub trait Machine: Send {
    type StateChange: Serialize + DeserializeOwned + Send;

    fn initialize<StringLike: Into<String>>(&mut self, name: StringLike);
    fn apply(&mut self, state_change: Self::StateChange);
}
