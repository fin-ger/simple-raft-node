use serde::{Serialize, Deserialize};
use raft::eraftpb::{Message, ConfChange};

use protobuf::Message as ProtobufMessage;

#[derive(Serialize, Deserialize)]
#[serde(remote = "Message")]
pub struct MessagePolyfill {
    #[serde(getter = "encode_message")]
    data: Vec<u8>,
}

fn encode_message(message: &Message) -> Vec<u8> {
    message.write_to_bytes().unwrap()
}

impl From<MessagePolyfill> for Message {
    fn from(polyfill: MessagePolyfill) -> Message {
        let mut result: Message = Default::default();
        result.merge_from_bytes(&polyfill.data).unwrap();
        result
    }
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "ConfChange")]
pub struct ConfChangePolyfill {
    #[serde(getter = "encode_conf_change")]
    data: Vec<u8>,
}

fn encode_conf_change(conf_change: &ConfChange) -> Vec<u8> {
    conf_change.write_to_bytes().unwrap()
}

impl From<ConfChangePolyfill> for ConfChange {
    fn from(polyfill: ConfChangePolyfill) -> ConfChange {
        let mut result: ConfChange = Default::default();
        result.merge_from_bytes(&polyfill.data).unwrap();
        result
    }
}
