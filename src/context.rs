use std::{sync::Arc, collections::HashMap};

use serde::Serialize;
use tokio::sync::Mutex;

use crate::state_store::StateStore;

pub struct Context<TStore, T = ()> {
    key: String,
    original_state: Option<T>,
    sends: Vec<FutureDeliverableMessage>,
    state_store: Arc<Mutex<TStore>>,
}

#[derive(Clone)]
pub struct FutureDeliverableMessage {
    pub topic: String,
    pub key: String,
    pub payload: Vec<u8>,
    pub headers: HashMap<String, String>,
}

impl<TStore, T> Context<TStore, T>
where
    T: Clone,
    TStore: StateStore<T>
{
    pub fn new(key: &str, state: Option<T>, state_store: Arc<Mutex<TStore>>) -> Self {
        Self {
            key: key.to_string(),
            original_state: state,
            sends: vec![],
            state_store,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn state_store(&self) -> Arc<Mutex<TStore>> {
        self.state_store.clone()
    }

    pub fn get_state(&self) -> &Option<T> {
        &self.original_state
    }

    pub fn emit<M: Serialize>(&mut self, topic: &str, key: &str, msg: &M, headers: HashMap<String, String>) {
        // let output = self.deserializers.get(topic).unwrap();
        let data = serde_json::to_vec(msg).unwrap();
        self.sends.push(FutureDeliverableMessage {
            topic: topic.to_string(),
            key: key.to_string(),
            payload: data,
            headers,
        });
    }

    pub fn to_send(self) -> Vec<FutureDeliverableMessage> {
        self.sends
    }
}
