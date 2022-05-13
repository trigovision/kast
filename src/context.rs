use serde::Serialize;

pub struct Context<T = ()> {
    key: String,
    origingal_state: Option<T>,
    new_state: Option<T>,
    sends: Vec<FutureDeliverableMessage>,
}

pub struct FutureDeliverableMessage {
    pub topic: String,
    pub key: String,
    pub payload: Vec<u8>,
}

impl<T> Context<T>
where
    T: Clone,
{
    pub fn new(key: &str, state: Option<T>) -> Self {
        Self {
            key: key.to_string(),
            origingal_state: state,
            new_state: None,
            sends: vec![],
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn get_state(&self) -> &Option<T> {
        &self.origingal_state
    }

    pub fn get_new_state(&self) -> &Option<T> {
        &self.origingal_state
    }

    pub fn set_state(&mut self, state: Option<T>) {
        self.new_state = state
    }

    pub fn emit<M: Serialize>(&mut self, topic: &str, key: &str, msg: &M) {
        let data = serde_json::to_vec(msg).unwrap();
        self.sends.push(FutureDeliverableMessage {
            topic: topic.to_string(),
            key: key.to_string(),
            payload: data,
        })
    }

    pub fn to_send(self) -> Vec<FutureDeliverableMessage> {
        self.sends
    }
}
