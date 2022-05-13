use std::{collections::HashMap, marker::PhantomData};

use futures::{
    channel::mpsc::{SendError, UnboundedReceiver, UnboundedSender},
    stream::SelectAll,
    SinkExt, StreamExt,
};
use rdkafka::{message::OwnedMessage, Message};

use crate::{context::Context, input::GenericInput, state_store::StateStore};

pub struct TestProcessor<TState, TExtraState, TStore, F1, F2> {
    pub inputs: HashMap<
        String,
        (
            Box<dyn GenericInput<TState, TExtraState>>,
            Option<UnboundedReceiver<OwnedMessage>>,
        ),
    >,
    // pub input_channels: HashMap<String, UnboundedReceiver<Vec<u8>>>,
    state_store_gen: F1,
    extra_state_gen: F2,
    _marker: PhantomData<(TState, TStore)>,
}

impl<TState, TExtraState, TStore, F1, F2> TestProcessor<TState, TExtraState, TStore, F1, F2>
where
    TState: Clone + Send + Sync + 'static,
    TStore: StateStore<String, TState> + Send + Sync,
    F1: FnOnce() -> TStore + Copy + Send + 'static,
    F2: FnOnce() -> TExtraState + Copy + Send + 'static,
    TExtraState: Send + 'static,
{
    pub fn new(
        inputs: Vec<Box<dyn GenericInput<TState, TExtraState>>>,
        state_store_gen: F1,
        extra_state_gen: F2,
    ) -> Self {
        let inputs = inputs
            .into_iter()
            .map(|input| (input.topic(), (input, None)))
            .collect();

        TestProcessor {
            inputs,
            state_store_gen,
            extra_state_gen,
            _marker: PhantomData,
        }
    }

    pub fn input<F, T>(&mut self, topic: String, decoder: F) -> Sender<T, F>
    where
        F: FnOnce(&T) -> Vec<u8> + Copy,
    {
        let (tx, rx) = futures::channel::mpsc::unbounded();
        self.inputs.entry(topic).and_modify(|s| s.1 = Some(rx));
        Sender::new(decoder, tx)
    }

    pub async fn run(self) {
        let gen = self.state_store_gen.clone();
        let gen2 = self.extra_state_gen.clone();

        // tokio::spawn(async move {
        let mut state_store = gen();
        let mut extra_state = gen2();

        let mut select_all = SelectAll::from_iter(
            self.inputs
                .into_iter()
                .filter_map(|(_topic, h)| match h.1 {
                    Some(rx) => Some((h.0, rx)),
                    None => None,
                })
                .map(|(handler, receiver)| receiver.map(move |msg| (handler.clone(), msg))),
        );

        while let Some((h, msg)) = select_all.next().await {
            //TODO: Key serializer?

            let key = std::str::from_utf8(msg.key().unwrap()).unwrap();

            let state = state_store.get(key).await.map(|s| s.clone());
            let mut ctx = Context::new(key, state);

            h.handle(&mut extra_state, &mut ctx, msg.payload()).await;
            if let Some(state) = &ctx.get_state() {
                state_store.set(key.to_string(), state.clone()).await;
            }
        }
    }
}

pub struct Sender<T, F>
where
    F: FnOnce(&T) -> Vec<u8> + Copy,
{
    decoder: F,
    tx: UnboundedSender<OwnedMessage>,
    _marker: PhantomData<T>,
}

impl<T, F> Sender<T, F>
where
    F: FnOnce(&T) -> Vec<u8> + Copy,
{
    fn new(decoder: F, tx: UnboundedSender<OwnedMessage>) -> Self {
        Self {
            decoder,
            tx,
            _marker: PhantomData,
        }
    }

    pub async fn send(&mut self, key: String, msg: &T) -> Result<(), SendError> {
        let data = (self.decoder)(msg);
        let msg = OwnedMessage::new(
            Some(data),
            Some(key.as_bytes().to_vec()),
            "".to_string(),
            rdkafka::Timestamp::NotAvailable,
            0,
            0,
            None,
        );
        self.tx.send(msg).await
    }
}
