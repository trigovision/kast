use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use futures::{
    channel::mpsc::{SendError, UnboundedReceiver, UnboundedSender},
    stream::SelectAll,
    SinkExt, StreamExt,
};
use rdkafka::{message::OwnedMessage, Message};
use serde::Serialize;
use tokio::sync::RwLock;

use crate::{
    context::Context,
    encoders::{Decoder, Encoder},
    input::GenericInput,
    output::Output,
    state_store::StateStore,
};

pub struct TestProcessor<TState, TExtraState, TStore, F1, F2> {
    pub inputs: HashMap<
        String,
        (
            Box<dyn GenericInput<TState, TExtraState>>,
            (
                UnboundedSender<OwnedMessage>,
                UnboundedReceiver<OwnedMessage>,
            ),
        ),
    >,
    outputs: HashMap<
        String,
        (
            Output,
            (
                UnboundedSender<OwnedMessage>,
                Arc<RwLock<UnboundedReceiver<OwnedMessage>>>,
            ),
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
    // T: Seri
{
    pub fn new(
        inputs: Vec<Box<dyn GenericInput<TState, TExtraState>>>,
        outputs: Vec<Output>,
        state_store_gen: F1,
        extra_state_gen: F2,
    ) -> Self {
        let inputs = inputs
            .into_iter()
            .map(|input| (input.topic(), (input, futures::channel::mpsc::unbounded())))
            .collect();

        let outputs = outputs
            .into_iter()
            .map(|out| {
                let (tx, rx) = futures::channel::mpsc::unbounded();
                (
                    out.topic().to_string(),
                    (out, (tx, Arc::new(RwLock::new(rx)))),
                )
            })
            .collect();

        TestProcessor {
            inputs,
            outputs,
            state_store_gen,
            extra_state_gen,
            _marker: PhantomData,
        }
    }

    pub fn input<F, T>(&mut self, topic: String, decoder: F) -> Sender<T, F>
    where
        F: Decoder,
        T: Serialize,
    {
        Sender::new(decoder, self.inputs.get(&topic).unwrap().1 .0.clone())
    }

    pub fn output<E, T>(&mut self, topic: String, encoder: E) -> Receiver<T, E>
    where
        E: Encoder<In = T>,
    {
        Receiver::new(encoder, self.outputs.get(&topic).unwrap().1 .1.clone())
    }

    pub async fn run(self) {
        let gen = self.state_store_gen.clone();
        let gen2 = self.extra_state_gen.clone();

        // tokio::spawn(async move {
        let mut state_store = gen();
        let mut extra_state = gen2();

        let tx_hashmap_from_out_to_in: HashMap<String, UnboundedSender<OwnedMessage>> = self
            .inputs
            .iter()
            .map(|(t, (_a, (tx, _rx)))| (t.to_string(), tx.clone()))
            .collect();

        let mut select_all = SelectAll::from_iter(
            self.inputs
                .into_iter()
                .map(|(_topic, h)| (h.0, h.1 .1))
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

            let producer: HashMap<String, UnboundedSender<OwnedMessage>> = self
                .outputs
                .iter()
                .map(|(t, (_o, (tx, _rx)))| (t.to_string(), tx.clone()))
                .collect();

            // println!("@ {}", sends.len());
            for s in ctx.to_send() {
                let msg = OwnedMessage::new(
                    Some(s.payload),
                    Some(s.key.as_bytes().to_vec()),
                    s.topic.clone(),
                    rdkafka::Timestamp::NotAvailable,
                    0,
                    0,
                    None,
                );

                if let Some(tx) = tx_hashmap_from_out_to_in.get(&s.topic) {
                    tx.unbounded_send(msg.clone()).unwrap();
                }

                producer.get(&s.topic).unwrap().unbounded_send(msg).unwrap();
            }
        }
    }
}

//Should we use with flat map on channels?
pub struct Sender<T, F>
where
    F: Decoder,
{
    decoder: F,
    tx: UnboundedSender<OwnedMessage>,
    _marker: PhantomData<T>,
}

impl<T, F> Sender<T, F>
where
    F: Decoder,
    T: Serialize,
{
    fn new(decoder: F, tx: UnboundedSender<OwnedMessage>) -> Self {
        Self {
            decoder,
            tx,
            _marker: PhantomData,
        }
    }

    pub async fn send(&mut self, key: String, msg: &T) -> Result<(), SendError> {
        let data = self.decoder.decode(msg);
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

pub struct Receiver<T, E>
where
    E: Encoder<In = T>,
{
    encoder: E,
    rx: Arc<RwLock<UnboundedReceiver<OwnedMessage>>>,
}

impl<T, E> Receiver<T, E>
where
    E: Encoder<In = T>,
{
    fn new(encoder: E, rx: Arc<RwLock<UnboundedReceiver<OwnedMessage>>>) -> Self {
        Self { encoder, rx }
    }

    pub async fn recv(&mut self) -> Option<T> {
        let data = self.rx.write().await.next().await;
        data.map(|d| self.encoder.encode(d.payload()))
    }
}
