pub mod encoders;
pub mod state_store;

use std::{collections::HashMap, marker::PhantomData, sync::Arc, time::Duration};

use dyn_clone::DynClone;
use encoders::Encoder;
use futures::{future::join_all, stream::SelectAll, Future, StreamExt};
use rdkafka::{
    consumer::{
        stream_consumer::StreamPartitionQueue, Consumer, DefaultConsumerContext, StreamConsumer,
    },
    producer::{DeliveryFuture, FutureProducer, FutureRecord},
    ClientConfig, Message,
};

use serde::{de::DeserializeOwned, Serialize};
use state_store::StateStore;

pub struct Context<T = (), S = ()> {
    key: String,
    origingal_state: Option<T>,
    new_state: Option<T>,
    producer: FutureProducer,
    sends: Vec<DeliveryFuture>,
    extra_state: S,
}

impl<T, S> Context<T, S>
where
    T: Clone,
    S: Clone,
{
    fn new(key: &str, state: Option<T>, producer: &FutureProducer, extra_state: S) -> Self {
        Self {
            key: key.to_string(),
            origingal_state: state,
            new_state: None,
            producer: producer.clone(),
            sends: vec![],
            extra_state,
        }
    }

    pub fn key(&self) -> String {
        self.key.clone()
    }

    pub fn get_state(&self) -> Option<T> {
        self.origingal_state.clone()
    }

    pub fn set_state(&mut self, state: Option<T>) {
        self.new_state = state
    }

    pub fn emit<M: Serialize>(&mut self, topic: &str, key: &str, msg: &M) {
        let data = serde_json::to_vec(msg).unwrap();
        let record = FutureRecord::to(topic).key(key).payload(&data);
        self.sends.push(self.producer.send_result(record).unwrap())
    }

    pub fn extra_state(&self) -> S {
        self.extra_state.clone()
    }
}

#[async_trait::async_trait]
pub trait GenericInput<T, S>: DynClone + Sync + Send {
    fn topic(&self) -> String;
    async fn handle(&self, ctx: &mut Context<T, S>, data: Option<&[u8]>);
}
dyn_clone::clone_trait_object!(<T, S> GenericInput<T, S>);

pub trait Handler<'a, T, S, R> {
    type Future: Future<Output = ()> + Send + 'a;

    fn call(self, ctx: &'a mut Context<T, S>, req: R) -> Self::Future;
}

impl<'a, Fut, T, S, R, F> Handler<'a, T, S, R> for F
where
    F: FnOnce(&'a mut Context<T, S>, R) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
    T: Send + 'static,
    S: Send + Sync + 'static,
{
    type Future = Fut;

    fn call(self, ctx: &'a mut Context<T, S>, req: R) -> Self::Future {
        (self)(ctx, req)
    }
}

#[derive(Clone)]
pub struct Input<T, S, F, E>
where
    E: Encoder<In = T>,
{
    topic: String,
    callback: F,
    encoder: E,
    _marker: PhantomData<S>,
}

impl<T, S, F, E> Input<T, S, F, E>
where
    E: Encoder<In = T>,
{
    pub fn new(topic: String, encoder: E, callback: F) -> Box<Self> {
        Box::new(Input {
            topic,
            callback,
            encoder,
            _marker: PhantomData,
        })
    }
}

#[async_trait::async_trait]
impl<R, T, S, F, E> GenericInput<T, S> for Input<R, S, F, E>
where
    for<'a> F: Handler<'a, T, S, R> + Send + Sync + Clone + Copy,
    E: Encoder<In = R> + Sync + Clone + Send + 'static,
    R: Sync + Send + Clone + DeserializeOwned + 'static + std::fmt::Debug,
    T: Clone + Send + Sync + 'static,
    S: Send + Sync + Clone,
{
    fn topic(&self) -> String {
        self.topic.clone()
    }

    async fn handle(&self, ctx: &mut Context<T, S>, data: Option<&[u8]>) {
        let msg = self.encoder.encode(data);
        println!("{:?}", msg);
        // let mut write = ctx.write().await;
        self.callback.call(ctx, msg.clone()).await;
        println!("{:?}", msg);
    }
}

pub struct Processor<TState, TExtraState, TStore, F1, F2> {
    pub config: ClientConfig,
    pub inputs: HashMap<String, Box<dyn GenericInput<TState, TExtraState>>>,
    pub stream_consumer: Arc<StreamConsumer>,
    state_store_gen: F1,
    extra_state_gen: F2,
    _marker: PhantomData<(TState, TStore)>,
}

impl<TState, TExtraState, TStore, F1, F2> Processor<TState, TExtraState, TStore, F1, F2>
where
    TState: Clone + Send + Sync + 'static,
    TStore: StateStore<String, TState> + Send + Sync,
    F1: FnOnce() -> TStore + Copy + Send + 'static,
    F2: FnOnce() -> TExtraState + Copy + Send + 'static,
    TExtraState: Clone + Send + 'static,
{
    pub fn new(
        config: ClientConfig,
        inputs: Vec<Box<dyn GenericInput<TState, TExtraState>>>,
        state_store_gen: F1,
        extra_state_gen: F2,
    ) -> Self {
        let stream_consumer: StreamConsumer = config.create().unwrap();
        let stream_consumer = Arc::new(stream_consumer);

        let inputs: HashMap<String, Box<dyn GenericInput<TState, TExtraState>>> = inputs
            .into_iter()
            .map(|input| (input.topic(), input))
            .collect();

        Processor {
            config,
            inputs,
            stream_consumer,
            state_store_gen,
            extra_state_gen,
            _marker: PhantomData,
        }
    }

    fn check_that_inputs_are_copartitioned(&self) -> Result<usize, ()> {
        let shit = self
            .stream_consumer
            .fetch_metadata(None, Duration::from_secs(1))
            .unwrap();

        let topics_partitions: Vec<(String, usize)> = shit
            .topics()
            .iter()
            .filter(|topic| self.inputs.contains_key(topic.name()))
            .map(|topic| (topic.name().to_string(), topic.partitions().len()))
            .collect();

        if topics_partitions.len() < 1 {
            Err(())
        } else {
            if topics_partitions
                .iter()
                .all(|(_, partitions)| partitions.eq(&topics_partitions[0].1))
            {
                Ok(topics_partitions[0].1)
            } else {
                Err(())
            }
        }
    }

    pub async fn run(&self) {
        let num_partitions = self.check_that_inputs_are_copartitioned().unwrap();

        let stream_consumer = self.stream_consumer.clone();

        for partition in 0..num_partitions {
            let partitioned_topics: Vec<(
                StreamPartitionQueue<DefaultConsumerContext>,
                Box<dyn GenericInput<TState, TExtraState>>,
            )> = self
                .inputs
                .iter()
                .map(|(topic_name, input_handler)| {
                    (
                        stream_consumer
                            .split_partition_queue(topic_name.as_ref(), partition as i32)
                            .unwrap(),
                        input_handler.clone(),
                    )
                })
                .collect();

            let gen = self.state_store_gen.clone();
            let gen2 = self.extra_state_gen.clone();
            let stream_consumer = self.stream_consumer.clone();
            let producer: FutureProducer = self.config.create().unwrap();

            tokio::spawn(async move {
                let mut state_store = gen();
                let extra_state = gen2();
                // let producer = stream_consumer
                let mut select_all = SelectAll::from_iter(
                    partitioned_topics
                        .iter()
                        .map(|(a, h)| a.stream().map(move |msg| (h, msg))),
                );

                while let Some((h, msg)) = select_all.next().await {
                    let msg = msg.unwrap();

                    //TODO: Key serializer?
                    let key = std::str::from_utf8(msg.key().unwrap()).unwrap();

                    println!(
                        "Received message from partition {:?}, topic {}, key {:?}",
                        partition,
                        msg.topic(),
                        msg.key(),
                    );

                    let state = state_store.get(key).await.map(|s| s.clone());
                    let mut ctx = Context::<TState, TExtraState>::new(
                        key,
                        state,
                        &producer,
                        extra_state.clone(),
                    );

                    h.handle(&mut ctx, msg.payload()).await;
                    if let Some(state) = &ctx.new_state {
                        state_store.set(key.to_string(), state.clone()).await;
                    }

                    let stream_consumer = stream_consumer.clone();
                    let msg_topic = msg.topic().to_string();
                    let msg_partition = msg.partition();
                    let msg_offset = msg.offset();

                    tokio::spawn(async move {
                        join_all(ctx.sends).await;
                        stream_consumer
                            .store_offset(&msg_topic, msg_partition, msg_offset)
                            .unwrap();
                    });
                }
            });
        }

        self.stream_consumer
            .subscribe(
                self.inputs
                    .iter()
                    .map(|(topic, _)| topic.as_ref())
                    .collect::<Vec<&str>>()
                    .as_slice(),
            )
            .unwrap();

        let stream_consumer = self.stream_consumer.clone();

        let task = tokio::spawn(async move {
            let message = stream_consumer.recv().await;
            panic!(
                "main stream consumer queue unexpectedly received message: {:?}",
                message
            );
        });

        task.await.unwrap();
    }
}
