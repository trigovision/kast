use std::{collections::HashMap, marker::PhantomData, sync::Arc, time::Duration};

use futures::{stream::SelectAll, StreamExt};
use rdkafka::{
    consumer::{
        stream_consumer::StreamPartitionQueue, Consumer, DefaultConsumerContext, StreamConsumer,
    },
    ClientConfig, Message,
};
use serde::de::DeserializeOwned;

//TODO: Should context be a trait?
pub struct Context<T> {
    origingal_state: Option<T>,
    new_state: Option<T>,
}

impl<T> Context<T>
where
    T: Clone,
{
    fn new(state: Option<T>) -> Self {
        Self {
            origingal_state: state,
            new_state: None,
        }
    }

    pub fn get_state(&self) -> Option<T> {
        self.origingal_state.clone()
    }

    pub fn set_state(&mut self, state: Option<T>) {
        self.new_state = state
    }
}

pub trait GenericInput<S>: InputClone<S> + Sync + Send {
    fn topic(&self) -> String;
    fn handle(&self, ctx: &mut Context<S>, data: Option<&[u8]>);
}

#[derive(Clone)]
pub struct Input<T, S, F>
where
    F: Copy + FnOnce(&mut Context<S>, &T) -> (),
{
    topic: String,
    callback: F,
    _marker: PhantomData<(T, S)>,
}

impl<T, S, F> Input<T, S, F>
where
    F: Copy + FnOnce(&mut Context<S>, &T) -> (),
{
    pub fn new(topic: String, callback: F) -> Self {
        Input {
            topic,
            callback,
            _marker: PhantomData,
        }
    }
}

impl<S> Clone for Box<dyn GenericInput<S>> {
    fn clone(&self) -> Box<dyn GenericInput<S>> {
        self.clone_box()
    }
}

pub trait InputClone<S> {
    fn clone_box(&self) -> Box<dyn GenericInput<S>>;
}

impl<T, S> InputClone<S> for T
where
    T: 'static + GenericInput<S> + Clone,
{
    fn clone_box(&self) -> Box<dyn GenericInput<S>> {
        Box::new(self.clone())
    }
}

impl<T, S, F> GenericInput<S> for Input<T, S, F>
where
    F: Copy + FnOnce(&mut Context<S>, &T) -> () + Sync + Clone + 'static + Send,
    T: Sync + Send + Clone + DeserializeOwned + 'static,
    S: Clone + Send + Sync + 'static,
{
    fn topic(&self) -> String {
        self.topic.clone()
    }

    fn handle(&self, ctx: &mut Context<S>, data: Option<&[u8]>) {
        let msg: T = serde_json::from_slice(data.expect("empty message")).unwrap();
        (self.callback)(ctx, &msg);
    }
}

pub struct Processor<S> {
    pub config: ClientConfig,
    pub inputs: HashMap<String, Box<dyn GenericInput<S>>>,
    pub stream_consumer: Arc<StreamConsumer>,
    _marker: PhantomData<S>,
}

impl<S> Processor<S>
where
    S: Clone + Send + 'static,
{
    pub fn new(config: ClientConfig, inputs: Vec<Box<dyn GenericInput<S>>>) -> Self {
        let stream_consumer: StreamConsumer = config.create().unwrap();
        let stream_consumer = Arc::new(stream_consumer);

        let inputs: HashMap<String, Box<dyn GenericInput<S>>> = inputs
            .into_iter()
            .map(|input| (input.topic(), input))
            .collect();

        Processor {
            config,
            inputs,
            stream_consumer,
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
                Box<dyn GenericInput<S>>,
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

            tokio::spawn(async move {
                let mut map: HashMap<String, S> = HashMap::new();

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
                        "Received message from partition {:?}, topic {}",
                        partition,
                        msg.topic(),
                    );

                    let state = map.get(key).map(|s| s.clone());
                    let mut ctx = Context::<S>::new(state);
                    h.handle(&mut ctx, msg.payload());
                    if let Some(state) = ctx.new_state {
                        map.insert(key.to_string(), state);
                    }
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
