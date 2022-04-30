use std::{collections::HashMap, marker::PhantomData, sync::Arc, time::Duration};

use futures::{stream::SelectAll, StreamExt};
use rdkafka::{
    consumer::{
        stream_consumer::StreamPartitionQueue, Consumer, DefaultConsumerContext, StreamConsumer,
    },
    ClientConfig, Message,
};
use serde::de::DeserializeOwned;

pub struct Context {}

pub trait GenericInput: InputClone + Sync + Send {
    fn topic(&self) -> String;
    fn handle(&self, data: Option<&[u8]>);
}

#[derive(Clone)]
pub struct Input<T, F>
where
    F: Copy + FnOnce(Context, &T) -> (),
{
    topic: String,
    callback: F,
    _marker: PhantomData<T>,
}

impl<T, F> Input<T, F>
where
    F: Copy + FnOnce(Context, &T) -> (),
{
    pub fn new(topic: String, callback: F) -> Self {
        Input {
            topic,
            callback,
            _marker: PhantomData,
        }
    }
}

impl Clone for Box<dyn GenericInput> {
    fn clone(&self) -> Box<dyn GenericInput> {
        self.clone_box()
    }
}

pub trait InputClone {
    fn clone_box(&self) -> Box<dyn GenericInput>;
}

impl<T> InputClone for T
where
    T: 'static + GenericInput + Clone,
{
    fn clone_box(&self) -> Box<dyn GenericInput> {
        Box::new(self.clone())
    }
}

impl<T, F> GenericInput for Input<T, F>
where
    F: Copy + FnOnce(Context, &T) -> () + Sync + Clone + 'static + Send,
    T: Sync + Send + Clone + DeserializeOwned + 'static,
{
    fn topic(&self) -> String {
        self.topic.clone()
    }

    fn handle(&self, data: Option<&[u8]>) {
        let ctx = Context {};
        let msg: T = serde_json::from_slice(data.expect("empty message")).unwrap();
        (self.callback)(ctx, &msg);
    }
}

pub struct Processor<T = ()> {
    pub config: ClientConfig,
    pub inputs: HashMap<String, Box<dyn GenericInput>>,
    pub stream_consumer: Arc<StreamConsumer>,
}

impl Processor {
    pub fn new(config: ClientConfig, inputs: Vec<Box<dyn GenericInput>>) -> Self {
        let stream_consumer: StreamConsumer = config.create().unwrap();
        let stream_consumer = Arc::new(stream_consumer);

        let inputs: HashMap<String, Box<dyn GenericInput>> = inputs
            .into_iter()
            .map(|input| (input.topic(), input))
            .collect();

        Processor {
            config,
            inputs,
            stream_consumer,
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
            let bla: Vec<(
                StreamPartitionQueue<DefaultConsumerContext>,
                Box<dyn GenericInput>,
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
                let mut select_all = SelectAll::new();
                let streams = bla.iter().map(|(a, h)| a.stream().map(move |msg| (h, msg)));
                for stream in streams {
                    select_all.push(stream)
                }

                while let Some((h, msg)) = select_all.next().await {
                    let msg = msg.unwrap();
                    println!(
                        "Received message from partition {:?}, topic {}",
                        partition,
                        msg.topic(),
                    );
                    h.handle(msg.payload())
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
            // stream_consumer.recv()
            let message = stream_consumer.recv().await;
            panic!(
                "main stream consumer queue unexpectedly received message: {:?}",
                message
            );
        });

        task.await.unwrap();
    }
}
