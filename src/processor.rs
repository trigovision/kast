use std::{collections::HashMap, marker::PhantomData, sync::Arc, time::Duration};

use futures::{future::join_all, stream::SelectAll, StreamExt};
use rdkafka::{
    consumer::{
        stream_consumer::StreamPartitionQueue, Consumer, DefaultConsumerContext, StreamConsumer,
    },
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message,
};

use crate::{context::Context, input::GenericInput, output::Output, state_store::StateStore};

pub struct Processor<TState, TExtraState, TStore, F1, F2> {
    config: ClientConfig,
    inputs: HashMap<String, Box<dyn GenericInput<TState, TExtraState>>>,
    stream_consumer: Arc<StreamConsumer>,
    state_store_gen: F1,
    extra_state_gen: F2,
    outputs: Vec<Output>,
    _marker: PhantomData<(TState, TStore)>,
}

impl<TState, TExtraState, TStore, F1, F2> Processor<TState, TExtraState, TStore, F1, F2>
where
    TState: Clone + Send + Sync + 'static,
    TStore: StateStore<String, TState> + Send + Sync,
    F1: FnOnce() -> TStore + Copy + Send + 'static,
    F2: FnOnce() -> TExtraState + Copy + Send + 'static,
    TExtraState: Send + 'static,
{
    pub fn new(
        config: ClientConfig,
        inputs: Vec<Box<dyn GenericInput<TState, TExtraState>>>,
        outputs: Vec<Output>,
        state_store_gen: F1,
        extra_state_gen: F2,
    ) -> Self {
        let stream_consumer: StreamConsumer = config.create().unwrap();
        let stream_consumer = Arc::new(stream_consumer);

        let inputs = inputs
            .into_iter()
            .map(|input| (input.topic(), input))
            .collect();

        Processor {
            config,
            inputs,
            outputs,
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

    pub async fn run(self) {
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
                let mut extra_state = gen2();
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

                    let state = state_store.get(key).await.map(|s| s.clone());
                    let mut ctx = Context::new(key, state);

                    h.handle(&mut extra_state, &mut ctx, msg.payload()).await;
                    if let Some(state) = &ctx.get_state() {
                        state_store.set(key.to_string(), state.clone()).await;
                    }

                    let stream_consumer = stream_consumer.clone();
                    let msg_topic = msg.topic().to_string();
                    let msg_partition = msg.partition();
                    let msg_offset = msg.offset();

                    let producer = producer.clone();
                    // NOTE: It is extremly important that this will happen here and not inside the spawn to gurantee order of msgs!
                    let sends = ctx.to_send().into_iter().map(move |s| {
                        producer
                            .send_result(FutureRecord::to(&s.topic).key(&s.key).payload(&s.payload))
                            .unwrap()
                    });

                    tokio::spawn(async move {
                        join_all(sends).await;
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
