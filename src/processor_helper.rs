use std::{collections::HashSet, fmt::Debug, marker::PhantomData, sync::Arc, time::Duration};

use futures::{Stream, StreamExt};
use rdkafka::{
    consumer::{
        stream_consumer::StreamPartitionQueue, Consumer, DefaultConsumerContext, MessageStream,
        StreamConsumer,
    },
    error::{KafkaError, KafkaResult},
    message::{BorrowedMessage, OwnedMessage, ToBytes},
    producer::{DeliveryFuture, FutureProducer, FutureRecord},
    ClientConfig, Message,
};

pub struct ShittyKafkaShit<'a> {
    a: StreamPartitionQueue<DefaultConsumerContext>,
    // b: MessageStream<'a>,
    b: PhantomData<&'a u32>,
}

impl<'a> ShittyKafkaShit<'a> {
    fn new(a: StreamPartitionQueue<DefaultConsumerContext>) -> Self {
        Self {
            a,
            // b: a.stream(),
            b: PhantomData,
        }
    }
}

//TODO: Make this borrowed message instead of owned!
impl<'a> Stream for ShittyKafkaShit<'a> {
    type Item = KafkaResult<OwnedMessage>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.a.stream().poll_next_unpin(cx).map_ok(|a| a.detach())
    }
}

pub trait PartitionHelper: Send + Clone {
    type DeliveryFutureType: futures::Future + Send;
    type Error: Debug + Send;
    type M: Message;
    type OwnedStreamableType: Stream<Item = Result<Self::M, Self::Error>> + Send + Sync + 'static;

    fn create_partitioned_topic_stream(&self, topic_name: &str) -> Self::OwnedStreamableType;

    fn send_result<'a, K, P>(
        &self,
        record: FutureRecord<'a, K, P>,
    ) -> Result<Self::DeliveryFutureType, (KafkaError, FutureRecord<'a, K, P>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized;

    fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> KafkaResult<()>;
}

pub trait KafkaProcessorImplementor: Send {
    type PartitionHelperType: PartitionHelper + Sync;

    fn prepare_input_outputs(
        &mut self,
        input_topics: &HashSet<String>,
        output_topics: &HashSet<String>,
    ) -> Result<(), ()>;

    fn ensure_copartitioned(&mut self) -> Result<usize, ()>;

    fn create_partitioned_consumer(&self, partition: i32) -> Self::PartitionHelperType;

    fn wait_to_finish(
        self,
        topics: &HashSet<String>,
    ) -> tokio::task::JoinHandle<Result<(), String>>;
}

pub struct KafkaProcessorHelper {
    pub(crate) stream_consumer: Arc<StreamConsumer>,
    future_producer: FutureProducer,
    inputs: HashSet<String>,
}

impl KafkaProcessorHelper {
    pub fn new(config: ClientConfig) -> Self {
        let stream_consumer: StreamConsumer = config.create().unwrap();
        let stream_consumer = Arc::new(stream_consumer);
        let future_producer: FutureProducer = config.create().unwrap();

        Self {
            stream_consumer,
            future_producer,
            inputs: HashSet::new(),
        }
    }
}

#[derive(Clone)]
pub struct KafkaPartitionProcessor {
    partition: i32,
    future_producer: FutureProducer,
    pub(crate) stream_consumer: Arc<StreamConsumer>,
}

impl PartitionHelper for KafkaPartitionProcessor {
    type DeliveryFutureType = DeliveryFuture;
    type Error = KafkaError;
    type M = OwnedMessage;
    type OwnedStreamableType = ShittyKafkaShit<'static>;

    fn create_partitioned_topic_stream(&self, topic_name: &str) -> Self::OwnedStreamableType {
        let bla = self
            .stream_consumer
            .clone()
            .split_partition_queue(topic_name, self.partition)
            .unwrap();

        ShittyKafkaShit::new(bla)
    }

    fn send_result<'a, K, P>(
        &self,
        record: FutureRecord<'a, K, P>,
    ) -> Result<Self::DeliveryFutureType, (KafkaError, FutureRecord<'a, K, P>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        self.future_producer.send_result(record)
    }

    fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> KafkaResult<()> {
        self.stream_consumer.store_offset(topic, partition, offset)
    }
}

impl KafkaProcessorImplementor for KafkaProcessorHelper {
    type PartitionHelperType = KafkaPartitionProcessor;

    fn prepare_input_outputs(
        &mut self,
        _input_topics: &HashSet<String>,
        _output_topics: &HashSet<String>,
    ) -> Result<(), ()> {
        Ok(())
    }

    fn create_partitioned_consumer(&self, partition: i32) -> KafkaPartitionProcessor {
        KafkaPartitionProcessor {
            partition,
            future_producer: self.future_producer.clone(),
            stream_consumer: self.stream_consumer.clone(),
        }
    }

    fn wait_to_finish(
        self,
        topics: &HashSet<String>,
    ) -> tokio::task::JoinHandle<Result<(), String>> {
        let stream_consumer = self.stream_consumer.clone();

        stream_consumer
            .subscribe(
                topics
                    .iter()
                    .map(|topic| topic.as_ref())
                    .collect::<Vec<&str>>()
                    .as_slice(),
            )
            .unwrap();

        let task = tokio::spawn(async move {
            //TODO: Fix this
            loop {
                let message = stream_consumer.recv().await;
                let err = format!(
                    "main stream consumer queue unexpectedly received message: {:?}",
                    message
                );
                return Err(err);
            }

            Ok(())
        });

        task
    }

    fn ensure_copartitioned(&mut self) -> Result<usize, ()> {
        let shit = self
            .stream_consumer
            .fetch_metadata(None, Duration::from_secs(1))
            .unwrap();

        let topics_partitions: Vec<(String, usize)> = shit
            .topics()
            .iter()
            .filter(|topic| self.inputs.contains(topic.name()))
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
}
