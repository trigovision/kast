use std::{collections::HashSet, fmt::Debug, pin::Pin, sync::Arc, time::Duration};

use futures::{Stream, StreamExt, TryStreamExt};
use rdkafka::{
    consumer::{
        stream_consumer::StreamPartitionQueue, Consumer, DefaultConsumerContext, StreamConsumer,
    },
    error::{KafkaError, KafkaResult},
    message::{OwnedMessage, ToBytes},
    producer::{DeliveryFuture, FutureProducer, FutureRecord},
    ClientConfig,
};

pub trait IntoKafkaStream {
    type Error;
    fn stream<'a>(
        &'a self,
    ) -> Pin<Box<dyn Stream<Item = Result<OwnedMessage, Self::Error>> + Send + 'a>>;
}

impl IntoKafkaStream for StreamPartitionQueue<DefaultConsumerContext> {
    type Error = KafkaError;

    fn stream<'a>(
        &'a self,
    ) -> Pin<Box<dyn Stream<Item = Result<OwnedMessage, Self::Error>> + Send + 'a>> {
        self.stream().map_ok(|m| m.detach()).boxed()
    }
}

pub trait PartitionHelper: Send + Clone {
    type DeliveryFutureType: futures::Future + Send;
    type Error: Debug + Send;
    type OwnedStreamableType: IntoKafkaStream<Error = Self::Error> + Send;

    fn create_partitioned_topic_stream(&self, topic_name: &str) -> Self::OwnedStreamableType;

    fn send_result<'a, K, P>(
        &self,
        record: FutureRecord<'a, K, P>,
    ) -> Result<Self::DeliveryFutureType, (KafkaError, FutureRecord<'a, K, P>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized;

    fn store_offset(&mut self, topic: &str, offset: i64) -> KafkaResult<()>;
}

#[derive(Debug, thiserror::Error)]
pub enum EnsureCopartitionedError {
    #[error("Connection to kafka broker failed")]
    ConnectionFailure,

    #[error("No topics found in broker")]
    NoTopics,

    #[error("Topics are not copartitioned: {0:?}")]
    NotCopartitioned(Vec<(String, usize)>),
}

#[async_trait::async_trait]
pub trait KafkaProcessorImplementor: Send {
    type PartitionHelperType: PartitionHelper + Sync;

    fn subscribe_inputs(&mut self, input_topics: &HashSet<String>) -> Result<(), ()>;

    fn ensure_copartitioned(&mut self) -> Result<usize, EnsureCopartitionedError>;

    fn create_partitioned_consumer(&self, partition: i32) -> Self::PartitionHelperType;

    /// Kafka requires us to recv() on the main stream consumer in order for partition queues to receive messages,
    /// which is an implementation detail that does not play well with the streaming abstractions.
    /// To cope, we put that kind of special logic in this function.
    async fn start(&self) -> Result<(), String>;

    /// This function is only relevant for testing - in real applications, the processor never finishes.
    async fn wait_to_finish(&self) -> Result<(), String>;
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
    type OwnedStreamableType = StreamPartitionQueue<DefaultConsumerContext>;

    fn create_partitioned_topic_stream(&self, topic_name: &str) -> Self::OwnedStreamableType {
        self.stream_consumer
            .split_partition_queue(topic_name, self.partition)
            .unwrap()
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

    fn store_offset(&mut self, topic: &str, offset: i64) -> KafkaResult<()> {
        self.stream_consumer
            .store_offset(topic, self.partition, offset)
    }
}

#[async_trait::async_trait]
impl KafkaProcessorImplementor for KafkaProcessorHelper {
    type PartitionHelperType = KafkaPartitionProcessor;

    fn subscribe_inputs(&mut self, input_topics: &HashSet<String>) -> Result<(), ()> {
        self.inputs.extend(input_topics.clone()); //TODO: This is strange
        self.stream_consumer
            .subscribe(
                input_topics
                    .iter()
                    .map(|topic| topic.as_ref())
                    .collect::<Vec<&str>>()
                    .as_slice(),
            )
            .unwrap();

        Ok(())
    }

    fn create_partitioned_consumer(&self, partition: i32) -> KafkaPartitionProcessor {
        KafkaPartitionProcessor {
            partition,
            future_producer: self.future_producer.clone(),
            stream_consumer: self.stream_consumer.clone(),
        }
    }

    async fn start(&self) -> Result<(), String> {
        let message = self.stream_consumer.recv().await;
        let err = format!(
            "main stream consumer queue unexpectedly received message: {:?}",
            message
        );
        Err(err)
    }

    async fn wait_to_finish(&self) -> Result<(), String> {
        panic!("Cannot wait for a kafka processor to finish")
    }

    fn ensure_copartitioned(&mut self) -> Result<usize, EnsureCopartitionedError> {
        let shit = self
            .stream_consumer
            .fetch_metadata(None, Duration::from_secs(5))
            .map_err(|_| EnsureCopartitionedError::ConnectionFailure)?;

        let topics_partitions: Vec<(String, usize)> = shit
            .topics()
            .iter()
            .filter(|topic| self.inputs.contains(topic.name()))
            .map(|topic| (topic.name().to_string(), topic.partitions().len()))
            .collect();

        if topics_partitions.is_empty() {
            Err(EnsureCopartitionedError::NoTopics)
        } else if topics_partitions
            .iter()
            .all(|(_, partitions)| partitions.eq(&topics_partitions[0].1))
        {
            Ok(topics_partitions[0].1)
        } else {
            Err(EnsureCopartitionedError::NotCopartitioned(
                topics_partitions,
            ))
        }
    }
}
