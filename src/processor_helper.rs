use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    marker::PhantomData,
    pin::Pin,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::{Stream, StreamExt};
use rdkafka::{
    consumer::{
        stream_consumer::StreamPartitionQueue, Consumer, DefaultConsumerContext, StreamConsumer,
    },
    error::{KafkaError, KafkaResult},
    message::{OwnedMessage, ToBytes},
    producer::{DeliveryFuture, FutureProducer, FutureRecord},
    ClientConfig, Message,
};
use serde::Serialize;
use tokio::{sync::broadcast::error::SendError, time::sleep};
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};

use crate::encoders::{Decoder, Encoder};

pub struct ShittyKafkaShit {
    a: StreamPartitionQueue<DefaultConsumerContext>,
    // b: MessageStream,
}

impl ShittyKafkaShit {
    fn new(a: StreamPartitionQueue<DefaultConsumerContext>) -> Self {
        Self {
            a,
            // b: a.stream().then(|f| f.map(|a| a.detach())),
        }
    }
}

//TODO: Make this borrowed message instead of owned!
impl Stream for ShittyKafkaShit {
    type Item = KafkaResult<OwnedMessage>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.a.stream().poll_next_unpin(cx).map_ok(|a| a.detach()) //TODO: fix this - it's expensive
    }
}

pub trait KafkaProcessorImplementor: Send + Clone
where
    <Self::DeliveryFutureType as futures::Future>::Output: Send,
{
    type Error: Debug + Send;
    type OwnedStreamableType: Stream<Item = Result<OwnedMessage, Self::Error>>
        + Send
        + Sync
        + 'static;
    type DeliveryFutureType: futures::Future + Send;

    fn validate_inputs_outputs(
        &mut self,
        input_topics: &HashSet<String>,
        output_topics: &HashSet<String>,
    ) -> Result<usize, ()>;

    fn create_partitioned_consumer(
        &self,
        topic_name: &str,
        partition: u16,
    ) -> Self::OwnedStreamableType;

    fn send_result<'a, K, P>(
        &self,
        record: FutureRecord<'a, K, P>,
    ) -> Result<Self::DeliveryFutureType, (KafkaError, FutureRecord<'a, K, P>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized;

    fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> KafkaResult<()>;

    fn wait_to_finish(
        self,
        topics: &HashSet<String>,
    ) -> tokio::task::JoinHandle<Result<(), String>>;
}

// pub trait PartitionedProcessorHelper: Send
// where
//     <Self::DeliveryFutureType as futures::Future>::Output: Send,
// {
//     type DeliveryFutureType: futures::Future + Send;

//     fn send_result<'a, K, P>(
//         &self,
//         record: FutureRecord<'a, K, P>,
//     ) -> Result<Self::DeliveryFutureType, (KafkaError, FutureRecord<'a, K, P>)>
//     where
//         K: ToBytes + ?Sized,
//         P: ToBytes + ?Sized;

//     fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> KafkaResult<()>;

//     fn notify_partition_handler_done(&self, partition: i32);
// }

#[derive(Clone)]
pub struct KafkaProcessorHelper {
    pub(crate) stream_consumer: Arc<StreamConsumer>,
    future_producer: FutureProducer,
}

impl KafkaProcessorHelper {
    pub fn new(config: ClientConfig) -> Self {
        let stream_consumer: StreamConsumer = config.create().unwrap();
        let stream_consumer = Arc::new(stream_consumer);
        let future_producer: FutureProducer = config.create().unwrap();

        Self {
            stream_consumer,
            future_producer,
        }
    }
}

impl KafkaProcessorHelper {
    fn check_that_inputs_are_copartitioned(&self, topics: &HashSet<String>) -> Result<usize, ()> {
        let shit = self
            .stream_consumer
            .fetch_metadata(None, Duration::from_secs(1))
            .unwrap();

        let topics_partitions: Vec<(String, usize)> = shit
            .topics()
            .iter()
            .filter(|topic| topics.contains(topic.name()))
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

impl KafkaProcessorImplementor for KafkaProcessorHelper {
    type Error = KafkaError;
    type OwnedStreamableType = ShittyKafkaShit;
    type DeliveryFutureType = DeliveryFuture;

    fn validate_inputs_outputs(
        &mut self,
        input_topics: &HashSet<String>,
        _output_topics: &HashSet<String>,
    ) -> Result<usize, ()> {
        self.check_that_inputs_are_copartitioned(input_topics)
    }

    fn create_partitioned_consumer(
        &self,
        topic_name: &str,
        partition: u16,
    ) -> Self::OwnedStreamableType {
        let bla = self
            .stream_consumer
            .clone()
            .split_partition_queue(topic_name, partition as i32)
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
}

pub struct TopicTestHelper {
    ch_tx: tokio::sync::broadcast::Sender<OwnedMessage>,
    ch_rx: tokio::sync::broadcast::Receiver<OwnedMessage>,
    sent: Arc<AtomicU32>,
    recv: Arc<AtomicU32>,
    input: bool,
    output: bool,
}

impl Clone for TopicTestHelper {
    fn clone(&self) -> Self {
        Self {
            ch_tx: self.ch_tx.clone(),
            ch_rx: self.ch_tx.subscribe(),
            sent: self.sent.clone(),
            recv: self.recv.clone(),
            input: self.input.clone(),
            output: self.output.clone(),
        }
    }
}

impl Default for TopicTestHelper {
    fn default() -> Self {
        let (tx, rx) = tokio::sync::broadcast::channel(1_000_000);

        Self {
            ch_tx: tx,
            ch_rx: rx,
            sent: Default::default(),
            recv: Default::default(),
            input: false,
            output: false,
        }
    }
}

#[derive(Clone)]
pub struct TestsProcessorHelper {
    topics: HashMap<String, TopicTestHelper>,
}

impl TestsProcessorHelper {
    pub fn new(topics: Vec<&str>) -> Self {
        let mut m = HashMap::new();
        for topic in topics {
            m.entry(topic.to_string())
                .or_insert(TopicTestHelper::default());
        }

        Self { topics: m }
    }
}

impl TestsProcessorHelper {
    pub fn input<F, T>(&mut self, topic: String, decoder: F) -> Sender<T, F>
    where
        F: Decoder,
        T: Serialize,
    {
        let input = self.topics.get_mut(&topic).unwrap();
        input.input = true;
        Sender::new(topic, decoder, input.ch_tx.clone(), input.sent.clone())
    }

    pub fn output<E, T>(&mut self, topic: String, encoder: E) -> Receiver<T, E>
    where
        E: Encoder<In = T>,
    {
        let output = self.topics.get_mut(&topic).unwrap();
        output.output = true;
        Receiver::new(encoder, output.ch_tx.subscribe())
    }
}

impl KafkaProcessorImplementor for TestsProcessorHelper {
    type Error = BroadcastStreamRecvError;
    type OwnedStreamableType = BroadcastStream<OwnedMessage>;
    type DeliveryFutureType = Pin<Box<dyn futures::Future<Output = ()> + Send>>;

    fn validate_inputs_outputs(
        &mut self,
        _input_topics: &HashSet<String>,
        _output_topics: &HashSet<String>,
    ) -> Result<usize, ()> {
        Ok(1)
    }

    fn create_partitioned_consumer(
        &self,
        topic_name: &str,
        _partition: u16,
    ) -> Self::OwnedStreamableType {
        BroadcastStream::new(self.topics.get(topic_name).unwrap().ch_tx.subscribe())
    }

    fn send_result<'a, K, P>(
        &self,
        record: FutureRecord<'a, K, P>,
    ) -> Result<Self::DeliveryFutureType, (KafkaError, FutureRecord<'a, K, P>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let msg = OwnedMessage::new(
            record.payload.map(|p| p.to_bytes().to_vec()),
            record.key.map(|k| k.to_bytes().to_vec()),
            record.topic.to_string(),
            rdkafka::Timestamp::NotAvailable,
            0,
            0,
            None,
        );

        self.topics
            .get(record.topic)
            .unwrap()
            .sent
            .fetch_add(1, Ordering::Relaxed);

        self.topics
            .get(record.topic)
            .unwrap()
            .ch_tx
            .send(msg)
            .unwrap();
        Ok(Box::pin(async move { () }))
    }

    fn store_offset(&self, topic: &str, _partition: i32, _offset: i64) -> KafkaResult<()> {
        self.topics
            .get(topic)
            .unwrap()
            .recv
            .fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    fn wait_to_finish(
        self,
        _topics: &HashSet<String>,
    ) -> tokio::task::JoinHandle<Result<(), String>> {
        let task = tokio::spawn(async move {
            // TODO: This is ugly and we can implement better "wait" mechanism
            // TODO: Support output topics where we don't receive every message, only if it's inputs
            loop {
                if self
                    .topics
                    .iter()
                    .filter(|(_, t)| t.input)
                    .all(|(_t, stats)| {
                        // println!("{:?}", _t);
                        let r = stats.recv.load(Ordering::Relaxed);
                        let s = stats.sent.load(Ordering::Relaxed);
                        s == r
                    })
                {
                    break;
                }
                sleep(Duration::from_nanos(0)).await;
            }

            Ok(())
        });

        task
    }
}

//Should we use with flat map on channels?
pub struct Sender<T, F>
where
    F: Decoder,
{
    topic: String,
    decoder: F,
    tx: tokio::sync::broadcast::Sender<OwnedMessage>,
    sem: Arc<AtomicU32>,
    _marker: PhantomData<T>,
}

impl<T, F> Sender<T, F>
where
    F: Decoder,
    T: Serialize,
{
    fn new(
        topic: String,
        decoder: F,
        tx: tokio::sync::broadcast::Sender<OwnedMessage>,
        sem: Arc<AtomicU32>,
    ) -> Self {
        Self {
            topic,
            decoder,
            tx,
            sem,
            _marker: PhantomData,
        }
    }

    pub async fn send(&mut self, key: String, msg: &T) -> Result<usize, SendError<OwnedMessage>> {
        let data = self.decoder.decode(msg);
        let msg = OwnedMessage::new(
            Some(data),
            Some(key.as_bytes().to_vec()),
            self.topic.clone(),
            rdkafka::Timestamp::NotAvailable,
            0,
            0,
            None,
        );

        self.sem.fetch_add(1, Ordering::Relaxed);
        self.tx.send(msg)
    }
}

pub struct Receiver<T, E>
where
    E: Encoder<In = T>,
{
    encoder: E,
    rx: tokio::sync::broadcast::Receiver<OwnedMessage>,
}

impl<T, E> Receiver<T, E>
where
    E: Encoder<In = T>,
{
    fn new(encoder: E, rx: tokio::sync::broadcast::Receiver<OwnedMessage>) -> Self {
        Self { encoder, rx }
    }

    pub async fn recv(&mut self) -> Result<T, tokio::sync::broadcast::error::RecvError> {
        let data = self.rx.recv().await;
        data.map(|d| self.encoder.encode(d.payload()))
    }

    pub fn try_recv(&mut self) -> Result<T, tokio::sync::broadcast::error::TryRecvError> {
        let data = self.rx.try_recv();
        data.map(|d| self.encoder.encode(d.payload()))
    }
}
