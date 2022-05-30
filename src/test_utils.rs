use std::{
    collections::{HashMap, HashSet},
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
    error::{KafkaError, KafkaResult},
    message::{OwnedMessage, ToBytes},
    producer::FutureRecord,
    Message,
};
use serde::Serialize;
use tokio::{sync::broadcast::error::SendError, time::sleep};
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};

pub struct TopicTestHelper {
    ch_tx: tokio::sync::broadcast::Sender<OwnedMessage>,
    sent: Arc<AtomicU32>,
    recv: Arc<AtomicU32>,
    input: bool,
    // Note: we store _ch_rx so sends to a channel which isn't input
    // Wont panic when there are no subscribers
    _ch_rx: tokio::sync::broadcast::Receiver<OwnedMessage>,
}

impl Clone for TopicTestHelper {
    fn clone(&self) -> Self {
        Self {
            ch_tx: self.ch_tx.clone(),
            _ch_rx: self.ch_tx.subscribe(),
            sent: self.sent.clone(),
            recv: self.recv.clone(),
            input: self.input,
        }
    }
}

impl Default for TopicTestHelper {
    fn default() -> Self {
        let (tx, rx) = tokio::sync::broadcast::channel(1_000_000);

        Self {
            ch_tx: tx,
            _ch_rx: rx,
            sent: Default::default(),
            recv: Default::default(),
            input: false,
        }
    }
}

pub struct TestsProcessorHelper {
    topics: HashMap<String, TopicTestHelper>,
}

impl TestsProcessorHelper {
    pub fn new(topics: Vec<&str>) -> Self {
        let mut m = HashMap::new();
        for topic in topics {
            m.entry(topic.to_string()).or_default();
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
        Sender::new(topic, decoder, input.ch_tx.clone(), input.sent.clone())
    }

    pub fn output<E, T>(&mut self, topic: String, encoder: E) -> Receiver<T, E>
    where
        E: Encoder<In = T>,
    {
        let output = self.topics.get_mut(&topic).unwrap();
        Receiver::new(encoder, output.ch_tx.subscribe())
    }
}

#[derive(Clone)]
pub struct TestsPartitionProcessor {
    topics: HashMap<String, TopicTestHelper>,
}

impl IntoKafkaStream for tokio::sync::broadcast::Sender<OwnedMessage> {
    type Error = BroadcastStreamRecvError;

    fn stream<'a>(
        &'a self,
    ) -> Pin<Box<dyn Stream<Item = Result<OwnedMessage, Self::Error>> + Send + 'a>> {
        BroadcastStream::new(self.subscribe()).boxed()
    }
}

impl PartitionHelper for TestsPartitionProcessor {
    type DeliveryFutureType = Pin<Box<dyn futures::Future<Output = ()> + Send>>;
    type Error = BroadcastStreamRecvError;
    type OwnedStreamableType = tokio::sync::broadcast::Sender<OwnedMessage>;

    fn create_partitioned_topic_stream(
        &self,
        topic_name: &str,
    ) -> tokio::sync::broadcast::Sender<OwnedMessage> {
        let rec = self.topics.get(topic_name).unwrap();
        rec.ch_tx.clone()
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

        let rec = self.topics.get(record.topic).unwrap();
        rec.sent.fetch_add(1, Ordering::Relaxed);
        rec.ch_tx.send(msg).unwrap();
        Ok(Box::pin(async move {}))
    }

    fn store_offset(&mut self, topic: &str, _offset: i64) -> KafkaResult<()> {
        self.topics
            .get(topic)
            .unwrap()
            .recv
            .fetch_add(1, Ordering::Relaxed);

        Ok(())
    }
}

use crate::{
    encoders::{Decoder, Encoder},
    processor_helper::{IntoKafkaStream, KafkaProcessorImplementor, PartitionHelper},
};
impl KafkaProcessorImplementor for TestsProcessorHelper {
    type PartitionHelperType = TestsPartitionProcessor;

    fn subscribe_inputs(&mut self, input_topics: &HashSet<String>) -> Result<(), ()> {
        for (k, v) in self.topics.iter_mut() {
            if input_topics.contains(k) {
                v.input = true;
            }
        }
        Ok(())
    }

    fn create_partitioned_consumer(&self, _partition: i32) -> TestsPartitionProcessor {
        TestsPartitionProcessor {
            topics: self.topics.clone(),
        }
    }

    fn wait_to_finish(self) -> tokio::task::JoinHandle<Result<(), String>> {
        tokio::spawn(async move {
            // TODO: This is ugly and we can implement better "wait" mechanism
            // TODO: Support output topics where we don't receive every message, only if it's inputs
            loop {
                if self
                    .topics
                    .iter()
                    .filter(|(_, t)| t.input)
                    .all(|(_t, stats)| {
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
        })
    }

    fn ensure_copartitioned(&mut self) -> Result<usize, ()> {
        Ok(1)
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
