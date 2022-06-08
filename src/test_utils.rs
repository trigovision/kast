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

use crate::{
    encoders::{Decoder, Encoder},
    processor_helper::{
        EnsureCopartitionedError, IntoKafkaStream, KafkaProcessorImplementor, PartitionHelper,
    },
};
use async_stream::stream;
use futures::Stream;
use rdkafka::{
    error::{KafkaError, KafkaResult},
    message::{OwnedMessage, ToBytes},
    producer::FutureRecord,
    Message,
};
use serde::Serialize;

#[derive(Clone)]
pub struct TopicTestHelper {
    ch_tx: async_channel::Sender<OwnedMessage>,
    sent: Arc<AtomicU32>,
    recv: Arc<AtomicU32>,
    input: bool,
    ch_rx: async_channel::Receiver<OwnedMessage>,
}

impl Default for TopicTestHelper {
    fn default() -> Self {
        let (tx, rx) = async_channel::bounded(1_000_000);

        Self {
            ch_tx: tx,
            ch_rx: rx,
            sent: Default::default(),
            recv: Default::default(),
            input: false,
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
            m.entry(topic.to_string()).or_default();
        }

        Self { topics: m }
    }
}

impl TestsProcessorHelper {
    pub fn input<F, T>(&mut self, topic: &str, decoder: F) -> Sender<T, F>
    where
        F: Encoder,
        T: Serialize,
    {
        let input = self.topics.get_mut(topic).unwrap();
        Sender::new(
            topic.to_string(),
            decoder,
            input.ch_tx.clone(),
            input.sent.clone(),
        )
    }

    pub fn output<E, T>(&mut self, topic: String, encoder: E) -> Receiver<T, E>
    where
        E: Decoder<In = T>,
    {
        let output = self.topics.get_mut(&topic).unwrap();
        Receiver::new(encoder, output.ch_rx.clone())
    }
}

#[derive(Clone)]
pub struct TestsPartitionProcessor {
    topics: HashMap<String, TopicTestHelper>,
}

impl IntoKafkaStream for TopicTestHelper {
    type Error = async_channel::RecvError;

    fn stream<'a>(
        &'a self,
    ) -> Pin<Box<dyn Stream<Item = Result<OwnedMessage, Self::Error>> + Send + 'a>> {
        Box::pin(stream! {
            loop {
                yield self.ch_rx.recv().await
            }
        })
    }
}

impl PartitionHelper for TestsPartitionProcessor {
    type DeliveryFutureType = Pin<Box<dyn futures::Future<Output = ()> + Send>>;
    type Error = async_channel::RecvError;
    type OwnedStreamableType = TopicTestHelper;

    fn create_partitioned_topic_stream(&self, topic_name: &str) -> Self::OwnedStreamableType {
        self.topics.get(topic_name).unwrap().clone()
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
        
        let ch_tx_clone = rec.ch_tx.clone();
        Ok(Box::pin(async move { ch_tx_clone.send(msg).await.unwrap(); }))
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

#[async_trait::async_trait]
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

    async fn start(&self) -> Result<(), String> {
        Ok(())
    }

    async fn wait_to_finish(&self) -> Result<(), String> {
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
                    println!("{} {} {}", _t, s, r);
                    s == r && r != 0
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Ok(())
    }

    fn ensure_copartitioned(&mut self) -> Result<usize, EnsureCopartitionedError> {
        Ok(1)
    }
}

//Should we use with flat map on channels?
pub struct Sender<T, F>
where
    F: Encoder,
{
    topic: String,
    decoder: F,
    tx: async_channel::Sender<OwnedMessage>,
    sem: Arc<AtomicU32>,
    _marker: PhantomData<T>,
}

impl<T, F> Sender<T, F>
where
    F: Encoder,
    T: Serialize,
{
    fn new(
        topic: String,
        decoder: F,
        tx: async_channel::Sender<OwnedMessage>,
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

    pub async fn send(
        &mut self,
        key: String,
        msg: &T,
    ) -> Result<(), async_channel::SendError<OwnedMessage>> {
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

        self.tx.send(msg).await
    }
}

pub struct Receiver<T, E>
where
    E: Decoder<In = T>,
{
    encoder: E,
    rx: async_channel::Receiver<OwnedMessage>,
}

impl<T, E> Receiver<T, E>
where
    E: Decoder<In = T>,
{
    fn new(encoder: E, rx: async_channel::Receiver<OwnedMessage>) -> Self {
        Self { encoder, rx }
    }

    pub async fn recv(&mut self) -> Result<T, async_channel::RecvError> {
        let data = self.rx.recv().await;
        data.map(|d| self.encoder.encode(d.payload()))
    }

    pub fn try_recv(&mut self) -> Result<T, async_channel::TryRecvError> {
        let data = self.rx.try_recv();
        data.map(|d| self.encoder.encode(d.payload()))
    }
}
