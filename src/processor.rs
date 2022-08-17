use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
    sync::Arc,
};

use futures::{future::join_all, stream::select_all, StreamExt};
use rdkafka::{producer::FutureRecord, Message, message::{OwnedHeaders, Headers}};
use tokio::{sync::{Barrier, Mutex}, select};

use crate::{
    context::Context,
    input::GenericInput,
    output::Output,
    processor_helper::{IntoKafkaStream, KafkaProcessorImplementor, PartitionHelper},
    state_store::{StateStore, StateStoreError},
};

fn format_join_error(e: tokio::task::JoinError) -> String {
    let p = e.into_panic();
    match p.downcast::<String>() {
        Ok(v) => *v,
        Err(p) => match p.downcast::<&str>() {
            Ok(v) => v.to_string(),
            Err(_) => "Unknown panic type".to_string(),
        },
    }
}

pub struct Processor<TState, TExtraState, TStore, F1, F2, H> {
    state_namespace: String,
    helper: H,
    inputs: HashMap<String, Box<dyn GenericInput<TState, TExtraState, TStore>>>,
    state_store_gen: F1,
    extra_state_gen: F2,
    outputs: Vec<Output>,
    _marker: PhantomData<(TState, TStore)>,
}

impl<TState, TExtraState, TStore, F1, F2, H> Processor<TState, TExtraState, TStore, F1, F2, H>
where
    TState: Clone + Send + Sync + 'static,
    TStore: StateStore<TState> + Send + Sync + 'static,
    F1: FnOnce() -> Arc<Mutex<TStore>> + Send + Clone + 'static,
    F2: FnOnce() -> TExtraState + Clone + Send + 'static,
    TExtraState: Send + 'static,
    H: KafkaProcessorImplementor + Sync + 'static,
    <<<H as KafkaProcessorImplementor>::PartitionHelperType as PartitionHelper>::DeliveryFutureType as futures::Future>::Output: std::marker::Send,
{
    pub fn new(
        state_namespace: &str,
        helper: H,
        inputs: Vec<Box<dyn GenericInput<TState, TExtraState, TStore>>>,
        outputs: Vec<Output>,
        state_store_gen: F1,
        extra_state_gen: F2,
    ) -> Self {
        let inputs = inputs
            .into_iter()
            .map(|input| (input.topic(), input))
            .collect();

        Processor {
            state_namespace: state_namespace.to_string(),
            helper,
            inputs,
            outputs,
            state_store_gen,
            extra_state_gen,
            _marker: PhantomData,
        }
    }

    pub async fn run_forever(&mut self) -> Result<(), String> {
        let input_topics_set: HashSet<String> = self.inputs.keys().cloned().collect();
        let output_topics_set: HashSet<String> =
            self.outputs.iter().map(|o| o.topic().to_string()).collect();

        self.helper
            .subscribe_inputs(&input_topics_set)
            .unwrap();

        let num_partitions = self.helper.ensure_copartitioned().expect("Not copartitioned");

        let partitions_barrier = Arc::new(Barrier::new(num_partitions + 1));
        let mut tokio_tasks = Vec::new();
        for partition in 0..num_partitions as i32 {
            let inputs = self.inputs.clone();               
            let output_topics_set = output_topics_set.clone();
            let partition_handler = self.helper.create_partitioned_consumer(partition as i32);
            let partitions_barrier_clone = partitions_barrier.clone();
            let gen = self.state_store_gen.clone();
            let gen2 = self.extra_state_gen.clone();
            let state_namespace = self.state_namespace.clone();
            tokio_tasks.push(tokio::spawn(async move {
                let state_store = gen();
                let mut extra_state = gen2();
                let stream_gens: Vec<_> = inputs.keys().into_iter().map(|topic| {
                    partition_handler.create_partitioned_topic_stream(topic)
                }).collect(); 
                let mut topic_partition_offset_locks = HashMap::new();
                let mut streams = select_all(
                    stream_gens.iter().map(|h| 
                        h.stream()
                    )
                );

                partitions_barrier_clone.wait().await;
                while let Some(msg) = streams.next().await {
                    let msg = msg.unwrap();
                    assert_eq!(msg.partition(), partition);
                    
                    // TODO: Key serializer?
                    let key = std::str::from_utf8(msg.key().unwrap()).unwrap();
                    tracing::debug!("Received kafka message on {}:{}", state_namespace, key);
                    
                    let state = match state_store.lock().await.get(&state_namespace, key).await {
                        Ok(state) => Some(state),
                        Err(StateStoreError::MissingKey(_)) => None,
                        Err(e) => {
                            panic!("Failed to get state while handling message on topic {}: {:?}", msg.topic(), e);
                        },
                    };
                    let mut ctx = Context::new(key, state, Arc::clone(&state_store));

                    let handler = inputs.get(msg.topic()).expect("streams are created from inputs keys");
                    if let Some(state) = handler.handle(&mut extra_state, &mut ctx, msg.payload(), get_headers(&msg)).await {
                        if let Err(e) = state_store.lock().await.set(&state_namespace, key, state.clone()).await {
                            panic!("Error updating state store on topic {}: {:?}", msg.topic(), e);
                        }
                    }

                    let msg_topic = msg.topic().to_string();
                    let msg_offset = msg.offset();

                    let helper_clone = partition_handler.clone();
                    let output_topics_set = output_topics_set.clone();
                    let sends = ctx.to_send().into_iter().map(move |s| {
                        assert!(output_topics_set.contains(&s.topic)); //TODO: Should we remove this assertion?
                        helper_clone
                            .send_result(FutureRecord::to(&s.topic).key(&s.key).payload(&s.payload).headers(make_owned_headers(s.headers)))
                            .unwrap()
                    }); 

                    let mut helper_clone = partition_handler.clone();
                    let offset_lock_for_partitioned_topic = topic_partition_offset_locks.entry(msg_topic.clone()).or_insert_with(|| Arc::new(Mutex::new(msg_offset))).clone();  
                    tracing::debug!("Handled kafka message on {}:{}", state_namespace, key);
                    tokio::spawn(async move {
                        join_all(sends).await;

                        // Note: the lock is required because the store offset func assumes that the offsets
                        // Are monotonically increasing, otherwise the commit interval can commit an older offset
                        let mut lock = offset_lock_for_partitioned_topic.lock().await;
                        if msg_offset > *lock {
                            *lock = msg_offset;
                        }
                        helper_clone.store_offset(&msg_topic, *lock).unwrap();
                    });
                }
            }));
        }


        partitions_barrier.wait().await;
        let result = select!(
            f =  futures::future::try_join_all(tokio_tasks.iter_mut()) => { 
                for t in tokio_tasks {
                    t.abort();
                }

                f.map_err(
                    |e| format!("Kast processor {} panicked: {:?}", self.state_namespace, format_join_error(e))
                ).map(|_| ()) 
            },
            f = self.helper.start() => f
        );

        result
    }

    pub async fn join(&self) -> Result<(), String> {
        self.helper.wait_to_finish().await
    }
}

fn get_headers(message: &impl Message) -> HashMap<String, String> {
    let mut result = HashMap::new();

    if let Some(headers) = message.headers() {
        for (k, v) in (0..headers.count())
            .filter_map(|i| headers.get(i))
            .map(|(k, v)| (k.to_string(), String::from_utf8_lossy(v).to_string()))
        {
            result.insert(k, v);
        }
    }

    result
}

fn make_owned_headers(headers: HashMap<String, String>) -> OwnedHeaders {
    let mut result = OwnedHeaders::new();

    for (k, v) in headers.into_iter() {
        result = result.add(&k, &v);
    }

    result
}