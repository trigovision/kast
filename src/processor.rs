use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
    sync::Arc,
};

use futures::{future::join_all, stream::select_all, StreamExt};
use rdkafka::{producer::FutureRecord, Message};
use tokio::sync::{Barrier, Mutex};

use crate::{
    context::Context,
    input::GenericInput,
    output::Output,
    processor_helper::{IntoKafkaStream, KafkaProcessorImplementor, PartitionHelper},
    state_store::StateStore,
};

pub struct Processor<TState, TExtraState, TStore, F1, F2, H> {
    helper: H,
    inputs: HashMap<String, Box<dyn GenericInput<TState, TExtraState>>>,
    state_store_gen: F1,
    extra_state_gen: F2,
    outputs: Vec<Output>,
    _marker: PhantomData<(TState, TStore)>,
}

impl<TState, TExtraState, TStore, F1, F2, H> Processor<TState, TExtraState, TStore, F1, F2, H>
where
    TState: Clone + Send + Sync + 'static,
    TStore: StateStore<String, TState> + Send + Sync,
    F1: FnOnce() -> TStore + Send + Clone + 'static,
    F2: FnOnce() -> TExtraState + Clone + Send + 'static,
    TExtraState: Send + 'static,
    H: KafkaProcessorImplementor + Sync + 'static,
    <<<H as KafkaProcessorImplementor>::PartitionHelperType as PartitionHelper>::DeliveryFutureType as futures::Future>::Output: std::marker::Send,
{
    pub fn new(
        helper: H,
        inputs: Vec<Box<dyn GenericInput<TState, TExtraState>>>,
        outputs: Vec<Output>,
        state_store_gen: F1,
        extra_state_gen: F2,
    ) -> Self {
        let inputs = inputs
            .into_iter()
            .map(|input| (input.topic(), input))
            .collect();

        Processor {
            helper,
            inputs,
            outputs,
            state_store_gen,
            extra_state_gen,
            _marker: PhantomData,
        }
    }

    pub async fn start(&mut self) {
        let input_topcis_set: HashSet<String> = self.inputs.keys().cloned().collect();
        let output_topcis_set: HashSet<String> =
            self.outputs.iter().map(|o| o.topic().to_string()).collect();

        self.helper
            .subscribe_inputs(&input_topcis_set)
            .unwrap();

        let num_partitions = self.helper.ensure_copartitioned().unwrap();

        let partitions_barrier = Arc::new(Barrier::new(num_partitions + 1));
        for partition in 0..num_partitions as i32 {
            let inputs = self.inputs.clone();               
            let output_topcis_set = output_topcis_set.clone();
            let partition_handler = self.helper.create_partitioned_consumer(partition as i32);
            let partitions_barrier_clone = partitions_barrier.clone();
            let gen = self.state_store_gen.clone();
            let gen2 = self.extra_state_gen.clone();
            tokio::spawn(async move {
                let mut state_store = gen();
                let mut extra_state = gen2();
                let stream_gens: Vec<_> = inputs.keys().into_iter().map(|topic| {
                    partition_handler.create_partitioned_topic_stream(&topic)
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
                    
                    //TODO: Key serializer?
                    let key = std::str::from_utf8(msg.key().unwrap()).unwrap();

                    let state = state_store.get(key).await.map(|s| s.clone());
                    let mut ctx = Context::new(key, state);

                    let handler = inputs.get(msg.topic()).expect("streams are created from inputs keys");
                    handler.handle(&mut extra_state, &mut ctx, msg.payload()).await;

                    if let Some(state) = &ctx.get_new_state() {
                        state_store.set(key.to_string(), state.clone()).await;
                    }

                    // let stream_consumer = stream_consumer.clone();
                    let msg_topic = msg.topic().to_string();
                    let msg_offset = msg.offset();

                    let helper_clone = partition_handler.clone();
                    let output_topcis_set = output_topcis_set.clone();
                    let sends = ctx.to_send().into_iter().map(move |s| {
                        assert!(output_topcis_set.contains(&s.topic.to_string())); //TODO: Should we remove this assertion?
                        helper_clone
                            .send_result(FutureRecord::to(&s.topic).key(&s.key).payload(&s.payload))
                            .unwrap()
                    }); 

                    let mut helper_clone = partition_handler.clone();
                    let offset_lock_for_partitioned_topic = topic_partition_offset_locks.entry(msg_topic.clone()).or_insert(Arc::new(Mutex::new(msg_offset))).clone();  
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
            });
        }

        partitions_barrier.wait().await;
    }

    pub async fn join(self) {
        self.helper
            .wait_to_finish()
            .await
            .unwrap()
            .unwrap();
    }
}
