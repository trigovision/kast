use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
    sync::Arc,
};

use futures::{future::join_all, stream::select_all, StreamExt};
use rdkafka::{producer::FutureRecord, Message};
use tokio::sync::Barrier;

use crate::{
    context::Context,
    input::GenericInput,
    output::Output,
    processor_helper::{KafkaProcessorImplementor, PartitionHelper},
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
    //TODO: Refine the constraints?
    H: KafkaProcessorImplementor + Sync + 'static,
    // <H::DeliveryFutureType as futures::Future>::Output: Send,
    <<<H as KafkaProcessorImplementor>::PartitionHelperType as PartitionHelper>::DeliveryFutureType as futures::Future>::Output: std::marker::Send,
    <<H as KafkaProcessorImplementor>::PartitionHelperType as PartitionHelper>::M: Sync + Send
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

    pub fn helper(&mut self) -> &mut H {
        &mut self.helper
    }

    pub async fn start(&mut self) {
        let input_topcis_set: HashSet<String> = self.inputs.keys().cloned().collect();
        let output_topcis_set: HashSet<String> =
            self.outputs.iter().map(|o| o.topic().to_string()).collect();

        self.helper
            .prepare_input_outputs(&input_topcis_set, &output_topcis_set)
            .unwrap();

        let num_partitions = self.helper.ensure_copartitioned().unwrap();

        let b = Arc::new(Barrier::new(num_partitions + 1)); //TODO: Change barrier to something else?
        for partition in 0..num_partitions {
            let partitioned_topics: Vec<Box<dyn GenericInput<TState, TExtraState>>> = self
                .inputs
                .iter()
                .map(|(_, input_handler)| input_handler.clone())
                .collect();

            let gen = self.state_store_gen.clone();
            let gen2 = self.extra_state_gen.clone();

            let output_topcis_set = output_topcis_set.clone();
            let partition_handler = self.helper.create_partitioned_consumer(partition as i32);
            let b_clone = b.clone();
            tokio::spawn(async move {
                let mut state_store = gen();
                let mut extra_state = gen2();
                let iter = partitioned_topics.into_iter().map(|h| {
                    Box::pin(partition_handler.create_partitioned_topic_stream(&h.topic()))
                    .map(move |msg| (h.clone(), msg))
                });

                let mut select_all = select_all(iter);
                b_clone.wait().await;
                while let Some((h, msg)) = select_all.next().await {
                    let msg = msg.unwrap();

                    //TODO: Key serializer?
                    let key = std::str::from_utf8(msg.key().unwrap()).unwrap();

                    let state = state_store.get(key).await.map(|s| s.clone());
                    let mut ctx = Context::new(key, state);

                    h.handle(&mut extra_state, &mut ctx, msg.payload()).await;

                    if let Some(state) = &ctx.get_new_state() {
                        state_store.set(key.to_string(), state.clone()).await;
                    }

                    // let stream_consumer = stream_consumer.clone();
                    let msg_topic = msg.topic().to_string();
                    let msg_partition = msg.partition();
                    let msg_offset = msg.offset();

                    // NOTE: It is extremly important that this will happen here and not inside the spawn to gurantee order of msgs!
                    let helper_clone = partition_handler.clone();



                    let output_topcis_set = output_topcis_set.clone();
                    let sends = ctx.to_send().into_iter().map(move |s| {
                        assert!(output_topcis_set.contains(&s.topic.to_string())); //TODO: Should we remove this assertion?
                        helper_clone
                            .send_result(FutureRecord::to(&s.topic).key(&s.key).payload(&s.payload))
                            .unwrap()
                    });



                    let helper_clone = partition_handler.clone();
                    tokio::spawn(async move {
                        join_all(sends).await;
                        helper_clone
                            .store_offset(&msg_topic, msg_partition, msg_offset)
                            .unwrap();
                    });
                }
            });
        }
        b.wait().await;
    }

    // TODO: Can we move it back to run and support something to notify initialization?
    pub async fn join(self) {
        let input_topcis_set: HashSet<String> = self.inputs.keys().cloned().collect();

        self.helper
            .wait_to_finish(&input_topcis_set)
            .await
            .unwrap()
            .unwrap();
    }
}
