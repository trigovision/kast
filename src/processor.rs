use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
};

use futures::{future::join_all, stream::SelectAll, StreamExt};
use rdkafka::{producer::FutureRecord, Message};

use crate::{
    context::Context, input::GenericInput, output::Output,
    processor_helper::KafkaProcessorImplementor, state_store::StateStore,
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
    F1: FnOnce() -> TStore + Copy + Send + 'static,
    F2: FnOnce() -> TExtraState + Copy + Send + 'static,
    TExtraState: Send + 'static,
    //TODO: Refine the constraints?
    H: KafkaProcessorImplementor + Sync + 'static,
    <H::DeliveryFutureType as futures::Future>::Output: Send,
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

    pub async fn run(&mut self) {
        let input_topcis_set: HashSet<String> = self.inputs.keys().cloned().collect();
        let output_topcis_set: HashSet<String> =
            self.outputs.iter().map(|o| o.topic().to_string()).collect();
        let num_partitions = self
            .helper
            .validate_inputs_outputs(&input_topcis_set, &output_topcis_set)
            .unwrap();

        for partition in 0..num_partitions {
            let partitioned_topics: Vec<(_, Box<dyn GenericInput<TState, TExtraState>>)> = self
                .inputs
                .iter()
                .map(|(topic_name, input_handler)| {
                    (
                        self.helper
                            .create_partitioned_consumer(topic_name, partition as u16), //TODO: Does it work with multiple consumers?
                        input_handler.clone(),
                    )
                })
                .collect();

            let gen = self.state_store_gen.clone();
            let gen2 = self.extra_state_gen.clone();

            let helper = self.helper.clone();
            tokio::spawn(async move {
                let mut state_store = gen();
                let mut extra_state = gen2();

                let iter = partitioned_topics
                    .into_iter()
                    .map(|(a, h)| Box::pin(a).map(move |msg| (h.clone(), msg)));

                let mut select_all = SelectAll::from_iter(iter);

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

                    // let stream_consumer = stream_consumer.clone();
                    let msg_topic = msg.topic().to_string();
                    let msg_partition = msg.partition();
                    let msg_offset = msg.offset();

                    // NOTE: It is extremly important that this will happen here and not inside the spawn to gurantee order of msgs!
                    let helper_clone = helper.clone();

                    let sends = ctx.to_send().into_iter().map(move |s| {
                        helper_clone
                            .send_result(FutureRecord::to(&s.topic).key(&s.key).payload(&s.payload))
                            .unwrap()
                    });

                    let helper_clone = helper.clone();
                    tokio::spawn(async move {
                        join_all(sends).await;
                        helper_clone
                            .store_offset(&msg_topic, msg_partition, msg_offset)
                            .unwrap();
                    });
                }
            });
        }
    }

    pub async fn join(self) {
        let input_topcis_set: HashSet<String> = self.inputs.keys().cloned().collect();

        self.helper
            .wait_to_finish(&input_topcis_set)
            .await
            .unwrap()
            .unwrap();
    }
}
