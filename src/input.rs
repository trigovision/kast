use crate::{context::Context, encoders::Encoder};
use dyn_clone::DynClone;
use futures::Future;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;

#[async_trait::async_trait]
pub trait GenericInput<T, S>: DynClone + Sync + Send {
    fn topic(&self) -> String;
    async fn handle(&self, state: &mut S, ctx: &mut Context<T>, data: Option<&[u8]>);
}
dyn_clone::clone_trait_object!(<T, S> GenericInput<T, S>);

pub trait Handler<'a, T, S, R> {
    type Future: Future<Output = ()> + Send + 'a;

    fn call(self, state: &'a mut S, ctx: &'a mut Context<T>, req: R) -> Self::Future;
}

impl<'a, Fut, T, S, R, F> Handler<'a, T, S, R> for F
where
    F: FnOnce(&'a mut S, &'a mut Context<T>, R) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
    T: Send + 'static,
    S: Send + Sync + 'static,
{
    type Future = Fut;

    fn call(self, state: &'a mut S, ctx: &'a mut Context<T>, req: R) -> Self::Future {
        (self)(state, ctx, req)
    }
}

#[derive(Clone)]
pub struct Input<R, S, F, E>
where
    E: Encoder<In = R>,
{
    topic: String,
    callback: F,
    encoder: E,
    _marker: PhantomData<S>,
}

impl<R, S, F, E> Input<R, S, F, E>
where
    E: Encoder<In = R>,
{
    pub fn new(topic: String, encoder: E, callback: F) -> Box<Self> {
        Box::new(Input {
            topic,
            callback,
            encoder,
            _marker: PhantomData,
        })
    }
}

#[async_trait::async_trait]
impl<R, T, S, F, E> GenericInput<T, S> for Input<R, S, F, E>
where
    for<'a> F: Handler<'a, T, S, R> + Send + Sync + Copy,
    E: Encoder<In = R> + Sync + Clone + Send + 'static,
    R: Sync + Send + Clone + DeserializeOwned + 'static + std::fmt::Debug,
    T: Clone + Send + Sync + 'static,
    S: Send + Sync + Clone,
{
    fn topic(&self) -> String {
        self.topic.clone()
    }

    async fn handle(&self, state: &mut S, ctx: &mut Context<T>, data: Option<&[u8]>) {
        let msg = self.encoder.encode(data);
        self.callback.call(state, ctx, msg.clone()).await;
    }
}
