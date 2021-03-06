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

pub trait Handler<'a, T, S, R, VArgs> {
    type Future: Future<Output = ()> + Send + 'a;

    fn call(self, state: &'a mut S, ctx: &'a mut Context<T>, req: R) -> Self::Future;
}

#[derive(Clone)]
pub struct Input<R, S, F, E, VArgs>
where
    E: Encoder<In = R>,
{
    topic: String,
    encoder: E,
    callback: F,
    _marker: PhantomData<(S, VArgs)>,
}

impl<R, S, F, E, VArgs> Input<R, S, F, E, VArgs>
where
    E: Encoder<In = R>,
{
    pub fn new(topic: String, encoder: E, callback: F) -> Box<Self> {
        Box::new(Input {
            topic,
            encoder,
            callback,
            _marker: PhantomData,
        })
    }
}

#[async_trait::async_trait]
impl<R, T, S, F, E, VArgs> GenericInput<T, S> for Input<R, S, F, E, VArgs>
where
    for<'a> F: Handler<'a, T, S, R, VArgs> + Send + Sync + Copy,
    E: Encoder<In = R> + Sync + Clone + Send + 'static,
    R: Sync + Send + Clone + DeserializeOwned + 'static + std::fmt::Debug,
    T: Clone + Send + Sync + 'static,
    S: Send + Sync + Clone,
    VArgs: Clone + Send + Sync,
{
    fn topic(&self) -> String {
        self.topic.clone()
    }

    async fn handle(&self, state: &mut S, ctx: &mut Context<T>, data: Option<&[u8]>) {
        let msg = self.encoder.encode(data);
        self.callback.call(state, ctx, msg).await;
    }
}

#[derive(Clone)]
pub struct MessageOnly;

impl<'a, Fut, T, S, R, F> Handler<'a, T, S, R, MessageOnly> for F
where
    F: FnOnce(R) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
    T: Send + 'static,
    S: Send + Sync + 'static,
{
    type Future = Fut;

    fn call(self, _state: &'a mut S, _ctx: &'a mut Context<T>, req: R) -> Self::Future {
        (self)(req)
    }
}

#[derive(Clone)]
pub struct MessageAndState;

impl<'a, Fut, T, S, R, F> Handler<'a, T, S, R, MessageAndState> for F
where
    F: FnOnce(&'a mut S, R) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
    T: Send + 'static,
    S: Send + Sync + 'static,
{
    type Future = Fut;

    fn call(self, state: &'a mut S, _ctx: &'a mut Context<T>, req: R) -> Self::Future {
        (self)(state, req)
    }
}

#[derive(Clone)]
pub struct MessageAndContext;

impl<'a, Fut, T, S, R, F> Handler<'a, T, S, R, MessageAndContext> for F
where
    F: FnOnce(&'a mut Context<T>, R) -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'a,
    T: Send + 'static,
    S: Send + Sync + 'static,
{
    type Future = Fut;

    fn call(self, _state: &'a mut S, ctx: &'a mut Context<T>, req: R) -> Self::Future {
        (self)(ctx, req)
    }
}

#[derive(Clone)]
pub struct MessageAndContextAndState;

impl<'a, Fut, T, S, R, F> Handler<'a, T, S, R, MessageAndContextAndState> for F
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
