use crate::{context::Context, encoders::Decoder};
use dyn_clone::DynClone;
use futures::Future;

use serde::de::DeserializeOwned;
use std::{marker::PhantomData, collections::HashMap};

#[async_trait::async_trait]
pub trait GenericInput<T, S, TStore>: DynClone + Sync + Send {
    fn topic(&self) -> String;
    async fn handle(&self, state: &mut S, ctx: &mut Context<TStore, T>, data: Option<&[u8]>, headers: HashMap<String, String>) -> Option<T>;
}
dyn_clone::clone_trait_object!(<T, S, TStore> GenericInput<T, S, TStore>);

pub trait Handler<'a, T, S, R, TStore, VArgs> {
    type Future: Future<Output = Option<T>> + Send + 'a;

    fn call(self, state: &'a mut S, ctx: &'a mut Context<TStore, T>, req: R, headers: HashMap<String, String>) -> Self::Future;
}

#[derive(Clone)]
pub struct Input<R, S, F, E, VArgs>
where
    E: Decoder<In = R>,
{
    topic: String,
    encoder: E,
    callback: F,
    _marker: PhantomData<(S, VArgs)>,
}

impl<R, S, F, E, VArgs> Input<R, S, F, E, VArgs>
where
    E: Decoder<In = R>,
{
    pub fn new(topic: &str, encoder: E, callback: F) -> Box<Self> {
        Box::new(Input {
            topic: topic.to_string(),
            encoder,
            callback,
            _marker: PhantomData,
        })
    }
}

#[async_trait::async_trait]
impl<R, T, S, F, E, TStore, VArgs> GenericInput<T, S, TStore> for Input<R, S, F, E, VArgs>
where
    for<'a> F: Handler<'a, T, S, R, TStore, VArgs> + Send + Sync + Copy,
    E: Decoder<In = R> + Sync + Clone + Send + 'static,
    R: Sync + Send + Clone + DeserializeOwned + 'static + std::fmt::Debug,
    T: Clone + Send + Sync + 'static,
    TStore: Send + Sync,
    S: Send + Sync + Clone,
    VArgs: Clone + Send + Sync,
{
    fn topic(&self) -> String {
        self.topic.clone()
    }

    async fn handle(&self, state: &mut S, ctx: &mut Context<TStore, T>, data: Option<&[u8]>, headers: HashMap<String, String>) -> Option<T> {
        let msg = self.encoder.encode(data);
        self.callback.call(state, ctx, msg, headers).await
    }
}

#[derive(Clone)]
pub struct MessageAndContextAndStateAndHeaders;

impl<'a, Fut, T, S, R, TStore, F> Handler<'a, T, S, R, TStore, MessageAndContextAndStateAndHeaders> for F
where
    F: FnOnce(&'a mut S, &'a mut Context<TStore, T>, R, HashMap<String, String>) -> Fut + Send,
    Fut: Future<Output = Option<T>> + Send + 'a,
    T: Send + 'static,
    TStore: 'a,
    S: Send + Sync + 'static,
{
    type Future = Fut;

    fn call(self, state: &'a mut S, ctx: &'a mut Context<TStore, T>, req: R, headers: HashMap<String, String>) -> Self::Future {
        (self)(state, ctx, req, headers)
    }
}
