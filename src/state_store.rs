use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

#[async_trait::async_trait]
pub trait StateStore<K, V>
where
    K: Eq + Hash,
{
    async fn get<Q: ?Sized + Sync>(&self, k: &Q) -> Option<&V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Eq + Hash;
    async fn set(&mut self, k: K, v: V) -> Option<V>;
}

pub struct InMemoryStateStore<K, V> {
    cache: HashMap<K, V>,
}

impl<K, V> InMemoryStateStore<K, V> {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl<K, V> StateStore<K, V> for InMemoryStateStore<K, V>
where
    K: Eq + Hash + Sync + Send,
    V: Send + Sync,
{
    async fn get<Q: ?Sized + Sync>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.cache.get(k)
    }
    async fn set(&mut self, k: K, v: V) -> Option<V> {
        self.cache.insert(k, v)
    }
}
