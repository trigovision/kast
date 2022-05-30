use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use tokio::sync::RwLock;

#[async_trait::async_trait]
pub trait StateStore<K, V>
where
    K: Eq + Hash,
    V: Clone,
{
    async fn get<Q: ?Sized + Sync>(&self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash;
    async fn set(&mut self, k: K, v: V) -> Option<V>;
}

#[async_trait::async_trait]
impl<K, V> StateStore<K, V> for HashMap<K, V>
where
    K: Eq + Hash + Sync + Send,
    V: Send + Sync + Clone,
{
    async fn get<Q: ?Sized + Sync>(&self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.get(k).cloned()
    }
    async fn set(&mut self, k: K, v: V) -> Option<V> {
        self.insert(k, v)
    }
}

#[async_trait::async_trait]
impl<K, V> StateStore<K, V> for Arc<RwLock<HashMap<K, V>>>
where
    K: Eq + Hash + Sync + Send,
    V: Send + Sync + Clone,
{
    async fn get<Q: ?Sized + Sync>(&self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.read().await.get(k).cloned()
    }

    async fn set(&mut self, k: K, v: V) -> Option<V> {
        self.write().await.insert(k, v)
    }
}
