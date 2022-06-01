use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

#[derive(thiserror::Error, Debug, Clone)]
pub enum StateStoreError {
    #[error("State store has no key {0}")]
    MissingKey(String),

    #[error("State store internal error: {0}")]
    Internal(String),
}


/// A type that implements this trait is a KV store with a slight abstraction that allows us to restrict access to specific sub-keys.
/// For example, if we want to store both a small application state (e.g. keypoints progress in pairing), but also a much bigger
/// dataset that only a small slice of it is relevant for a given moment (e.g. a several hours worth of keypoints), we might
/// have two wrapper types `PairingState` and `KeypointsState`, both of which hide away a `StateStore` implementor, and expose different
/// methods for access. Those methods internally will provide the `namespace` value to `get` and `set` to determine where in the store
/// each kind of data is stored.
#[async_trait::async_trait]
pub trait StateStore<V>
where
    V: Clone,
{
    async fn get(&self, namespace: &str, k: &str) -> Result<V, StateStoreError>;
    async fn set(&mut self, namespace: &str, k: &str, v: V) -> Result<(), StateStoreError>;
}

#[async_trait::async_trait]
impl<V> StateStore<V> for HashMap<String, V>
where
    V: Send + Sync + Clone,
{
    async fn get(&self, namespace: &str, k: &str) -> Result<V, StateStoreError> {
        self.get(&format!("{}::{}", namespace, k))
            .cloned()
            .ok_or_else(|| StateStoreError::MissingKey(k.to_string()))
    }
    async fn set(&mut self, namespace: &str, k: &str, v: V) -> Result<(), StateStoreError> {
        let _ = self.insert(format!("{}::{}", namespace, k), v);
        Ok(())
    }
}

#[async_trait::async_trait]
impl<V> StateStore<V> for Arc<RwLock<HashMap<String, V>>>
where
    V: Send + Sync + Clone,
{
    async fn get(&self, namespace: &str, k: &str) -> Result<V, StateStoreError>
where {
        self.read()
            .await
            .get(&format!("{}::{}", namespace, k))
            .cloned()
            .ok_or_else(|| StateStoreError::MissingKey(k.to_string()))
    }

    async fn set(&mut self, namespace: &str, k: &str, v: V) -> Result<(), StateStoreError> {
        let _ = self.write().await.insert(format!("{}::{}", namespace, k), v);
        Ok(())
    }
}
