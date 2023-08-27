use std::fmt::Display;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// For the reason that I have used `async_trait` here,
/// see https://blog.rust-lang.org/inside-rust/2022/11/17/async-fn-in-trait-nightly.html#limitation-spawning-from-generics.
#[async_trait]
pub trait DiskManager<K, V>: Send + Sync + 'static
where
    K: Display,
{
    async fn create(&self, key: &K);
    async fn read(&self, key: &K) -> V;
    async fn write(&self, key: &K, value: &V);
    async fn remove(&self, key: &K);

    async fn read_as_stream(&self, key: &K) -> impl AsyncSeekExt + AsyncReadExt + Unpin;
    async fn write_as_stream(&self, key: &K) -> impl AsyncSeekExt + AsyncWriteExt + Unpin;
}
