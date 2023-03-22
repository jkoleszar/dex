use std::fmt::Display;
use std::future::Future;
use std::marker::Unpin;
use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;

pub trait LogIfError<T, E: Display>: Into<Result<T, E>> {
    fn log_if_error(self, context: &str) -> Result<T, E> {
        let result = self.into();
        if let Err(e) = &result {
            log::error!("{context}: {e}");
        }
        result
    }
}
impl<T, E: Display> LogIfError<T, E> for Result<T, E> {}

#[async_trait]
pub trait LogSlowFuture<T>: Future<Output = T> + Unpin {
    async fn log_slow<'a>(
        &'a mut self,
        timeout: Duration,
        message: &'static str,
    ) -> Pin<Box<dyn Future<Output = T> + 'a>> {
        Box::pin(async move {
            let printer = tokio::spawn(async move {
                tokio::time::sleep(timeout).await;
                log::info!("still waiting: {message}");
            });
            let r = self.await;
            printer.abort();
            r
        })
    }
}
impl<T, U: Future<Output = T> + Unpin> LogSlowFuture<T> for U {}
