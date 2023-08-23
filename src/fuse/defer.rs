use std::future::Future;

struct Defer<T: Future + Send + 'static>(Option<T>)
where
    T: Future + Send + 'static,
    T::Output: Send + 'static;

impl<T> Drop for Defer<T>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    fn drop(&mut self) {
        self.0.take().map(|f| tokio::spawn(f));
    }
}

pub fn defer<T>(f: T) -> impl Drop
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    Defer(Some(f))
}
