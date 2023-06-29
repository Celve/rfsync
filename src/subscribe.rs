use tokio::sync::mpsc::Receiver;

/// A observer keeps its eyes on something that might be changed.
/// It notices all subscribers when changes occur.
pub trait Observer {
    type Event;

    /// Create a oneshot channel, returning the receiver the subscriber needs.
    /// The observer would not notice the existence of the subscriber.
    fn observe(&mut self) -> Receiver<Self::Event>;
}

/// A subscriber subscribes to an observer, and receives events from it.
pub trait Subscriber {
    fn subscribe(&mut self, observer: impl Observer);
}
