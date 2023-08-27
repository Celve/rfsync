pub trait Iterator {
    type Item;

    async fn next(&mut self) -> Option<Self::Item>;
}
