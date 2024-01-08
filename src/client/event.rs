#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum EventHandler<T> {
    TokioMpsc(tokio::sync::mpsc::Sender<T>),
}

impl<T: Send + Sync> From<tokio::sync::mpsc::Sender<T>> for EventHandler<T> {
    fn from(value: tokio::sync::mpsc::Sender<T>) -> Self {
        Self::TokioMpsc(value)
    }
}

impl<T> EventHandler<T> {
    pub(crate) async fn handle(&self, event: T) {
        match self {
            Self::TokioMpsc(sender) => {
                let _ = sender.send(event).await;
            }
        }
    }
}