use super::*;
use crate::runtime::{AsyncStream, WorkerHandle};

#[tokio::test]
async fn cancelled_checkout_returns_connection_and_requests_retry() {
    let (requester, mut receiver) = connection_requester::channel(WorkerHandle::new_mocked());
    let mut checkout = Box::pin(requester.request());
    assert!(futures::poll!(&mut checkout).is_pending());
    let request = receiver.recv().await.unwrap();
    assert!(!request.is_abandoned());
    drop(checkout);

    #[cfg(feature = "tracing-unstable")]
    let event_emitter = CmapEventEmitter::new(None, ObjectId::new(), None);
    #[cfg(not(feature = "tracing-unstable"))]
    let event_emitter = CmapEventEmitter::new(None);
    let mut connection = PooledConnection::new(
        PendingConnection {
            id: 1,
            address: ServerAddress::default(),
            generation: PoolGeneration::normal(),
            event_emitter,
            time_created: Instant::now(),
            cancellation_receiver: None,
        },
        AsyncStream::Null,
    );
    let connection_id = connection.id;
    let (manager, _) = manager::channel();
    connection.mark_checked_out(manager.clone(), None);
    let mut available = VecDeque::new();
    assert_eq!(
        ConnectionPoolWorker::fulfill_checkout(request, connection, &mut available),
        ControlFlow::Continue(())
    );
    assert_eq!(available.len(), 1);

    let mut checkout = Box::pin(requester.request());
    assert!(futures::poll!(&mut checkout).is_pending());
    let request = receiver.recv().await.unwrap();
    let mut connection = available.pop_back().unwrap();
    connection.mark_checked_out(manager, None);
    assert_eq!(
        ConnectionPoolWorker::fulfill_checkout(request, connection, &mut available),
        ControlFlow::Break(())
    );
    assert_eq!(checkout.await.unwrap_pooled_connection().id, connection_id);
    assert!(available.is_empty());
}
