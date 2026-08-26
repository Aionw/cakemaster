use cakemaster::server::{ClientTaskQueue, ClientTaskQueueError};

#[tokio::test]
async fn invalid_capacity_and_batch_size_are_rejected() {
    assert_eq!(
        ClientTaskQueue::<()>::new(0).unwrap_err(),
        ClientTaskQueueError::ZeroCapacity
    );

    let (_tx, mut rx) = ClientTaskQueue::<()>::new(1).unwrap().into_parts();
    assert_eq!(
        rx.recv_many(0).await,
        Err(ClientTaskQueueError::ZeroBatchSize)
    );
}
