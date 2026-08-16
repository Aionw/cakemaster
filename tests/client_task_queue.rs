use cakemaster::server::{ClientTaskQueue, ClientTaskQueueError};

#[tokio::test]
async fn tx_rx_preserve_fifo_order() {
    let (tx, mut rx) = ClientTaskQueue::new(4).unwrap().into_parts();

    tx.send("first").await.unwrap();
    tx.send("second").await.unwrap();

    assert_eq!(rx.recv_many(4).await.unwrap(), vec!["first", "second"]);
}

#[tokio::test]
async fn each_client_queue_is_independent() {
    let (client_a_tx, mut client_a_rx) = ClientTaskQueue::new(2).unwrap().into_parts();
    let (client_b_tx, mut client_b_rx) = ClientTaskQueue::new(2).unwrap().into_parts();

    client_a_tx.send("a").await.unwrap();
    client_b_tx.send("b").await.unwrap();

    assert_eq!(client_a_rx.recv().await, Some("a"));
    assert_eq!(client_b_rx.recv().await, Some("b"));
}

#[tokio::test]
async fn send_applies_async_backpressure_per_client() {
    let (tx, mut rx) = ClientTaskQueue::new(1).unwrap().into_parts();
    tx.send("first").await.unwrap();

    let blocked_send = {
        let tx = tx.clone();
        tokio::spawn(async move { tx.send("second").await })
    };
    tokio::task::yield_now().await;
    assert!(!blocked_send.is_finished());

    assert_eq!(rx.recv().await, Some("first"));
    blocked_send.await.unwrap().unwrap();
    assert_eq!(rx.recv().await, Some("second"));
}

#[tokio::test]
async fn cancelling_receive_does_not_consume_a_later_task() {
    let (tx, mut rx) = ClientTaskQueue::new(1).unwrap().into_parts();

    tokio::select! {
        task = rx.recv() => panic!("unexpected task: {task:?}"),
        () = tokio::task::yield_now() => {}
    }

    tx.send("task").await.unwrap();
    assert_eq!(rx.recv().await, Some("task"));
}

#[tokio::test]
async fn closing_rx_rejects_sends_but_drains_buffered_tasks() {
    let (tx, mut rx) = ClientTaskQueue::new(2).unwrap().into_parts();
    tx.send("buffered").await.unwrap();

    rx.close();
    assert!(rx.is_closed());
    assert!(tx.is_closed());
    assert!(tx.send("late").await.is_err());
    assert_eq!(rx.recv().await, Some("buffered"));
    assert_eq!(rx.recv().await, None);
}

#[tokio::test]
async fn dropping_all_tx_handles_closes_rx() {
    let (tx, mut rx) = ClientTaskQueue::<()>::new(1).unwrap().into_parts();
    let cloned = tx.clone();
    drop(tx);
    assert!(!rx.is_closed());

    drop(cloned);
    assert_eq!(rx.recv().await, None);
    assert!(rx.is_closed());
}

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
