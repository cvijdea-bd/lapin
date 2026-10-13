#![cfg(feature = "tokio")]

//! Checks that channel recovery replays the QoS settings, and does so before
//! recreating the consumers.
//!
//! Runs against a real broker: the client connects through a TCP relay that
//! copies bytes in both directions, so the AMQP peer is RabbitMQ itself, and
//! the relay cuts its sockets to trigger recovery.
//!
//! The check is behavioral: a consumer with `prefetch_count = 1` and three
//! messages waiting in the queue must still receive a single unacked delivery
//! after recovery. Without the replay the broker has no prefetch limit and
//! pushes every message at once. This also pins the ordering down, as the
//! broker applies a qos to the consumers declared after it only.

mod common;

use common::Relay;
use futures_lite::stream::StreamExt;
use lapin::{
    BasicProperties, Connection, ConnectionProperties, Consumer,
    message::Delivery,
    options::{
        BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, QueueDeclareOptions,
        QueueDeleteOptions,
    },
    types::FieldTable,
};
use std::time::{Duration, Instant, SystemTime};

const TIMEOUT: Duration = Duration::from_secs(10);
/// How long we wait to be convinced that no extra delivery is coming.
const QUIET: Duration = Duration::from_secs(1);
const MESSAGES: usize = 3;

/// Waits for the next delivery, ignoring the errors the consumer reports while
/// the connection is being recovered. Returns `None` on timeout.
async fn next_delivery(consumer: &mut Consumer, timeout: Duration) -> Option<Delivery> {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.checked_duration_since(Instant::now())?;
        match tokio::time::timeout(remaining, consumer.next()).await {
            Ok(Some(Ok(delivery))) => return Some(delivery),
            // Recovery in progress: the consumer may surface the connection error.
            Ok(Some(Err(_))) => {}
            Ok(None) => return None,
            Err(_) => return None,
        }
    }
}

#[tokio::test]
async fn recovery_replays_qos_before_consumers() {
    let relay = Relay::start();

    let properties = ConnectionProperties::default()
        .enable_auto_recover()
        .configure_backoff(|backoff| {
            backoff
                .with_min_delay(Duration::from_millis(10))
                .with_max_delay(Duration::from_millis(100))
                .with_max_times(64)
        });
    let connection =
        tokio::time::timeout(TIMEOUT, Connection::connect_uri(relay.uri(), properties))
            .await
            .expect("connection stayed pending")
            .expect("connection failed");
    let channel = connection.create_channel().await.expect("create_channel");

    let queue = format!(
        "lapin-recovery-qos-{}",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    channel
        .queue_declare(
            queue.clone().into(),
            // Recent RabbitMQ versions reject transient non-exclusive queues.
            QueueDeclareOptions::durable(),
            FieldTable::default(),
        )
        .await
        .expect("queue_declare");
    channel
        .basic_qos(1, BasicQosOptions::default())
        .await
        .expect("basic_qos");
    for _ in 0..MESSAGES {
        channel
            .basic_publish(
                "".into(),
                queue.clone().into(),
                BasicPublishOptions::default(),
                b"payload",
                BasicProperties::default(),
            )
            .await
            .expect("basic_publish")
            .await
            .expect("publish confirm");
    }

    let mut consumer = channel
        .basic_consume(
            queue.clone().into(),
            "recovery-qos".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("basic_consume");

    // Baseline: the prefetch limit holds on the initial connection.
    next_delivery(&mut consumer, TIMEOUT)
        .await
        .expect("no delivery before recovery");
    assert!(
        next_delivery(&mut consumer, QUIET).await.is_none(),
        "broker exceeded prefetch_count before recovery"
    );

    // Cut the relay and let recovery redial the broker. The unacked delivery is
    // requeued, so all MESSAGES messages are pending again.
    relay.cut();
    let delivery = next_delivery(&mut consumer, TIMEOUT)
        .await
        .expect("no delivery after recovery");
    assert!(
        next_delivery(&mut consumer, QUIET).await.is_none(),
        "qos was not replayed: broker exceeded prefetch_count after recovery"
    );
    assert_eq!(
        channel.status().qos().prefetch_count,
        Some(1),
        "recovered channel lost its qos settings"
    );

    delivery.ack(Default::default()).await.expect("ack");
    channel
        .queue_delete(queue.into(), QueueDeleteOptions::default())
        .await
        .expect("queue_delete");
    connection.close(0, "".into()).await.expect("close");
}
