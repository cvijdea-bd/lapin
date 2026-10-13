#![cfg(feature = "tokio")]

//! Checks what callers see once auto-recovery gives up: a waiter on the
//! recovery gets an error, the consumer ends, and the connection and channel
//! settle in their error states.
//!
//! Runs against a real broker through a relay. Once cut, the relay either
//! refuses every reconnect, so the TCP connect attempts run out, or closes each
//! one at once, so the reconnect attempts run out.

mod common;

use common::Relay;
use futures_lite::stream::StreamExt;
use lapin::{
    Connection, ConnectionProperties,
    options::{BasicConsumeOptions, BasicQosOptions, QueueDeclareOptions},
    types::FieldTable,
};
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(30);

async fn recovery_gives_up(fail_reconnects: fn(&Relay)) {
    let relay = Relay::start();
    // Slow enough to see the channel recovering, short enough to give up quickly.
    let properties = ConnectionProperties::default()
        .enable_auto_recover()
        .configure_backoff(|backoff| {
            backoff
                .with_min_delay(Duration::from_millis(200))
                .with_max_delay(Duration::from_millis(200))
                .with_max_times(1)
        });
    let conn = Connection::connect_uri(relay.uri(), properties)
        .await
        .unwrap();
    let channel = conn.create_channel().await.unwrap();
    let queue = channel
        .queue_declare(
            "".into(),
            QueueDeclareOptions {
                exclusive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut consumer = channel
        .basic_consume(
            queue.name().clone(),
            "abandoned_recovery".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    fail_reconnects(&relay);
    relay.cut();
    let deadline = Instant::now() + TIMEOUT;
    while !channel.status().reconnecting() {
        assert!(
            Instant::now() < deadline,
            "the channel never started recovering"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    // A call on a recovering channel fails with an error that waits for the recovery.
    let error = channel
        .basic_qos(1, BasicQosOptions::default())
        .await
        .unwrap_err();

    let waited = tokio::time::timeout(TIMEOUT, channel.wait_for_recovery(error))
        .await
        .expect("wait_for_recovery never completed");
    assert!(waited.is_err(), "{waited:?}");
    tokio::time::timeout(TIMEOUT, async { while consumer.next().await.is_some() {} })
        .await
        .expect("the consumer never ended");
    assert!(conn.status().errored());
    assert!(!channel.status().connected() && !channel.status().reconnecting());
}

#[tokio::test]
async fn recovery_that_cannot_connect_gives_up() {
    recovery_gives_up(Relay::refuse_new_connections).await;
}

#[tokio::test]
async fn recovery_whose_connections_drop_gives_up() {
    recovery_gives_up(Relay::reset_new_connections).await;
}
