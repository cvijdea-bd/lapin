#![cfg(feature = "tokio")]

//! Checks that `wait_for_confirms` completes when another publish lands on the
//! channel while it waits.
//!
//! Runs against a real broker. `wait_for_confirms` starts while confirms of an
//! earlier batch are still arriving, then one more message is published. A
//! round in which the whole batch was already confirmed is retried.

use futures_lite::future;
use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions},
    types::FieldTable,
};
use std::time::Duration;

const BATCH: usize = 2000;
const ROUNDS: usize = 5;
const TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn wait_for_confirms_completes_after_a_later_publish() {
    let addr = std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());
    let conn = Connection::connect(&addr, ConnectionProperties::default())
        .await
        .unwrap();
    let channel = conn.create_channel().await.unwrap();
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
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
    let publish = || {
        channel.basic_publish(
            "".into(),
            queue.name().clone(),
            BasicPublishOptions::default(),
            b"hello",
            BasicProperties::default(),
        )
    };

    for _ in 0..ROUNDS {
        let mut confirms = Vec::with_capacity(BATCH + 1);
        for _ in 0..BATCH {
            confirms.push(publish().await.unwrap());
        }
        let mut wait = Box::pin(channel.wait_for_confirms());
        if future::poll_once(&mut wait).await.is_some() {
            // Every confirm arrived first: nothing raced the wait this round.
            continue;
        }
        confirms.push(publish().await.unwrap());
        tokio::time::timeout(TIMEOUT, wait)
            .await
            .expect("wait_for_confirms never completed")
            .unwrap();
        for confirm in confirms {
            confirm.await.unwrap();
        }
    }

    conn.close(200, "bye".into()).await.unwrap();
}
