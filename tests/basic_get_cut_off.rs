#![cfg(feature = "tokio")]

//! Checks that a `basic_get` whose message is cut off mid-transfer fails
//! instead of staying pending.
//!
//! Runs against a real broker through a relay. The message is large enough to
//! arrive in many body frames, and the relay cuts the connection once part of
//! the `basic_get` reply has passed.

mod common;

use common::Relay;
use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions},
    types::FieldTable,
};
use std::time::Duration;

const PAYLOAD: usize = 4 << 20;
/// Bytes of the reply let through before the cut: well past the get-ok, well
/// short of the whole message.
const CUT_AFTER: usize = 512 << 10;
const TIMEOUT: Duration = Duration::from_secs(10);

async fn get_cut_off(properties: ConnectionProperties) {
    let relay = Relay::start();
    let conn = Connection::connect_uri(relay.uri(), properties)
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
    channel
        .basic_publish(
            "".into(),
            queue.name().clone(),
            BasicPublishOptions::default(),
            &vec![0; PAYLOAD],
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();

    relay.cut_after(CUT_AFTER);
    let get = channel.basic_get(queue.name().clone(), BasicGetOptions::default());
    let result = tokio::time::timeout(TIMEOUT, get)
        .await
        .expect("basic_get never completed");
    assert!(result.is_err(), "{result:?}");
}

#[tokio::test]
async fn basic_get_cut_off_mid_message_fails() {
    get_cut_off(ConnectionProperties::default()).await;
}

#[tokio::test]
async fn basic_get_cut_off_mid_message_fails_with_auto_recovery() {
    get_cut_off(
        ConnectionProperties::default()
            .enable_auto_recover()
            .configure_backoff(|backoff| {
                backoff
                    .with_min_delay(Duration::from_millis(10))
                    .with_max_delay(Duration::from_millis(100))
            }),
    )
    .await;
}
