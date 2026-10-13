//! A TCP relay in front of the broker, shared by the tests that need to break
//! a connection in ways a broker does not on its own.

// Each test uses only part of the relay.
#![allow(dead_code)]

use lapin::uri::AMQPUri;
use std::{
    io::{ErrorKind, Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

/// A TCP relay in front of the broker named by `AMQP_ADDR`.
///
/// The client connects through the relay, so its AMQP peer is the broker
/// itself. The relay copies bytes in both directions until a test cuts, fails
/// or silences the connection.
pub struct Relay {
    uri: AMQPUri,
    state: Arc<State>,
}

#[derive(Default)]
struct State {
    /// Both sockets of every session relayed so far.
    sockets: Mutex<Vec<TcpStream>>,
    accept: Mutex<Accept>,
    /// Whether the relay swallows all traffic.
    silent: AtomicBool,
    /// Bytes from the broker still let through before the session is cut.
    budget: Mutex<Option<usize>>,
}

/// How the relay treats new connections.
#[derive(Clone, Copy, Default, PartialEq)]
enum Accept {
    #[default]
    Relay,
    /// Closes each new connection as soon as it is accepted.
    Reset,
    /// Stops listening, so new connections are refused.
    Refuse,
}

impl Relay {
    /// Starts a relay in front of the broker named by `AMQP_ADDR`.
    pub fn start() -> Self {
        let mut uri: AMQPUri = std::env::var("AMQP_ADDR")
            .unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into())
            .parse()
            .expect("invalid AMQP_ADDR");
        let upstream = format!("{}:{}", uri.authority.host, uri.authority.port);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        // The listener is polled, so that refusing connections can drop it.
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        uri.authority.host = addr.ip().to_string();
        uri.authority.port = addr.port();
        let state = Arc::new(State::default());
        let relay = Self {
            uri,
            state: state.clone(),
        };
        thread::spawn(move || listen(listener, upstream, state));
        relay
    }

    /// The broker's URI, pointed at the relay.
    pub fn uri(&self) -> AMQPUri {
        self.uri.clone()
    }

    /// Cuts every session relayed so far, in both directions.
    pub fn cut(&self) {
        for socket in self.state.sockets.lock().unwrap().drain(..) {
            let _ = socket.shutdown(Shutdown::Both);
        }
    }

    /// Closes every new connection as soon as it is accepted.
    pub fn reset_new_connections(&self) {
        *self.state.accept.lock().unwrap() = Accept::Reset;
    }

    /// Stops listening, so every new connection is refused.
    pub fn refuse_new_connections(&self) {
        *self.state.accept.lock().unwrap() = Accept::Refuse;
    }

    /// Swallows all traffic in both directions from now on, and never reports
    /// a close to either side, like a network that stopped delivering.
    pub fn go_silent(&self) {
        self.state.silent.store(true, Ordering::SeqCst);
    }

    /// Cuts the session once more than `bytes` further bytes have come from
    /// the broker. Only the bytes within that budget reach the client.
    pub fn cut_after(&self, bytes: usize) {
        *self.state.budget.lock().unwrap() = Some(bytes);
    }
}

impl State {
    fn accept(&self) -> Accept {
        *self.accept.lock().unwrap()
    }

    /// Counts `n` bytes from the broker against the budget. Returns false once
    /// they exceed it, and lifts the budget so that later sessions are not cut.
    fn spend(&self, n: usize) -> bool {
        let mut budget = self.budget.lock().unwrap();
        match *budget {
            None => true,
            Some(left) if n <= left => {
                *budget = Some(left - n);
                true
            }
            Some(_) => {
                *budget = None;
                false
            }
        }
    }
}

/// Relays each accepted connection to `upstream`. Returning drops the
/// listener, so every further connection is refused.
fn listen(listener: TcpListener, upstream: String, state: Arc<State>) {
    while state.accept() != Accept::Refuse {
        match listener.accept() {
            Ok((client, _)) => {
                // On Windows, accepted sockets inherit the listener's non-blocking mode.
                client.set_nonblocking(false).unwrap();
                if state.accept() != Accept::Relay {
                    let _ = client.shutdown(Shutdown::Both);
                    continue;
                }
                let server = TcpStream::connect(&upstream).expect("broker unreachable");
                state
                    .sockets
                    .lock()
                    .unwrap()
                    .extend([client.try_clone().unwrap(), server.try_clone().unwrap()]);
                pump(
                    client.try_clone().unwrap(),
                    server.try_clone().unwrap(),
                    state.clone(),
                    false,
                );
                pump(server, client, state.clone(), true);
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("accept failed: {error}"),
        }
    }
}

/// Copies `from` into `to` until either side closes or the session is cut.
fn pump(mut from: TcpStream, mut to: TcpStream, state: Arc<State>, from_broker: bool) {
    thread::spawn(move || {
        let mut buf = [0; 16384];
        while let Ok(n) = from.read(&mut buf) {
            if n == 0 || (from_broker && !state.spend(n)) {
                break;
            }
            if !state.silent.load(Ordering::SeqCst) && to.write_all(&buf[..n]).is_err() {
                break;
            }
        }
        // A silent network never reports a close. The relay keeps a handle on
        // both sockets, so they stay open.
        if !state.silent.load(Ordering::SeqCst) {
            let _ = from.shutdown(Shutdown::Both);
            let _ = to.shutdown(Shutdown::Both);
        }
    });
}
