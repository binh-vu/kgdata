use crate::db::remotedb::nngserver::process_request;

use super::{request::serialize_optional_bytes, Client as TraitSocket, Request, Response};
use interprocess::local_socket::{LocalSocketListener, LocalSocketStream};
use kgdata::error::KGDataError;
use log::info;
use nng::{
    options::{Options, RecvTimeout},
    Error, Message, Protocol, Socket,
};
use std::io::Read;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{thread::sleep, time::Duration};
const CHECK_SIGNALS_INTERVAL: Duration = Duration::from_millis(30);

pub struct InterProcessClient {
    socket: Mutex<LocalSocketStream>,
}

impl TraitSocket for InterProcessClient {
    type Message = Vec<u8>;

    fn open(url: &str) -> Result<Self, KGDataError> {
        Ok(Self {
            socket: Mutex::new(LocalSocketStream::connect(url)?),
        })
    }

    #[inline(always)]
    fn send(&self, req: &[u8]) -> Result<(), KGDataError> {
        Ok(self.socket.lock().unwrap().write_all(req)?)
    }

    #[inline(always)]
    fn recv(&self) -> Result<Self::Message, KGDataError> {
        let mut buf = Vec::new();
        let size = self.socket.lock().unwrap().read_to_end(&mut buf)?;
        buf.truncate(size);
        Ok(buf)
    }
}

/// Serve an instance of rocksdb at the given URL.
pub fn serve_db(url: &str, db: &rocksdb::DB) -> Result<(), KGDataError> {
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        info!("Received Ctrl-C, try to exit...");
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    let socket = LocalSocketListener::bind(url).unwrap();
    socket.set_nonblocking(true).unwrap();
    info!("Serving a database at {}", url);

    let mut buf = Vec::new();
    loop {
        let mut conn = loop {
            match socket.accept() {
                Ok(m) => break m,
                Err(e) => {
                    match e.kind() {
                        std::io::ErrorKind::WouldBlock => {
                            if !running.load(Ordering::SeqCst) {
                                // users send a signal, perhaps to stop the process.
                                return Err(KGDataError::InterruptedError(
                                    "Receiving an terminated signal",
                                )
                                .into());
                            }

                            sleep(CHECK_SIGNALS_INTERVAL);
                        }
                        _ => {
                            return Err(KGDataError::IOError(e));
                        }
                    }
                }
            }
        };

        let nbytes = conn.read_to_end(&mut buf)?;
        let req = Request::deserialize(&buf[..nbytes])?;
        conn.write_all(&process_request(db, &req)?)?;
    }
}
