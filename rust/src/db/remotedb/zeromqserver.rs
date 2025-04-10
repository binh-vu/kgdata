use kgdata::error::KGDataError;
use log::info;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::{request::serialize_optional_bytes, Client, Request, Response};

pub struct ZMQClient {
    socket: Mutex<zmq::Socket>,
}

impl Client for ZMQClient {
    type Message = Vec<u8>;
    fn open(url: &str) -> Result<Self, KGDataError> {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::REQ)?;
        socket.connect(url)?;
        Ok(Self {
            socket: Mutex::new(socket),
        })
    }

    // fn send(&self, req: &[u8]) -> Result<(), KGDataError> {
    //     Ok(self.socket.lock().unwrap().send(req, 0)?)
    // }

    // fn recv(&self) -> Result<Self::Message, KGDataError> {
    //     self.socket
    //         .lock()
    //         .unwrap()
    //         .recv_bytes(0)
    //         .map_err(|err| KGDataError::ZeroMQError(err))
    // }

    fn request(&self, req: &[u8]) -> Result<Self::Message, KGDataError> {
        let socket = self.socket.lock().unwrap();
        socket.send(req, 0)?;
        socket
            .recv_bytes(0)
            .map_err(|err| KGDataError::ZeroMQError(err))
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

    let ctx = zmq::Context::new();
    let socket = ctx.socket(zmq::REP)?;
    socket.bind(url)?;

    let mut msg = zmq::Message::new();

    info!("Serving a database at {}", url);
    loop {
        socket.recv(&mut msg, 0)?;
        let request = Request::deserialize(&msg)?;
        let response = process_request(db, &request)?;
        socket.send(response, 0)?;
    }
}

pub fn process_request(db: &rocksdb::DB, req: &Request) -> Result<Vec<u8>, KGDataError> {
    let resp = match req {
        Request::Get(key) => match db.get_pinned(key)? {
            None => Response::SuccessGet(&[]).serialize(),
            Some(value) => Response::SuccessGet(value.as_ref()).serialize(),
        },
        Request::BatchGet(keys) => {
            let values = keys
                .iter()
                .map(|key| db.get_pinned(key))
                .collect::<Result<Vec<_>, _>>()?;
            serialize_optional_bytes(Response::SUCCESS_BATCH_GET, &values)
        }
        Request::Contains(key) => {
            let msg = match db.get_pinned(key)? {
                None => Response::SuccessContains(false),
                Some(_) => Response::SuccessContains(true),
            };
            msg.serialize()
        }
    };

    Ok(resp)
}
