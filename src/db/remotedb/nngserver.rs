use kgdata::error::KGDataError;
use log::info;
use nng::{
    options::{Options, RecvTimeout},
    Error, Message, Protocol, Socket,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{thread::sleep, time::Duration};

use super::{request::serialize_optional_bytes, Client, Request, Response};

const CHECK_SIGNALS_INTERVAL: Duration = Duration::from_millis(100);
const DIAL_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const DIAL_MAX_RETRIES: usize = 200; // ten seconds

pub struct NNGClient {
    socket: Mutex<Socket>,
}

impl Client for NNGClient {
    type Message = nng::Message;

    fn open(url: &str) -> Result<Self, KGDataError> {
        Ok(Self {
            socket: Mutex::new(dial(url)?),
        })
    }

    // #[inline(always)]
    // fn send(&self, req: &[u8]) -> Result<(), KGDataError> {
    //     // self.socket.send(req).map_err(from_nngerror)
    //     self.socket.lock().unwrap().send(req).map_err(from_nngerror)
    // }

    // #[inline(always)]
    // fn recv(&self) -> Result<nng::Message, KGDataError> {
    //     Ok(self.socket.lock().unwrap().recv()?)
    // }
    #[inline(always)]
    fn request(&self, req: &[u8]) -> Result<nng::Message, KGDataError> {
        let socket = self.socket.lock().unwrap();
        socket.send(req).map_err(from_nngerror)?;
        Ok(socket.recv()?)
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

    let socket = Socket::new(Protocol::Rep0)?;
    socket.listen(url)?;
    socket.set_opt::<RecvTimeout>(Some(CHECK_SIGNALS_INTERVAL))?;

    info!("Serving a database at {}", url);

    loop {
        let mut nnmsg = loop {
            if let Ok(m) = socket.recv() {
                break m;
            }

            match socket.recv() {
                Ok(m) => break m,
                Err(Error::TimedOut) => {
                    if !running.load(Ordering::SeqCst) {
                        // users send a signal, perhaps to stop the process.
                        return Err(KGDataError::InterruptedError(
                            "Receiving an terminated signal",
                        )
                        .into());
                    }
                }
                Err(e) => return Err(KGDataError::NNGError(e)),
            }
        };
        let msg = Request::deserialize(nnmsg.as_slice())?;

        let rep = match &msg {
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

        nnmsg.clear();
        nnmsg.push_back(&rep);
        socket.send(nnmsg).map_err(from_nngerror)?;
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

#[inline]
pub fn from_nngerror(err: (Message, Error)) -> KGDataError {
    KGDataError::NNGError(err.1)
}

#[inline]
pub fn dial(url: &str) -> Result<Socket, KGDataError> {
    let socket = Socket::new(Protocol::Req0)?;

    for _ in 0..DIAL_MAX_RETRIES {
        match socket.dial(url) {
            Ok(_) => return Ok(socket),
            Err(Error::ConnectionRefused) => {
                sleep(DIAL_RETRY_INTERVAL);
            }
            Err(err) => return Err(KGDataError::NNGError(err).into()),
        }
    }

    return Err(KGDataError::NNGError(Error::ConnectionRefused).into());
}
