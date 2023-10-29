use super::{ipcdeser, Client, Request, Response};
use kgdata::error::KGDataError;
use log::info;
use nng::{
    options::{Options, RecvTimeout},
    Error, Message, Protocol, Socket,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{thread::sleep, time::Duration};
use thread_local::ThreadLocal;

const CHECK_SIGNALS_INTERVAL: Duration = Duration::from_millis(200);
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

    #[inline(always)]
    fn request(&self, req: &[u8]) -> Result<nng::Message, KGDataError> {
        let socket = self.socket.lock().unwrap();
        socket.send(req).map_err(from_nngerror)?;
        Ok(socket.recv()?)
    }
}

pub struct NNGLocalClient {
    url: String,
    socket: ThreadLocal<Socket>,
}

impl Client for NNGLocalClient {
    type Message = nng::Message;

    fn open(url: &str) -> Result<Self, KGDataError> {
        Ok(Self {
            url: url.to_owned(),
            socket: ThreadLocal::new(),
        })
    }

    #[inline(always)]
    fn request(&self, req: &[u8]) -> Result<nng::Message, KGDataError> {
        let socket = self.socket.get_or_try(|| dial(&self.url))?;
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

    // 10 MB
    let mut buffer = ipcdeser::VecBuffer::with_capacity(10 * 1024 * 1024);
    info!("Serving a database at {}", url);
    loop {
        let mut nnmsg = loop {
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
        let req = Request::deserialize(nnmsg.as_slice())?;
        buffer.clear();
        let resp_size = match req {
            Request::Get(key) => match db.get_pinned(key)? {
                None => Response::SuccessGet(&[]).serialize_to_buf(&mut buffer),
                Some(value) => Response::SuccessGet(value.as_ref()).serialize_to_buf(&mut buffer),
            },
            Request::BatchGet(keys) => {
                let values = keys
                    .iter()
                    .map(|key| db.get_pinned(key))
                    .collect::<Result<Vec<_>, _>>()?;
                ipcdeser::serialize_optional_lst_to_buffer(
                    Response::SUCCESS_BATCH_GET,
                    &values,
                    &mut buffer,
                )
            }
            Request::Contains(key) => {
                let msg = match db.get_pinned(key)? {
                    None => Response::SuccessContains(false),
                    Some(_) => Response::SuccessContains(true),
                };
                msg.serialize_to_buf(&mut buffer)
            }
        };
        nnmsg.clear();
        nnmsg.push_back(&buffer.0[..resp_size]);
        socket.send(nnmsg).map_err(from_nngerror)?;
    }
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
