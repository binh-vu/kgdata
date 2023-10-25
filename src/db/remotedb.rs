use ahash::AHasher;
use kgdata::error::KGDataError;
use log::info;
use nng::{
    options::{Options, RecvTimeout},
    Error, Message, Protocol, Socket,
};
use std::borrow::Borrow;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{thread::sleep, time::Duration};

const CHECK_SIGNALS_INTERVAL: Duration = Duration::from_millis(100);
const DIAL_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const DIAL_MAX_RETRIES: usize = 200; // ten seconds

pub struct RemoteRocksDBDict<K: AsRef<[u8]> + 'static, V: 'static> {
    sockets: Vec<Socket>,
    deser_value: fn(&[u8]) -> Result<V, KGDataError>,
    deser_key: PhantomData<fn() -> K>,
}

impl<K: AsRef<[u8]>, V> RemoteRocksDBDict<K, V> {
    pub fn new<Q: Sized>(
        urls: &[Q],
        deser_value: fn(&[u8]) -> Result<V, KGDataError>,
    ) -> Result<Self, KGDataError>
    where
        Q: AsRef<str>,
    {
        let sockets = urls
            .into_iter()
            .map(|url| dial(url.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            sockets,
            deser_value,
            deser_key: PhantomData,
        })
    }

    pub fn get<Q: ?Sized>(&self, key: &Q) -> Result<Option<V>, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]>,
    {
        let k = key.as_ref();
        let socket = &self.sockets[find_worker(k, self.sockets.len())];
        socket
            .send(&ReqMsg::Get(k).serialize())
            .map_err(from_nngerror)?;
        let msg = socket.recv()?;
        match RepMsg::deserialize(&msg)? {
            RepMsg::Error => {
                Err(KGDataError::IPCImplError("Remote DB encounters an error".to_owned()).into())
            }
            RepMsg::SuccessGet(data) => {
                if data.len() == 0 {
                    // key does not exist in the primary
                    Ok(None)
                } else {
                    (self.deser_value)(data).map(|v| Some(v))
                }
            }
            _ => Err(KGDataError::IPCImplError(
                "Invalid message. Please report the bug.".to_owned(),
            )
            .into()),
        }
    }

    pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> Result<bool, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]>,
    {
        let k = key.as_ref();
        let socket = &self.sockets[find_worker(k, self.sockets.len())];
        socket
            .send(&ReqMsg::Contains(k).serialize())
            .map_err(from_nngerror)?;

        let msg = socket.recv()?;
        match RepMsg::deserialize(&msg)? {
            RepMsg::Error => {
                Err(KGDataError::IPCImplError("Remote DB encounters an error".to_owned()).into())
            }
            RepMsg::SuccessContains(b) => Ok(b),
            _ => Err(KGDataError::IPCImplError(
                "Invalid message. Please report the bug.".to_owned(),
            )
            .into()),
        }
    }
}

#[derive(Debug)]
pub enum ReqMsg<'s> {
    // stop
    Stop,

    // Get a key from the database
    Get(&'s [u8]),

    // Check if a key exists in the database
    Contains(&'s [u8]),
}

#[derive(Debug)]
pub enum RepMsg<'s> {
    Error,
    SuccessStop,
    // contains value, empty if there is no value
    SuccessGet(&'s [u8]),
    // whether the key exists
    SuccessContains(bool),
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
        let msg = ReqMsg::deserialize(nnmsg.as_slice())?;

        let rep = match &msg {
            ReqMsg::Stop => {
                nnmsg.clear();
                nnmsg.push_back(&RepMsg::SuccessStop.serialize());
                socket.send(nnmsg).map_err(from_nngerror)?;
                return Ok(());
            }
            ReqMsg::Get(key) => match db.get_pinned(key)? {
                None => RepMsg::SuccessGet(&[]).serialize(),
                Some(value) => RepMsg::SuccessGet(value.as_ref()).serialize(),
            },
            ReqMsg::Contains(key) => {
                let msg = match db.get_pinned(key)? {
                    None => RepMsg::SuccessContains(false),
                    Some(_) => RepMsg::SuccessContains(true),
                };
                msg.serialize()
            }
        };

        nnmsg.clear();
        nnmsg.push_back(&rep);
        socket.send(nnmsg).map_err(from_nngerror)?;
    }
}

impl<'s> RepMsg<'s> {
    pub fn deserialize(buf: &'s [u8]) -> Result<Self, KGDataError> {
        match buf[0] {
            0 => Ok(Self::Error),
            1 => Ok(Self::SuccessStop),
            2 => Ok(Self::SuccessGet(&buf[1..])),
            3 => Ok(Self::SuccessContains(buf[1] == 1)),
            _ => Err(KGDataError::IPCImplError(
                "Invalid message. Please report the bug.".to_owned(),
            )
            .into()),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Self::Error => vec![0],
            Self::SuccessStop => vec![1],
            Self::SuccessGet(value) => {
                let mut buf = Vec::with_capacity(value.len() + 1);
                buf.push(2);
                buf.extend_from_slice(value);
                buf
            }
            Self::SuccessContains(value) => vec![3, *value as u8],
        }
    }
}

impl<'s> ReqMsg<'s> {
    pub fn deserialize(buf: &'s [u8]) -> Result<Self, KGDataError> {
        match buf[0] {
            0 => Ok(Self::Stop),
            1 => Ok(Self::Get(&buf[1..])),
            2 => Ok(Self::Contains(&buf[1..])),
            _ => Err(KGDataError::IPCImplError(
                "Invalid message. Please report the bug.".to_owned(),
            )
            .into()),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Self::Stop => vec![0],
            Self::Get(key) => {
                let mut buf = Vec::with_capacity(key.len() + 1);
                buf.push(1);
                buf.extend_from_slice(key);
                return buf;
            }
            Self::Contains(key) => {
                let mut buf = Vec::with_capacity(key.len() + 1);
                buf.push(2);
                buf.extend_from_slice(key);
                return buf;
            }
        }
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

/// Find the worker that is responsible for the given key.
#[inline]
fn find_worker(k: &[u8], nworkers: usize) -> usize {
    let mut hasher = AHasher::default();
    hasher.write(k);
    (hasher.finish() % nworkers as u64) as usize
}
