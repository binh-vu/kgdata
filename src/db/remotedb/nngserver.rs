use super::{ipcserde, Client, Request, Response};
use kgdata::error::KGDataError;
use log::info;
use nng::{
    options::{Options, RecvTimeout},
    Error, Message, Protocol, Socket,
};
use std::sync::Mutex;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::{thread::sleep, time::Duration};
use thread_local::ThreadLocal;

const CHECK_SIGNALS_INTERVAL: Duration = Duration::from_millis(200);
const DIAL_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const DIAL_MAX_RETRIES: usize = 200; // ten seconds
#[allow(dead_code)]
const SHARED_MEM_SIZE: usize = 50 * 1024 * 1024; // 50 MB

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
    // shm: ReadonlySharedMemBuffer,
}

impl Client for NNGLocalClient {
    type Message = nng::Message;

    fn open(url: &str) -> Result<Self, KGDataError> {
        Ok(Self {
            url: url.to_owned(),
            socket: ThreadLocal::new(),
            // shm: ReadonlySharedMemBuffer(SharedMemBuffer::open(&SharedMemBuffer::get_flink(url))?),
        })
    }

    #[inline(always)]
    fn request(&self, req: &[u8]) -> Result<nng::Message, KGDataError> {
        let socket = self.socket.get_or_try(|| dial(&self.url))?;
        socket.send(req).map_err(from_nngerror)?;
        Ok(socket.recv()?)
    }

    // fn get_shm(&self) -> Option<&ReadonlySharedMemBuffer> {
    //     Some(&self.shm)
    // }
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
    let mut buffer = ipcserde::VecBuffer::with_capacity(10 * 1024 * 1024);
    // let mut shm = SharedMemBuffer::new(&SharedMemBuffer::get_flink(url), SHARED_MEM_SIZE)?;

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
                None => Response::ser_success_get(ipcserde::EmptySlice {}, &mut buffer),
                Some(value) => Response::ser_success_get(value, &mut buffer),
            },
            // Request::BatchGet(keys) => {
            //     let values = keys
            //         .into_iter()
            //         .map(|key| {
            //             Ok::<_, KGDataError>(ipcserde::OptionDBPinnableSlice(db.get_pinned(key)?))
            //         })
            //         .collect::<Result<Vec<_>, _>>()?;
            //     Response::ser_compressed_shm_success_batch_get(&values, &mut buffer, &mut shm)?
            // }
            Request::BatchGet(keys) => Response::ser_compressed_success_batch_get(
                keys.into_iter().map(|key| {
                    ipcserde::OptionDBPinnableSlice(db.get_pinned(key).expect("Error getting key"))
                }),
                &mut buffer,
            ),
            Request::Contains(key) => {
                let contain = db.get_pinned(key)?.is_some();
                Response::ser_success_contains(contain, &mut buffer)
            }
            Request::Test(value) => {
                // we read a file of entities and calculate sum of id number
                let (nlines, filename) = {
                    let tmp = value.split(":").collect::<Vec<_>>();
                    (tmp[0], tmp[1])
                };
                let mut entids = std::fs::read_to_string(&filename)
                    .unwrap()
                    .lines()
                    .filter(|x| x.len() > 0)
                    .map(String::from)
                    .collect::<Vec<_>>();
                entids.truncate(nlines.parse::<usize>().unwrap());

                let sum = entids
                    .into_iter()
                    .map(|entid| {
                        let key: &[u8] = entid.as_ref();
                        let ent =
                            crate::db::deser_entity(&db.get_pinned(key).unwrap().unwrap()).unwrap();
                        ent.id[1..].parse::<i32>().unwrap() % 2
                    })
                    .sum::<i32>() as u32;

                Response::ser_success_test(sum, &mut buffer)
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
