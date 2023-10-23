use rocksdb::{DBCompressionType, Options};

/// Start a primary instance of rocksdb that is responsible
/// for writing records to the database.
pub fn primary_db(url: &str, dbpath: &str, options: &Options) -> Result<()> {
    let db = rocksdb::DB::open_for_read_only(&options, dbpath, false)?;

    let socket = Socket::new(Protocol::Rep0)?;
    socket.listen(url)?;
    socket.set_opt::<RecvTimeout>(Some(CHECK_SIGNALS_INTERVAL))?;

    loop {
        let mut nnmsg = loop {
            if let Ok(m) = socket.recv() {
                break m;
            }

            match socket.recv() {
                Ok(m) => break m,
                Err(Error::TimedOut) => {
                    let signal = unsafe { PyErr_CheckSignals() };
                    if signal != 0 {
                        // users send a signal, perhaps to stop the process.
                        return Err(HugeDictError::PyErr(PyInterruptedError::new_err(
                            "Receiving an incoming signal",
                        ))
                        .into());
                    }
                }
                Err(e) => return Err(HugeDictError::NNGError(e).into()),
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
            ReqMsg::Put(key, value) => {
                db.put(key, value)?;
                RepMsg::SuccessPut.serialize()
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
