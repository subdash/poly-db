use kvs_engine::{Engine, EngineError, Reader};
use tokio::{
    sync::{mpsc, oneshot},
    task::spawn_blocking,
};

pub enum Request {
    Set {
        key: String,
        value: String,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Remove {
        key: String,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
}

pub fn channel(capacity: usize) -> (mpsc::Sender<Request>, mpsc::Receiver<Request>) {
    let (sender, receiver) = mpsc::channel(capacity);
    (sender, receiver)
}

pub fn spawn_with(
    mut engine: Engine,
    mut rx: mpsc::Receiver<Request>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        while let Some(request) = rx.blocking_recv() {
            match request {
                Request::Set { key, value, reply } => {
                    let engine_response = engine.set(key, value);
                    let _ = reply.send(engine_response);
                }
                Request::Remove { key, reply } => {
                    let engine_response = engine.remove(&key);
                    let _ = reply.send(engine_response);
                }
            }
        }

        if let Err(e) = engine.sync() {
            eprintln!("writer thread: final sync failed: {e}")
        }
    })
}

pub fn spawn(engine: Engine, capacity: usize) -> (KvHandle, std::thread::JoinHandle<()>) {
    let reader = engine.reader();
    let (sender, receiver) = channel(capacity);
    let join_handle = spawn_with(engine, receiver);
    let kv_handle = KvHandle::new(sender, reader);

    (kv_handle, join_handle)
}

pub struct KvHandle {
    sender: mpsc::Sender<Request>,
    reader: Reader,
}

impl KvHandle {
    pub fn new(tx: mpsc::Sender<Request>, reader: Reader) -> Self {
        KvHandle { sender: tx, reader }
    }

    pub async fn get(&self, key: String) -> Result<String, EngineError> {
        let reader = self.reader.clone();

        spawn_blocking(move || reader.get(&key))
            .await
            .map_err(|_| EngineError::ShuttingDown)?
    }

    pub async fn set(&self, key: String, value: String) -> Result<(), EngineError> {
        self.dispatch(|reply_tx| Request::Set {
            key,
            value,
            reply: reply_tx,
        })
        .await
    }

    pub async fn remove(&self, key: String) -> Result<(), EngineError> {
        self.dispatch(|reply_tx| Request::Remove {
            key,
            reply: reply_tx,
        })
        .await
    }

    async fn dispatch(
        &self,
        make_request: impl FnOnce(oneshot::Sender<Result<(), EngineError>>) -> Request,
    ) -> Result<(), EngineError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = make_request(reply_tx);

        // Send might fail because the receiver is gone
        self.sender
            .send(request)
            .await
            .map_err(|_| EngineError::ShuttingDown)?;

        // Receive might fail because the writer died mid-request
        match reply_rx.await {
            Ok(engine_result) => engine_result,
            Err(_) => Err(EngineError::ShuttingDown),
        }
    }
}
