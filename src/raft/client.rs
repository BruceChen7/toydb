use super::{Request, Response, Status};
use crate::error::{Error, Result};

use tokio::sync::{mpsc, oneshot};

/// A client for a local Raft server.
#[derive(Clone)]
pub struct Client {
    // multi producer and single sender
    request_tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Result<Response>>)>,
}

impl Client {
    /// Creates a new Raft client.
    /// 创建raft client
    pub fn new(
        request_tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Result<Response>>)>,
    ) -> Self {
        Self { request_tx }
    }

    /// Executes a request against the Raft cluster.
    async fn request(&self, request: Request) -> Result<Response> {
        let (response_tx, response_rx) = oneshot::channel();
        self.request_tx.send((request, response_tx))?;
        // 响应返回
        response_rx.await?
    }

    /// Mutates the Raft state machine.
    pub async fn mutate(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        match self.request(Request::Mutate(command)).await? {
            // 返回结果
            Response::State(response) => Ok(response),
            // 如果是error
            resp => Err(Error::Internal(format!("Unexpected Raft mutate response {:?}", resp))),
        }
    }

    /// Queries the Raft state machine.
    pub async fn query(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        match self.request(Request::Query(command)).await? {
            Response::State(response) => Ok(response),
            resp => Err(Error::Internal(format!("Unexpected Raft query response {:?}", resp))),
        }
    }

    /// Fetches Raft node status.
    pub async fn status(&self) -> Result<Status> {
        match self.request(Request::Status).await? {
            Response::Status(status) => Ok(status),
            resp => Err(Error::Internal(format!("Unexpected Raft status response {:?}", resp))),
        }
    }
}
