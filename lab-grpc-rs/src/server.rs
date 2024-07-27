//! The gRPC server.
//!

use crate::{log, rpc::kv_store::*, SERVER_ADDR};
use anyhow::Result;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tonic::{transport::Server, Request, Response, Status};

pub struct KvStore {
    kv_store: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

#[tonic::async_trait]
impl kv_store_server::KvStore for KvStore {
    async fn example(
        &self,
        req: Request<ExampleRequest>,
    ) -> Result<Response<ExampleReply>, Status> {
        log::info!("Received example request.");
        Ok(Response::new(ExampleReply {
            output: req.into_inner().input + 1,
        }))
    }

    async fn echo(&self, req: Request<EchoRequest>) -> Result<Response<EchoReply>, Status> {
        log::info!("Received echo request.");
        Ok(Response::new(EchoReply {
            msg: req.into_inner().msg,
        }))
    }

    async fn put(&self, req: Request<PutRequest>) -> Result<Response<PutReply>, Status> {
        log::info!("Received put request.");
        let mut kv = self.kv_store.write().await;
        let pair = req.into_inner();
        kv.insert(pair.key, pair.value);
        Ok(Response::new(PutReply {}))
    }

    async fn get(&self, req: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        log::info!("Received get request.");
        let kv = self.kv_store.read().await;
        let key = req.into_inner().key;
        if let Some(value) = kv.get(&key) {
            Ok(Response::new(GetReply {
                value: value.clone(),
            }))
        } else {
            Err(Status::new(tonic::Code::NotFound, "Key does not exist."))
        }
    }
}

pub async fn start() -> Result<()> {
    let svc = kv_store_server::KvStoreServer::new(KvStore {
        kv_store: Arc::new(RwLock::new(HashMap::new())),
    });

    log::info!("Starting KV store server.");
    Server::builder()
        .add_service(svc)
        .serve(SERVER_ADDR.parse().unwrap())
        .await?;
    Ok(())
}
