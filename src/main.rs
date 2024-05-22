use std::sync::{Arc, RwLock};

use tonic::{transport::Server, Request, Response, Status};

use ::espikey::MemTable;
use espikey::kv_service_server::{KvService, KvServiceServer};
use espikey::{GetRequest, GetResponse, SetRequest, SetResponse};

pub mod espikey {
    tonic::include_proto!("espikey");
}

#[derive(Debug)]
struct EspikeyServer {
    storage: Arc<RwLock<MemTable>>,
}

#[tonic::async_trait]
impl KvService for EspikeyServer {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let request = request.into_inner();

        let storage = self.storage.read().unwrap();
        let response = match storage.get(&request.key) {
            Some(v) => espikey::GetResponse {
                status: espikey::Status::Ok.into(),
                value: Some(v.to_vec()),
            },
            None => espikey::GetResponse {
                status: espikey::Status::NotFound.into(),
                value: None,
            },
        };
        Ok(Response::new(response))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let request = request.into_inner();
        {
            let mut storage = self.storage.write().unwrap();
            storage.set(&request.key, &request.value);
        }

        let response = espikey::SetResponse {
            status: espikey::Status::Ok.into(),
        };
        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = "[::1]:50051".parse()?;
    let espikey_svc = EspikeyServer {
        storage: Arc::new(RwLock::new(MemTable::new())),
    };

    Server::builder()
        .add_service(KvServiceServer::new(espikey_svc))
        .serve(addr)
        .await?;
    Ok(())
}
