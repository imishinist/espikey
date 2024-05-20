use tonic::{transport::Server, Request, Response, Status};

use espikey::kv_service_server::{KvService, KvServiceServer};
use espikey::{GetRequest, GetResponse, SetRequest, SetResponse};

pub mod espikey {
    tonic::include_proto!("espikey");
}

#[derive(Debug, Default)]
struct EspikeyServer {}

#[tonic::async_trait]
impl KvService for EspikeyServer {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        println!("Got a request: {:?}", request);
        let response = espikey::GetResponse {
            value: "Hello, world!".into(),
        };
        Ok(Response::new(response))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        println!("Got a request: {:?}", request);
        let response = espikey::SetResponse { status: 1 };
        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = "[::1]:50051".parse()?;
    let espikey_svc = EspikeyServer::default();

    Server::builder()
        .add_service(KvServiceServer::new(espikey_svc))
        .serve(addr)
        .await?;
    Ok(())
}
