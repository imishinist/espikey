use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use clap::Parser;
use tonic::{transport::Server, Request, Response, Status};

use ::espikey::DB;
use espikey::kv_service_server::{KvService, KvServiceServer};
use espikey::{GetRequest, GetResponse, SetRequest, SetResponse};

pub mod espikey {
    tonic::include_proto!("espikey");
}

#[derive(Debug)]
struct EspikeyServer {
    storage: Arc<RwLock<DB>>,
}

#[tonic::async_trait]
impl KvService for EspikeyServer {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let request = request.into_inner();

        let storage = self.storage.read().unwrap();
        let response = match storage.get(&request.key) {
            Ok(v) => espikey::GetResponse {
                status: espikey::Status::Ok.into(),
                value: Some(v),
            },
            Err(_status) => espikey::GetResponse {
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
            let _ = storage.put(&request.key, &request.value);
        }

        let response = espikey::SetResponse {
            status: espikey::Status::Ok.into(),
        };
        Ok(Response::new(response))
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about=None)]
#[clap(propagate_version = true)]
struct EspikeyCli {
    #[clap(short, long, default_value_t = 50051)]
    port: u16,

    #[clap(short, long, default_value = "espikey.log")]
    file: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = EspikeyCli::parse();
    println!("Starting Espikey server on port {}", args.port);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.file)?;

    let addr = format!("[::1]:{}", args.port).parse()?;
    let espikey_svc = EspikeyServer {
        storage: Arc::new(RwLock::new(DB::open())),
    };

    Server::builder()
        .add_service(KvServiceServer::new(espikey_svc))
        .serve(addr)
        .await?;
    Ok(())
}
