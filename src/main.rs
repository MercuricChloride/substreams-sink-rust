use anyhow::{format_err, Context, Error};
use futures03::StreamExt;
use pb::sf::substreams::rpc::v2::{BlockScopedData, BlockUndoSignal};
use pb::sf::substreams::v1::Package;

use prost::Message;
use std::{env, process::exit, sync::Arc};
use substreams::SubstreamsEndpoint;
use substreams_stream::{BlockResponse, SubstreamsStream};

use dotenv::dotenv;

use substreams_sink_rust_lib::{start_stream, StreamConfig};

mod pb;
mod substreams;
mod substreams_stream;

const PACKAGE_FILE: &str = "https://github.com/streamingfast/substreams-uniswap-v3/releases/download/v0.2.8/substreams.spkg";

#[tokio::main]
async fn main() -> Result<(), Error> {
    dotenv().ok();

    let api_key = env::var("API_KEY").expect("API_KEY must be set in the .env file!");

    let config = StreamConfig {
        endpoint_url: "https://mainnet.eth.streamingfast.io:443".to_string(),
        package_file: PACKAGE_FILE.to_string(),
        module_name: "graph_out".to_string(),
        token: Some(api_key),
        start: 12369621,
        stop: 12369721,
    };

    start_stream(config).await
}
