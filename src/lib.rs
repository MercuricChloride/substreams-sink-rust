use anyhow::{format_err, Context, Error};
use futures03::StreamExt;
use pb::sf::substreams::rpc::v2::{BlockScopedData, BlockUndoSignal};
use pb::sf::substreams::v1::Package;

use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use prost_types::{Any, FileDescriptorProto, FileDescriptorSet};
use std::sync::mpsc;
use std::{env, process::exit, sync::Arc};
use substreams::SubstreamsEndpoint;
use substreams_stream::{BlockResponse, SubstreamsStream};

pub use pb::*;

use prost_wkt_types::*;

pub mod pb;
mod substreams;
mod substreams_stream;

pub struct StreamConfig {
    pub endpoint_url: String,
    pub package_file: String,
    pub module_name: String,
    pub token: Option<String>,
    pub start: i64,
    pub stop: u64,
}

pub async fn start_stream_channel(
    config: StreamConfig,
) -> Result<mpsc::Receiver<String>, anyhow::Error> {
    let (tx, rx) = mpsc::channel();

    let StreamConfig {
        endpoint_url,
        package_file,
        module_name,
        token,
        stop,
        start,
        ..
    } = config;

    let package = read_package(&package_file).await?;
    let endpoint = Arc::new(SubstreamsEndpoint::new(&endpoint_url, token).await?);
    let file_descriptor_set = FileDescriptorSet {
        file: package.proto_files.clone(),
    };

    let descriptor_pool = DescriptorPool::from_file_descriptor_set(file_descriptor_set)
        .expect("Failed to convert file descriptor set to descriptor pool");

    let cursor: Option<String> = load_persisted_cursor()?;

    let mut stream = SubstreamsStream::new(
        endpoint.clone(),
        cursor,
        package.modules.clone(),
        module_name.to_string(),
        start,
        stop,
    );

    tokio::task::spawn(async move {
        loop {
            match stream.next().await {
                None => {
                    println!("Stream consumed");
                    break;
                }
                Some(Ok(BlockResponse::New(data))) => {
                    let result = process_block_scoped_data(&data, &descriptor_pool).unwrap();
                    persist_cursor(data.cursor).unwrap();
                    tx.send(result).unwrap();
                }
                Some(Ok(BlockResponse::Undo(undo_signal))) => {
                    //process_block_undo_signal(&undo_signal).unwrap();
                    //persist_cursor(undo_signal.last_valid_cursor).unwrap();
                    //tx.send(undo_signal.last_valid_cursor).unwrap();
                }
                Some(Err(err)) => {
                    println!();
                    println!("Stream terminated with error");
                    println!("{:?}", err);
                    exit(1);
                }
            }
        }
    });

    Ok(rx)
}

pub async fn start_stream(config: StreamConfig) -> Result<(), Error> {
    let StreamConfig {
        endpoint_url,
        package_file,
        module_name,
        token,
        stop,
        start,
        ..
    } = config;

    let package = read_package(&package_file).await?;
    let block_range = read_block_range(&package, &module_name)?;
    let endpoint = Arc::new(SubstreamsEndpoint::new(&endpoint_url, token).await?);
    let file_descriptor_set = FileDescriptorSet {
        file: package.proto_files.clone(),
    };

    let descriptor_pool = DescriptorPool::from_file_descriptor_set(file_descriptor_set)
        .expect("Failed to convert file descriptor set to descriptor pool");

    let cursor: Option<String> = load_persisted_cursor()?;

    let mut stream = SubstreamsStream::new(
        endpoint.clone(),
        cursor,
        package.modules.clone(),
        module_name.to_string(),
        start,
        stop,
    );

    loop {
        match stream.next().await {
            None => {
                println!("Stream consumed");
                break;
            }
            Some(Ok(BlockResponse::New(data))) => {
                process_block_scoped_data(&data, &descriptor_pool)?;
                persist_cursor(data.cursor)?;
            }
            Some(Ok(BlockResponse::Undo(undo_signal))) => {
                process_block_undo_signal(&undo_signal)?;
                persist_cursor(undo_signal.last_valid_cursor)?;
            }
            Some(Err(err)) => {
                println!();
                println!("Stream terminated with error");
                println!("{:?}", err);
                exit(1);
            }
        }
    }

    Ok(())
}

fn process_block_scoped_data(
    data: &BlockScopedData,
    pool: &DescriptorPool,
) -> Result<String, Error> {
    let output = data.output.as_ref().unwrap().map_output.as_ref().unwrap();

    let message_type = &output.type_url.trim_start_matches("type.googleapis.com/");
    let descriptor = pool.get_message_by_name(message_type).expect(&format!(
        "Failed to get message descriptor for type: {} from pool",
        message_type
    ));
    let dynamic_value = DynamicMessage::decode(descriptor, output.value.as_slice()).unwrap();

    let as_json = serde_json::to_string_pretty(&dynamic_value).unwrap();
    Ok(as_json)
}

fn process_block_undo_signal(_undo_signal: &BlockUndoSignal) -> Result<(), anyhow::Error> {
    // `BlockUndoSignal` must be treated as "delete every data that has been recorded after
    // block height specified by block in BlockUndoSignal". In the example above, this means
    // you must delete changes done by `Block #7b` and `Block #6b`. The exact details depends
    // on your own logic. If for example all your added record contain a block number, a
    // simple way is to do `delete all records where block_num > 5` which is the block num
    // received in the `BlockUndoSignal` (this is true for append only records, so when only `INSERT` are allowed).
    unimplemented!("you must implement some kind of block undo handling, or request only final blocks (tweak substreams_stream.rs)")
}

fn persist_cursor(_cursor: String) -> Result<(), anyhow::Error> {
    // FIXME: Handling of the cursor is missing here. It should be saved each time
    // a full block has been correctly processed/persisted. The saving location
    // is your responsibility.
    //
    // By making it persistent, we ensure that if we crash, on startup we are
    // going to read it back from database and start back our SubstreamsStream
    // with it ensuring we are continuously streaming without ever losing a single
    // element.
    Ok(())
}

fn load_persisted_cursor() -> Result<Option<String>, anyhow::Error> {
    // FIXME: Handling of the cursor is missing here. It should be loaded from
    // somewhere (local file, database, cloud storage) and then `SubstreamStream` will
    // be able correctly resume from the right block.
    Ok(None)
}

fn read_block_range(pkg: &Package, module_name: &str) -> Result<(i64, u64), anyhow::Error> {
    let module = pkg
        .modules
        .as_ref()
        .unwrap()
        .modules
        .iter()
        .find(|m| m.name == module_name)
        .ok_or_else(|| format_err!("module '{}' not found in package", module_name))?;

    let mut input: String = "".to_string();
    if let Some(range) = env::args().nth(4) {
        input = range;
    };

    let (prefix, suffix) = match input.split_once(":") {
        Some((prefix, suffix)) => (prefix.to_string(), suffix.to_string()),
        None => ("".to_string(), input),
    };

    let start: i64 = match prefix.as_str() {
        "" => module.initial_block as i64,
        x if x.starts_with("+") => {
            let block_count = x
                .trim_start_matches("+")
                .parse::<u64>()
                .context("argument <stop> is not a valid integer")?;

            (module.initial_block + block_count) as i64
        }
        x => x
            .parse::<i64>()
            .context("argument <start> is not a valid integer")?,
    };

    let stop: u64 = match suffix.as_str() {
        "" => 0,
        "-" => 0,
        x if x.starts_with("+") => {
            let block_count = x
                .trim_start_matches("+")
                .parse::<u64>()
                .context("argument <stop> is not a valid integer")?;

            start as u64 + block_count
        }
        x => x
            .parse::<u64>()
            .context("argument <stop> is not a valid integer")?,
    };

    Ok((start, stop))
}

async fn read_package(input: &str) -> Result<Package, anyhow::Error> {
    if input.starts_with("http") {
        return read_http_package(input).await;
    }

    // Assume it's a local file

    let content =
        std::fs::read(input).context(format_err!("read package from file '{}'", input))?;
    Package::decode(content.as_ref()).context("decode command")
}

async fn read_http_package(input: &str) -> Result<Package, anyhow::Error> {
    let body = reqwest::get(input).await?.bytes().await?;

    Package::decode(body).context("decode command")
}
