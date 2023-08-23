use anyhow::{format_err, Context, Error};
use futures03::{future::join_all, StreamExt};
use pb::sf::substreams::rpc::v2::{BlockScopedData, BlockUndoSignal};
use pb::sf::substreams::v1::Package;

use persist::Persist;
use prost::Message;
use std::io::Write;
use std::{env, process::exit, sync::Arc};
use substreams::SubstreamsEndpoint;
use substreams_stream::{BlockResponse, SubstreamsStream};

use crate::pb::schema::EntriesAdded;
use crate::triples::Action;

pub mod constants;
mod pb;
mod persist;
mod sink_actions;
mod substreams;
mod substreams_stream;
mod triples;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = env::args();
    if args.len() != 4 {
        println!("usage: stream <endpoint> <spkg> <module>");
        println!();
        println!("The environment variable SUBSTREAMS_API_TOKEN must be set also");
        println!("and should contain a valid Substream API token.");
        exit(1);
    }

    let endpoint_url = env::args().nth(1).unwrap();
    let package_file = env::args().nth(2).unwrap();
    let module_name = env::args().nth(3).unwrap();

    let token_env = env::var("SUBSTREAMS_API_TOKEN").unwrap_or("".to_string());
    let mut token: Option<String> = None;

    if token_env.len() > 0 {
        token = Some(token_env);
    }

    let package = read_package(&package_file).await?;
    let endpoint = Arc::new(SubstreamsEndpoint::new(&endpoint_url, token).await?);

    let cursor: Option<String> = load_persisted_cursor()?;

    let mut stream = SubstreamsStream::new(
        endpoint.clone(),
        cursor,
        package.modules.clone(),
        module_name.to_string(),
        // Start/stop block are not handled within this project, feel free to play with it
        36472424,
        0,
    );

    loop {
        match stream.next().await {
            None => {
                println!("Stream consumed");
                break;
            }
            Some(Ok(BlockResponse::New(data))) => {
                process_block_scoped_data(&data).await?;
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

async fn process_block_scoped_data(data: &BlockScopedData) -> Result<(), Error> {
    // You can decode the actual Any type received using this code:
    //
    //     use prost::Message;
    //     let value = Message::decode::<GeneratedStructName>(data.value.as_slice())?;
    //
    // Where GeneratedStructName is the Rust code generated for the Protobuf representing
    // your type.
    if let Some(output) = &data.output {
        if let Some(map_output) = &output.map_output {
            let value = EntriesAdded::decode(map_output.value.as_slice())?;

            if value.entries.len() == 0 {
                println!("Empty Block #{}:", data.clock.as_ref().unwrap().number);
                return Ok(());
            }

            println!(
                "Block with some data #{}:",
                data.clock.as_ref().unwrap().number
            );

            let entries_to_fetch: Vec<_> = value
                .entries
                .iter()
                .map(Action::decode_from_entry)
                .collect();

            let entries = join_all(entries_to_fetch).await;

            let sink_actions = entries
                .iter()
                .filter_map(|action| action.get_sink_actions())
                .flatten()
                .collect::<Vec<_>>();

            let mut persist: Persist = Persist::open();

            for sink_action in sink_actions {
                sink_action
                    .handle_sink_action(&mut persist)
                    .expect("Couldn't Handle Sink Action");
            }

            persist.save();
        }
    }

    Ok(())
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
    let mut file = std::fs::File::create("cursor.txt")?;
    file.write_all(_cursor.as_bytes())?;

    Ok(())
}

fn load_persisted_cursor() -> Result<Option<String>, anyhow::Error> {
    if let Ok(cursor) = std::fs::read_to_string("cursor.txt") {
        return Ok(Some(cursor));
    }
    Ok(None)
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
