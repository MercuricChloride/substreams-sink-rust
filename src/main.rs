use anyhow::{format_err, Context, Error};
use clap::Parser;
use futures03::{future::join_all, StreamExt};
use pb::schema::EntryAdded;
use pb::sf::substreams::rpc::v2::{BlockScopedData, BlockUndoSignal};
use pb::sf::substreams::v1::Package;

use prost::Message;
use sink_actions::SqlError;
use std::io::Read;
use std::{process::exit, sync::Arc};
use substreams::SubstreamsEndpoint;
use substreams_stream::{BlockResponse, SubstreamsStream};

use tokio_postgres::{Client, NoTls};

use crate::pb::schema::EntriesAdded;
use crate::sink_actions::handle_sink_actions;
use crate::triples::Action;

pub mod constants;
mod pb;
mod persist;
mod sink_actions;
mod substreams;
mod substreams_stream;
mod triples;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Substreams endpoint
    #[arg(short, long)]
    #[clap(index = 1)]
    substreams_endpoint: String,

    /// Path or link to spkg
    #[arg(short, long)]
    #[clap(index = 2)]
    spkg: String,

    /// Module name
    #[arg(short, long)]
    #[clap(index = 3)]
    module: String,

    /// Postgres host
    #[arg(short, long)]
    host: String,

    /// Postgres username
    #[arg(short, long)]
    username: String,

    /// Postgres password
    #[arg(short, long)]
    password: String,

    /// Postgres port, default 5432
    #[arg(short, long, default_value = "5432")]
    port: String,

    /// Substreams API token, if not provided, SUBSTREAMS_API_TOKEN environment variable will be used
    #[arg(short, long, env = "SUBSTREAMS_API_TOKEN")]
    token: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let Args {
        substreams_endpoint: endpoint_url,
        spkg: package_file,
        module: module_name,
        token,
        host,
        username,
        password,
        port,
    } = Args::parse();

    let package = read_package(&package_file).await?;
    let endpoint = Arc::new(SubstreamsEndpoint::new(&endpoint_url, Some(token)).await?);

    let connection_string = format!(
        "host={} user={} port={} password={}",
        host, username, port, password
    );

    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let cursor: Option<String> = load_persisted_cursor(&client).await?;

    let mut stream = SubstreamsStream::new(
        endpoint.clone(),
        cursor,
        package.modules.clone(),
        module_name.to_string(),
        // Start/stop block are not handled within this project, feel free to play with it
        36472424,
        46904315,
    );

    let mut failed_queries: Vec<SqlError> = vec![];

    loop {
        match stream.next().await {
            None => {
                println!("Stream consumed");
                break;
            }
            Some(Ok(BlockResponse::New(data))) => {
                process_block_scoped_data(&data, &client, &mut failed_queries).await?;
            }
            Some(Ok(BlockResponse::Undo(undo_signal))) => {
                process_block_undo_signal(&undo_signal)?;
                //persist_cursor(undo_signal.last_valid_cursor, &client).await?;
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

async fn process_block_scoped_data(
    data: &BlockScopedData,
    client: &Client,
    failed_queries: &mut Vec<SqlError>,
) -> Result<(), Error> {
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

            let entries_to_handle = value
                .entries
                .iter()
                .map(|entry| handle_entry(&entry, &client))
                .collect::<Vec<_>>();

            // wait for all the sink actions to finish
            let sql_results = join_all(entries_to_handle).await;

            // prepare the retry queries futures
            let retry_queries_futures = failed_queries
                .iter()
                .map(|query| query.retry_query(client))
                .collect::<Vec<_>>();

            // join all the retry queries futures
            let retry_queries = join_all(retry_queries_futures)
                .await
                .into_iter()
                .filter_map(|result| match result {
                    Ok(_) => None,
                    Err(err) => Some(err),
                })
                .collect::<Vec<_>>();

            // get all of the new query failures
            let query_failures = sql_results
                .into_iter()
                .flatten()
                .filter_map(|result| {
                    if let Err(err) = result {
                        match &err {
                            SqlError::UnsafeFailure(query) => {
                                println!("Unsafe query failed {}", query);
                                Some(err.clone())
                            }
                            SqlError::SafeQueryFailure(query) => {
                                println!("Safe query failed {:?}", query);
                                None
                            }
                            SqlError::BothQueriesFailed(safe_query, unsafe_query) => {
                                println!(
                                    "Both queries failed safe: {:?} unsafe:{}",
                                    safe_query, unsafe_query
                                );
                                None
                            }
                        }
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            // reset the failed queries to the new failed queries and the old ones that didn't work again
            failed_queries.clear();
            failed_queries.extend(query_failures);
            failed_queries.extend(retry_queries);

            // if the data isn't empty, lets store the cursor
            persist_cursor(&data.cursor, &client).await?;
        }
    }

    Ok(())
}

async fn handle_entry(entry: &EntryAdded, client: &Client) -> Vec<Result<(), SqlError>> {
    let action = Action::decode_from_entry(entry).await;
    let sink_actions = action.get_sink_actions();
    if let Some(sink_actions) = sink_actions {
        handle_sink_actions(sink_actions, client).await
    } else {
        vec![]
    }
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

async fn persist_cursor(_cursor: &String, client: &Client) -> Result<(), anyhow::Error> {
    let rows_changed = client
        .execute("UPDATE cursors set cursor = $1 where id = 0", &[_cursor])
        .await?;

    println!("updated {} rows", rows_changed);

    Ok(())
}

async fn load_persisted_cursor(client: &Client) -> Result<Option<String>, anyhow::Error> {
    let row = client
        .query_one("SELECT cursor FROM cursors WHERE id=0", &[])
        .await?;
    let cursor: &str = row.get("cursor");

    println!("Found cursor: {}", cursor);

    match cursor {
        s if s.starts_with("") => Ok(None),
        _ => Ok(Some(cursor.to_string())),
    }
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
