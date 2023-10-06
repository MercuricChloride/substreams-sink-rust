use anyhow::{format_err, Context, Error};
use clap::Parser;
use dotenv::dotenv;
use futures03::{future::join_all, StreamExt};
use futures03::{Future, TryStreamExt};
use gui::{controls_handle, ui, GuiData, StatefulList};
use migration::DbErr;
use models::triples::bootstrap;
use pb::schema::EntryAdded;
use pb::sf::substreams::rpc::v2::{BlockScopedData, BlockUndoSignal};
use pb::sf::substreams::v1::Package;

use entity::*;
use sea_orm::{ActiveValue, ConnectOptions, DatabaseTransaction, TransactionTrait};

use prost::Message;
use sea_orm::{Database, DatabaseConnection, EntityTrait};
use sea_query::OnConflict;
use sink_actions::SinkAction;
use tokio::sync::mpsc::{self, channel, unbounded_channel, Receiver, Sender, UnboundedSender};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio::{join, spawn, try_join};
use tui::tui_handle;

use std::io::{Read, Stdout};
use std::pin::Pin;
use std::time::{Duration, SystemTime};
use std::{process::exit, sync::Arc};
use substreams::SubstreamsEndpoint;
use substreams_stream::{BlockResponse, SubstreamsStream};

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};

use ratatui::{backend::CrosstermBackend, Terminal};
use std::{io, thread};

use crate::pb::schema::EntriesAdded;
use crate::sink_actions::handle_sink_actions;
use crate::triples::Action;

use commands::Args;

use models::cursor;

pub mod commands;
pub mod constants;
pub mod gui;
pub mod models;
pub mod pb;
pub mod persist;
pub mod retry;
pub mod sink_actions;
pub mod substreams;
pub mod substreams_stream;
pub mod triples;
pub mod tui;

pub const MAX_CONNECTIONS: usize = 499;
//pub const MAX_CONNECTIONS: usize = 1;

pub async fn main() -> Result<(), Error> {
    // load the .env file
    dotenv().ok();

    let Args {
        command,
        substreams_endpoint: endpoint_url,
        spkg: package_file,
        module: module_name,
        token,
        host,
        username,
        password,
        database,
        gui,
    } = Args::parse();

    //let start_block = 36472425;
    let start_block = 36800000;
    let stop_block = 47942548;

    let package = read_package(&package_file).await?;
    let endpoint = Arc::new(SubstreamsEndpoint::new(&endpoint_url, Some(token)).await?);

    // the reason for the long timeout is because any interactions with the db will be blocking if the db can't
    // handle any more connections at once.
    let mut connection_options = ConnectOptions::new(format!(
        "postgres://{username}:{password}@{host}/{database}"
    ));
    connection_options.max_connections(MAX_CONNECTIONS as u32);
    connection_options.connect_timeout(Duration::from_secs(60));
    connection_options.idle_timeout(Duration::from_secs(60));

    let db: Arc<DatabaseConnection> = Arc::new(Database::connect(connection_options).await?);

    // bootstrap the database
    bootstrap(&db).await?;

    let cursor: Option<String> = cursor::get(&db).await?;

    let mut stream = SubstreamsStream::new(
        endpoint.clone(),
        cursor,
        package.modules.clone(),
        module_name.to_string(),
        // Start/stop block are not handled within this project, feel free to play with it
        start_block as i64,
        stop_block,
    );

    if !gui {
        // spawn a concurrent process to handle the substream data
        let (tx, mut rx) = channel::<Action>(10000);

        let receiver_task = async {
            let mut start;
            while let Some(action) = rx.recv().await {
                let db = db.clone();
                start = Instant::now();
                println!("Processing entry");

                let result = match &command {
                    commands::Commands::Deploy { spaces } => {
                        handle_action(action, db.clone()).await
                    }
                    commands::Commands::DeployGlobal { root_space_address } => {
                        handle_global_action(action, db.clone()).await
                    }
                };

                if let Err(err) = result {
                    println!("Error processing entry: {:?}", err);
                    return Err(Error::from(err));
                }

                println!("Entry processed in {:?}", start.elapsed());
            }
            Ok(())
        };

        let stream_task = async {
            let tx = tx;
            loop {
                match stream.next().await {
                    None => {
                        println!("Stream consumed");
                        break;
                    }
                    Some(Ok(BlockResponse::New(data))) => {
                        if let Some(output) = &data.output {
                            if let Some(map_output) = &output.map_output {
                                let block_number = data.clock.as_ref().unwrap().number;
                                let value = EntriesAdded::decode(map_output.value.as_slice())?;
                                // if the block is empty, store the cursor and set the flag to false
                                if value.entries.len() == 0 {
                                    cursor::store(&db, data.cursor.clone(), block_number).await?;
                                } else {
                                    println!(
                                        "Processing block {}, {} entries to process.",
                                        block_number,
                                        value.entries.len()
                                    );
                                    process_block_scoped_data(value, data, db.clone(), &tx).await?;
                                }
                            }
                        }
                    }
                    Some(Ok(BlockResponse::Undo(undo_signal))) => {
                        process_block_undo_signal(&undo_signal).unwrap();
                    }
                    Some(Err(err)) => {
                        println!("Stream terminated with error");
                        println!("{:?}", err);
                        return Err(Error::msg(err));
                    }
                }
            }
            Ok(())
        };

        let res = try_join!(stream_task, receiver_task);

        match res {
            Ok(_) => Ok(()),
            Err(err) => Err(err.into()),
        }
    } else {
        todo!("need to fix gui");
        // TODO Add the message handler back if it is appropriate
        // spawn a task to handle messages
        // let gui_data_lock_clone = gui_data_lock.clone();
        // let message_task = tokio::spawn(async move {
        //     while let Some(response) = message_receiver.recv().await {
        //         if !gui {
        //             //if response.starts_with("CREATE") {
        //             //println!("{}", response);
        //             //}
        //             println!("{}", response);
        //         } else {
        //             let mut gui_data = gui_data_lock_clone.write().await;
        //             gui_data.tasks.push(response);
        //             gui_data.task_count += 1;
        //             drop(gui_data);
        //         }
        //     }
        // });
    }
}

async fn process_block_scoped_data(
    value: EntriesAdded,
    data: BlockScopedData,
    db: Arc<DatabaseConnection>,
    sender: &Sender<Action>,
) -> Result<(), Error> {
    let entries = value.entries;
    let block_number = data.clock.as_ref().unwrap().number;

    println!("Getting actions for block {}", block_number);
    let actions = join_all(entries.iter().map(Action::decode_from_entry)).await;
    println!("Actions retrieved");
    for action in actions.into_iter() {
        match action {
            Ok(action) => sender.send(action).await?,
            Err(err) => {
                println!("Error getting action: {:?}", err);
                continue;
            }
        }
    }
    println!("Actions sent");

    cursor::store(&db, data.cursor, block_number).await?;

    Ok(())
}

// This function should only use a single connection to the database
// We might want to make this function more concurrent, but we will see.
async fn handle_action(action: Action, db: Arc<DatabaseConnection>) -> Result<(), DbErr> {
    action.execute_action_triples(&db).await?;
    println!("Action triples added to db");

    action.add_author_to_db(&db).await?;
    println!("Author added to db");

    let sink_actions = action.get_sink_actions();
    handle_sink_actions(sink_actions, &db).await?;
    println!("Sink actions handled");

    Ok(())
}

/// This function is very similar to `handle_action`, however it is used to only perform the actions
/// for running the global, stripped down version of the sink.
/// So it operates on different sink actions etc.
async fn handle_global_action(action: Action, db: Arc<DatabaseConnection>) -> Result<(), DbErr> {
    //action.execute_action_triples(&db).await?;
    //println!("Action triples added to db");

    action.add_author_to_db(&db).await?;
    println!("Author added to db");

    let sink_actions = action.get_global_sink_actions();
    handle_sink_actions(sink_actions, &db).await?;
    println!("Sink actions handled");

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
