use anyhow::{format_err, Context, Error};
use clap::Parser;
use crossterm::event::{Event, KeyCode, KeyEventKind};
use futures03::future::try_join_all;
use futures03::stream::{FuturesOrdered, FuturesUnordered};
use futures03::{future::join_all, StreamExt};
use futures03::{Future, TryStreamExt};
use gui::{controls_handle, ui, GuiData, StatefulList};
use migration::DbErr;
use pb::schema::EntryAdded;
use pb::sf::substreams::rpc::v2::{BlockScopedData, BlockUndoSignal};
use pb::sf::substreams::v1::Package;

use entity::*;
use sea_orm::{ActiveValue, ConnectOptions, DatabaseTransaction, TransactionTrait};

use prost::Message;
use sea_orm::{Database, DatabaseConnection, EntityTrait};
use sea_query::OnConflict;
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

use models::cursor;

pub mod constants;
pub mod gui;
pub mod models;
mod pb;
mod persist;
mod sink_actions;
mod substreams;
mod substreams_stream;
mod triples;
pub mod tui;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
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

    /// Whether or not to use the GUI
    #[arg(short, long)]
    gui: bool,
}

pub async fn main() -> Result<(), Error> {
    let Args {
        substreams_endpoint: endpoint_url,
        spkg: package_file,
        module: module_name,
        token,
        host,
        username,
        password,
        port,
        gui,
    } = Args::parse();

    let start_block = 36472425;
    let stop_block = 46904315;
    //let stop_block = 36473425;

    let gui_data_lock = Arc::new(RwLock::new(GuiData {
        sink_start_time: SystemTime::now(),
        start_block,
        stop_block,
        block_number: 0,
        information_in_block: false,
        tasks: StatefulList::with_items(vec![]),
        task_count: 0,
        should_quit: false,
    }));

    let package = read_package(&package_file).await?;
    let endpoint = Arc::new(SubstreamsEndpoint::new(&endpoint_url, Some(token)).await?);

    let mut opt = ConnectOptions::new(format!("postgres://{username}:{password}@{host}/postgres"));
    opt.max_connections(499)
        .max_lifetime(Duration::from_secs(60))
        .connect_timeout(Duration::from_secs(60));
    let (message_sender, mut message_receiver) = channel(499); // 499 is the max number of connections

    // spawn a task to handle messages
    let gui_data_lock_clone = gui_data_lock.clone();
    let message_task = tokio::spawn(async move {
        while let Some(response) = message_receiver.recv().await {
            if !gui {
                //if response.starts_with("CREATE") {
                //println!("{}", response);
                //}
                println!("{}", response);
            } else {
                let mut gui_data = gui_data_lock_clone.write().await;
                gui_data.tasks.push(response);
                gui_data.task_count += 1;
                drop(gui_data);
            }
        }
    });

    let db: Arc<DatabaseConnection> = Arc::new(Database::connect(opt).await?);

    let cursor: Option<String> = cursor::get(&db, &message_sender).await?;

    let mut stream = SubstreamsStream::new(
        endpoint.clone(),
        cursor,
        package.modules.clone(),
        module_name.to_string(),
        // Start/stop block are not handled within this project, feel free to play with it
        start_block as i64,
        stop_block,
    );

    //let mut futures = Arc::new(RwLock::new(FuturesOrdered::new()));
    // we need to be able to respond to user input while the stream is running
    // ie close the stream and exit the program
    if gui {
        // let task_clone = gui_data_lock.clone();
        // let tui_task_clone = gui_data_lock.clone();
        // let controls_clone = gui_data_lock.clone();
        // let db_clone = gui_data_lock.clone();
        // let (sender, mut receiver) = unbounded_channel();

        // // spawn a concurrent process to handle the responses from the stream
        // let db_task = tokio::spawn(async move {
        //     let mut futures = FuturesUnordered::new();
        //     while let Some(join_handle) = receiver.recv().await {
        //         match join_handle {
        //             Some(join_handle) => {
        //                 futures.push(join_handle);
        //             }
        //             None => {
        //                 receiver.close();
        //             }
        //         }
        //     }

        //     while let Some(result) = futures.next().await {
        //         match result {
        //             Ok(_) => {}
        //             Err(err) => {
        //                 let mut gui_data = db_clone.write().await;
        //                 gui_data.tasks.push(format!("Error: {}", err));
        //                 gui_data.should_quit = true;
        //                 drop(gui_data);

        //                 // close the receiver channel so we exit on an error
        //                 receiver.close();
        //                 break;
        //             }
        //         }
        //     }
        // });
        // // spawn a concurrent process to handle the GUI
        // let tui = tokio::spawn(tui_handle(tui_task_clone, gui));
        // // spawn a concurrent process to handle the user input
        // let controls = tokio::spawn(controls_handle(controls_clone));
        // // spawn a concurrent process to handle the substream data
        // let stream_task = tokio::spawn(async move {
        //     loop {
        //         let stream_task_clone = task_clone.clone();
        //         let reader = stream_task_clone.read().await;
        //         // if the signal is to quit, close the stream
        //         if reader.should_quit {
        //             sender.send(None)?;
        //             return Ok(());
        //         }
        //         drop(reader);

        //         match stream.next().await {
        //             None => {
        //                 sender.send(None)?;
        //                 return Err(Error::msg("Stream consumed"));
        //             }
        //             Some(Ok(BlockResponse::New(data))) => {
        //                 let future = tokio::spawn(process_block_scoped_data(
        //                     data,
        //                     db.clone(),
        //                     stream_task_clone,
        //                     gui,
        //                 ));
        //                 sender.send(Some(future))?;
        //             }
        //             Some(Ok(BlockResponse::Undo(undo_signal))) => {
        //                 process_block_undo_signal(&undo_signal).unwrap();
        //             }
        //             Some(Err(err)) => {
        //                 println!();
        //                 println!("Stream terminated with error");
        //                 println!("{:?}", err);
        //             }
        //         }
        //     }
        // });

        // let res = try_join!(stream_task, tui, controls, db_task);

        // match res {
        //     Ok(_) => Ok(()),
        //     Err(err) => {
        //         message_task.abort();
        //         Err(err.into())
        //     }
        // }
        todo!("need to fix gui");
    } else {
        let task_clone = gui_data_lock.clone();
        let (sender, mut receiver) = channel(5); // the number of concurrent blocks to run at once

        // spawn a concurrent process to handle the responses from the stream
        let db_task = async {
            while let Some(future) = receiver.recv().await {
                match future {
                    Some(future) => {
                        future.await?;
                    }
                    None => {
                        println!("Stream consumed");
                        receiver.close();
                    }
                }
            }
            Ok(())
        };

        // spawn a concurrent process to handle the substream data
        let stream_task = async move {
            loop {
                let stream_task_clone = task_clone.clone();
                let reader = stream_task_clone.read().await;
                // if the signal is to quit, close the stream
                if reader.should_quit {
                    sender.send(None).await.unwrap();
                    return Ok(());
                }
                drop(reader);

                match stream.next().await {
                    None => {
                        println!("Stream consumed");
                        if let Err(err) = sender.send(None).await {
                            println!("Error sending to channel: {:?}", err);
                        }
                        return Ok(());
                    }
                    Some(Ok(BlockResponse::New(data))) => {
                        let future = process_block_scoped_data(
                            data,
                            db.clone(),
                            stream_task_clone,
                            gui,
                            message_sender.clone(),
                        );

                        if let Err(err) = sender.send(Some(future)).await {
                            println!("Error sending to channel: {:?}", err);
                        }
                    }
                    Some(Ok(BlockResponse::Undo(undo_signal))) => {
                        process_block_undo_signal(&undo_signal).unwrap();
                    }
                    Some(Err(err)) => {
                        println!();
                        println!("Stream terminated with error");
                        println!("{:?}", err);
                        return Err(Error::msg(err));
                    }
                }
            }
        };

        let res = try_join!(stream_task, db_task);

        match res {
            Ok(_) => {
                message_task.abort();
                Ok(())
            }
            Err(err) => {
                message_task.abort();
                Err(err.into())
            }
        }
    }
}

async fn process_block_scoped_data(
    data: BlockScopedData,
    db: Arc<DatabaseConnection>,
    gui_data_lock: Arc<RwLock<GuiData>>,
    use_gui: bool,
    sender: Sender<String>,
) -> Result<(), Error> {
    if let Some(output) = &data.output {
        if let Some(map_output) = &output.map_output {
            let value = EntriesAdded::decode(map_output.value.as_slice())?;
            let mut gui_data = gui_data_lock.write().await;

            let block_number = data.clock.as_ref().unwrap().number;
            gui_data.block_number = block_number;

            if value.entries.len() == 0 {
                // println!("Empty Block #{}:", data.clock.as_ref().unwrap().number);
                cursor::store(&db, data.cursor, block_number, &sender).await?;
                gui_data.information_in_block = false;
                return Ok(());
            }
            gui_data.information_in_block = true;
            drop(gui_data);

            //let (tx, mut rx) = channel(499);

            let entries = value.entries;

            let mut entry_futures = FuturesUnordered::from_iter(
                entries
                    .iter()
                    .map(|entry| handle_entry(&entry, db.clone(), sender.clone())),
            );

            // let message_task = tokio::spawn(async move {
            //     while let Some(response) = rx.recv().await {
            //         if !use_gui {
            //             //if response.starts_with("CREATE") {
            //             //println!("{}", response);
            //             //}
            //             println!("{}", response);
            //         } else {
            //             let mut gui_data = gui_data_lock.write().await;
            //             gui_data.tasks.push(response);
            //             gui_data.task_count += 1;
            //             drop(gui_data);
            //         }
            //     }
            // });

            cursor::store(&db, data.cursor, block_number, &sender).await?;

            while let Some(entry) = entry_futures.next().await {
                match entry {
                    Ok(_) => {}
                    Err(err) => {
                        return Err(Error::msg(err));
                    }
                }
            }

            drop(sender);
        }
    }

    Ok(())
}

async fn handle_entry(
    entry: &EntryAdded,
    db: Arc<DatabaseConnection>,
    sender: Sender<String>,
) -> Result<(), Error> {
    let action = Action::decode_from_entry(entry).await?;
    let sink_actions = action.clone().get_sink_actions();

    let sender_clone = sender.clone();
    let action_clone = action.clone();
    let db_clone = db.clone();
    let task_one = async move {
        let mut futures = FuturesUnordered::new();
        // we need to add the action triples to the database before we can handle the sink actions
        futures.push(action_clone.execute_action_triples(&db_clone, &sender_clone));

        while let Some(result) = futures.next().await {
            match result {
                Ok(_) => {}
                Err(err) => {
                    return Err(err);
                }
            }
        }

        Ok(())
    };

    let sender_clone = sender.clone();
    let db_clone = db.clone();

    let task_two = async move { handle_sink_actions(sink_actions, &db_clone, &sender_clone).await };

    let task_three = async move { action.add_author_to_db(&db, &sender).await };

    try_join!(task_one, task_two, task_three)?;

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
