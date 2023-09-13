use anyhow::{format_err, Context, Error};
use clap::Parser;
use crossterm::event::{Event, KeyCode, KeyEventKind};
use futures03::{future::join_all, StreamExt};
use gui::{ui, GuiData, StatefulList};
use migration::DbErr;
use pb::schema::EntryAdded;
use pb::sf::substreams::rpc::v2::{BlockScopedData, BlockUndoSignal};
use pb::sf::substreams::v1::Package;

use entity::*;
use sea_orm::{ActiveValue, ConnectOptions, DatabaseTransaction, TransactionTrait};

use prost::Message;
use sea_orm::{Database, DatabaseConnection, EntityTrait};
use sea_query::OnConflict;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::RwLock;
use tokio::time::Instant;
use tokio::{join, try_join};

use std::io::{Read, Stdout};
use std::time::Duration;
use std::{process::exit, sync::Arc};
use substreams::SubstreamsEndpoint;
use substreams_stream::{BlockResponse, SubstreamsStream};

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};

use ratatui::{
    backend::CrosstermBackend,
    widgets::{Block, Borders},
    Terminal,
};
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
    } = Args::parse();

    let start_block = 0;
    let stop_block = 46904315;

    let gui_data_lock = Arc::new(RwLock::new(GuiData {
        start_block,
        stop_block,
        block_number: 0,
        information_in_block: false,
        tasks: StatefulList::with_items(vec!["Test Item".to_string(), "Another Item".to_string()]),
    }));

    let package = read_package(&package_file).await?;
    let endpoint = Arc::new(SubstreamsEndpoint::new(&endpoint_url, Some(token)).await?);

    let mut opt = ConnectOptions::new(format!("postgres://{username}:{password}@{host}/postgres"));
    opt.max_lifetime(Duration::from_secs(1000))
        .max_connections(70)
        .connect_timeout(Duration::from_secs(1000));

    let db: DatabaseConnection = Database::connect(opt).await?;

    let cursor: Option<String> = cursor::get(&db).await?;

    let mut stream = SubstreamsStream::new(
        endpoint.clone(),
        cursor,
        package.modules.clone(),
        module_name.to_string(),
        // Start/stop block are not handled within this project, feel free to play with it
        36472424,
        46904315,
    );

    let gui_task_clone = gui_data_lock.clone();
    let terminal_task = tokio::task::spawn(async move {
        let mut terminal = setup_terminal().unwrap();
        run(&mut terminal, gui_task_clone).await;
        restore_terminal(&mut terminal);
    });

    let task_clone = gui_data_lock.clone();

    let stream_task = tokio::task::spawn(async move {
        let stream_task_clone = task_clone.clone();
        loop {
            let stream_task_clone = stream_task_clone.clone();
            match stream.next().await {
                None => {
                    println!("Stream consumed");
                    break;
                }
                Some(Ok(BlockResponse::New(data))) => {
                    process_block_scoped_data(data, &db, stream_task_clone).await;
                }
                Some(Ok(BlockResponse::Undo(undo_signal))) => {
                    process_block_undo_signal(&undo_signal).unwrap();
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

    let _ = join!(stream_task, terminal_task);

    Ok(())
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>, Error> {
    let mut stdout = io::stdout();
    //enable_raw_mode()?;
    execute!(stdout, EnterAlternateScreen)?;
    Ok(Terminal::new(CrosstermBackend::new(stdout))?)
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> Result<(), Error> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen,)?;
    Ok(terminal.show_cursor()?)
}

async fn run(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    gui_data_lock: Arc<RwLock<GuiData>>,
) -> Result<(), Error> {
    let mut render_count = 0;
    loop {
        render_count += 1;

        let gui_data = gui_data_lock.read().await;
        terminal.draw(|frame| {
            ui(frame, &gui_data, render_count);
        });
        drop(gui_data);
        tokio::time::sleep(Duration::from_millis(100)).await;
        // if event::poll(Duration::from_millis(0))? {
        //     if let Event::Key(key) = event::read()? {
        //         if KeyCode::Char('q') == key.code {
        //             break;
        //         }
        //     }
        // }
    }

    Ok(())
}

async fn process_block_scoped_data(
    data: BlockScopedData,
    db: &DatabaseConnection,
    gui_data_lock: Arc<RwLock<GuiData>>,
) -> Result<(), Error> {
    if let Some(output) = &data.output {
        if let Some(map_output) = &output.map_output {
            let value = EntriesAdded::decode(map_output.value.as_slice())?;
            let mut gui_data = gui_data_lock.write().await;

            gui_data.block_number = data.clock.as_ref().unwrap().number;

            if value.entries.len() == 0 {
                // println!("Empty Block #{}:", data.clock.as_ref().unwrap().number);
                cursor::store(db, data.cursor);
                gui_data.information_in_block = false;
                return Ok(());
            }
            gui_data.information_in_block = true;
            drop(gui_data);

            let (tx, mut rx) = tokio::sync::mpsc::channel(500_000);

            let db = db.clone();
            let results = tokio::task::spawn(async move {
                let entries_to_handle = value
                    .entries
                    .iter()
                    .map(|entry| handle_entry(&entry, &db, &tx))
                    .collect::<Vec<_>>();

                let results = join_all(entries_to_handle)
                    .await
                    .into_iter()
                    .flatten()
                    .collect::<Vec<Result<(), _>>>();

                cursor::store(&db, data.cursor).await;

                results
            });

            while let Some(response) = rx.recv().await {
                let mut gui_data = gui_data_lock.write().await;
                gui_data.tasks.items.push(response);
                drop(gui_data)
            }

            let results = try_join!(results);

            for result in results.unwrap().0 {
                if let Err(err) = result {
                    return Err(err.into());
                }
            }
        }
    }

    Ok(())
}

async fn handle_entry(
    entry: &EntryAdded,
    db: &DatabaseConnection,
    sender: &Sender<String>,
) -> Vec<Result<(), DbErr>> {
    let action = Action::decode_from_entry(entry).await;
    let sink_actions = action.get_sink_actions();
    handle_sink_actions(sink_actions, db, sender).await
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
