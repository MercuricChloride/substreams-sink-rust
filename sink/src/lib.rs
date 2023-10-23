use crate::pb::schema::EntriesAdded;
use crate::sink_actions::ActionDependencies;
use crate::triples::Action;
use anyhow::{format_err, Context, Error};
use clap::Parser;
use commands::Args;
use dotenv::dotenv;
use futures03::stream::FuturesUnordered;
use futures03::Future;
use futures03::{
    future::{join_all, try_join_all},
    stream::FuturesOrdered,
    StreamExt,
};
use migration::DbErr;
use models::{cursor, triples::bootstrap};
use pb::sf::substreams::{
    rpc::v2::{BlockScopedData, BlockUndoSignal},
    v1::Package,
};
use prost::Message;
use sea_orm::{
    ConnectOptions, ConnectionTrait, DatabaseTransaction, IsolationLevel, TransactionTrait,
};
use sea_orm::{Database, DatabaseConnection};
use sink_actions::{SinkAction, SinkActionDependency};
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::{sync::Arc, time::Duration};
use substreams::SubstreamsEndpoint;
use substreams_stream::{BlockResponse, SubstreamsStream};
use tokio::sync::RwLock;
use tokio::{
    sync::mpsc::{channel, Sender},
    time::Instant,
    try_join,
};
use triples::ActionTriple;

pub mod actions;
pub mod commands;
pub mod constants;
pub mod macros;
pub mod models;
pub mod pb;
pub mod queries;
pub mod retry;
pub mod sink_actions;
pub mod substreams;
pub mod substreams_stream;
pub mod triples;

pub async fn main() -> Result<(), Error> {
    // load the .env file
    dotenv().ok();

    let Args {
        command,
        substreams_endpoint: endpoint_url,
        spkg: package_file,
        module: module_name,
        token,
        database_url,
        max_connections,
        action_cache,
    } = Args::parse();

    let start_block = 36472425;
    let stop_block = 47942548;

    // Load the pacakge and endpoint
    let package = read_package(&package_file).await?;
    let endpoint = Arc::new(SubstreamsEndpoint::new(&endpoint_url, Some(token)).await?);

    // configure the database connections
    let mut connection_options = ConnectOptions::new(database_url);
    connection_options.max_connections(max_connections);
    connection_options.connect_timeout(Duration::from_secs(60));
    connection_options.idle_timeout(Duration::from_secs(60));

    let db: Arc<DatabaseConnection> = Arc::new(Database::connect(connection_options).await?);

    // bootstrap the database
    #[cfg(not(feature = "no_db_sync"))]
    bootstrap(db.clone()).await?;

    let cursor: Option<String> = cursor::get(&db).await?;

    let mut stream = SubstreamsStream::new(
        endpoint.clone(),
        cursor,
        package.modules.clone(),
        module_name.to_string(),
        start_block as i64,
        stop_block,
    );

    // spawn a concurrent process to handle the substream data
    let (tx, mut rx) = channel::<Box<Action>>(10000);

    #[cfg(not(feature = "no_db_sync"))]
    let receiver_task = async {
        let (spaces, use_space_queries) = match command {
            commands::Commands::Deploy { spaces } => (spaces, true),
            commands::Commands::DeployGlobal { root_space_address } => {
                (vec![root_space_address], false)
            }
        };

        let mut start;
        while let Some(action) = rx.recv().await {
            let db = db.clone();
            start = Instant::now();
            println!("Processing entry");

            let result = handle_action(
                *action,
                db.clone(),
                use_space_queries,
                max_connections as usize,
            ).await?;

            let ActionFutures{ actions_and_author  } = result;

            actions_and_author.await?;

            println!("Entry processed in {:?}", start.elapsed());
        }
        Ok::<(), Error>(())
    };

    if let Some(action_cache) = action_cache {
        let task = async {
            // if the action cache is defined, we don't want to start a stream and instead read the actions from the cache
            let path = format!("{action_cache}");
            let entries = std::fs::read_dir(path.clone())
                .map_err(|err| format_err!("Error reading action cache directory: {:?} {err:?}", &path))?;
            let mut entries: Vec<_> = entries.filter_map(Result::ok).collect();

            println!("Sorting entries");
            entries.sort_by(|a, b| {
                let a = a.file_name();
                let b = b.file_name();

                let a = a.to_str().unwrap();
                let b = b.to_str().unwrap();

                let a = a.split("_").collect::<Vec<_>>();
                let b = b.split("_").collect::<Vec<_>>();

                let a_block = a[0].parse::<usize>().unwrap();
                let b_block = b[0].parse::<usize>().unwrap();

                if a_block == b_block {
                    a[1].parse::<usize>()
                        .unwrap()
                        .cmp(&b[1].parse::<usize>().unwrap())
                } else {
                    a_block.cmp(&b_block)
                }
            });

            for entry in entries.into_iter() {
                let entry_data = std::fs::read(&entry.path()).expect("Couldn't read entry data for {entry:?}");
                let path_str = entry.path().to_str().unwrap().to_string();
                let split_path = path_str.split("_").collect::<Vec<_>>();

                let space = split_path[3].to_string();
                let author = split_path[4].trim_end_matches(".json").to_string();

                let mut action: Action = serde_json::from_slice(&entry_data)?;
                action.space = space.clone();
                action.author = author.clone();

                for triple in action.actions.iter_mut() {
                    update_space_and_author(triple, space.clone(), author.clone());
                }

                tx.send(Box::new(action)).await?;
            }
            Ok::<(), Error>(())
        };

        #[cfg(not(feature = "no_db_sync"))]
        let res = try_join!(task, receiver_task);

        match res {
            Ok(_) => Ok(()),
            Err(err) => Err(err.into()),
        }
    } else {
        let stream_task = async {
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
                                println!("Processing block {}", block_number);
                                let value =
                                    EntriesAdded::decode(map_output.value.as_slice()).unwrap();
                                let tx = tx.clone();
                                // if the block is empty, store the cursor and set the flag to false
                                if value.entries.len() == 0 {
                                    cursor::store(&db, data.cursor.clone(), block_number).await?;
                                } else {
                                    println!(
                                        "Processing block {}, {} entries to process.",
                                        block_number,
                                        value.entries.len()
                                    );
                                    async move {
                                        let futures: Vec<_> = value
                                            .entries
                                            .into_iter()
                                            .map(|entry| Action::decode_from_entry(entry))
                                            .collect();
                                        let actions = join_all(futures).await.into_iter().filter_map(|action| action.ok()).collect::<Vec<_>>();
                                        for action in actions {
                                            tx.send(Box::new(action)).await.unwrap();
                                        }
                                    }
                                    .await;
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

        #[cfg(not(feature = "no_db_sync"))]
        let res = try_join!(stream_task, receiver_task);

        match res {
            Ok(_) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}

// This function should only use a single connection to the database
// We might want to make this function more concurrent, but we will see.

pub struct ActionFutures {
    pub actions_and_author: Pin<Box<dyn Future<Output = Result<(), Error>>>>,
    //pub sink_actions: Pin<Box<dyn Future<Output = Result<(), Error>>>>,
}

async fn handle_action(
    action: Action,
    db: Arc<DatabaseConnection>,
    use_space_queries: bool,
    max_connections: usize,
) -> Result<ActionFutures, Error> {
    // let sink_actions = if use_space_queries {
    //     action.get_sink_actions()
    // } else {
    //     action.get_global_sink_actions()
    // };

    let actions_and_author = Box::pin(async move {
        let txn = db.clone().begin().await?;

        action
            .execute_action_triples(&txn, use_space_queries, max_connections)
            .await?;

        action.add_author_to_db(&txn).await?;

        txn.commit().await?;
        Ok::<(), Error>(())
    });

    Ok(ActionFutures { actions_and_author })

    // let space = &action.space;
    // let author = &action.author;
    // try_action(sink_actions, db, use_space_queries, author, space).await?;

    //Ok(())
}

async fn try_action<'a>(
    actions: Vec<SinkAction<'a>>,
    db: Arc<DatabaseConnection>,
    use_space_queries: bool,
    author: &str,
    space: &str,
) -> Result<(), Error> {
    let mut actions: VecDeque<SinkAction<'a>> = actions.into();
    let mut waiting_queue: VecDeque<SinkAction<'_>> = VecDeque::new();
    // These denote the first tier of actions to handle in the DB(things that have no deps)
    let mut first_actions: Vec<_> = Vec::new();
    // These denote the third tier of actions to handle in the DB(actions with deps that should be handled in tier 1 or 2)
    let mut third_actions: Vec<_> = Vec::new();

    // Contains a map from a Sink Action Dependency to a boolean indicating whether or not the dependency has been met
    let mut dependency_nodes: HashMap<SinkActionDependency, bool> = HashMap::new();
    // Contains tuples of (Action, DependentAction)
    let mut dependency_edges: Vec<(SinkAction<'_>, SinkActionDependency)> = Vec::new();

    let first_txn = db
        .begin_with_config(Some(IsolationLevel::ReadCommitted), None)
        .await?;

    while let Some(action) = actions.pop_front() {
        if let Some(_) = action.dependencies() {
            waiting_queue.push_back(action);
        } else {
            // if something doesn't have dependencies, we can push it directly to the first_actions queue
            let sub_tx = first_txn.begin().await?;
            let move_action = action.clone();
            first_actions.push(async move {
                move_action
                    .execute(&sub_tx, use_space_queries)
                    .await
                    .unwrap();
                sub_tx.commit().await
            });
        }
    }

    println!("Handling first actions...\n");
    try_join_all(first_actions).await?;
    println!("Handled all the first actions");

    while let Some(action) = waiting_queue.pop_front() {
        let mut dependencies_met = true;

        println!("Checking fallback actions for: {action:?}");
        // Fallback actions are the default actions that should be executed
        let dependencies = action.dependencies();

        if action.has_fallback() {
            if let Some(dependencies) = dependencies {
                let mut filtered_dependencies = vec![];

                for dep in dependencies.iter() {
                    filtered_dependencies.push(dep);
                }

                let flat_dependencies: Vec<_> = filtered_dependencies
                    .iter()
                    .flat_map(|dep| {
                        dep.fallback_actions(author.into(), space.into())
                            .unwrap_or(vec![])
                    })
                    .collect();

                for action in flat_dependencies {
                    println!("executing fallback dependency {action:?}");
                    let cloned = action.clone();
                    if let Some(as_dep) = SinkActionDependency::match_action(&action) {
                        let is_met = as_dep.met(&mut dependency_nodes, &first_txn).await.unwrap();
                        if !is_met {
                            dependency_nodes.insert(as_dep, true);
                            cloned.execute(&first_txn, use_space_queries).await?;
                        }
                    }
                }
            }
        } else {
            if let Some(dependencies) = dependencies {
                // handle the dependencies
                for dep in dependencies {
                    if !dep.met(&mut dependency_nodes, &first_txn).await? {
                        // println!("Dependency {:?} not met for action: {:?}", dep, action);
                        dependencies_met = false;
                    }
                }
            }
        }

        if dependencies_met {
            println!("Executing waiting list action: {:?}", action);

            let sub_tx = first_txn.begin().await?;
            let move_action = action.clone();
            //second_actions.push(async move {
            if let Ok(_) = move_action.execute(&sub_tx, use_space_queries).await {
                sub_tx.commit().await?;
            } else {
                println!("COULDN'T HANDLE ACTION: {action:?}");
                sub_tx.rollback().await?;
            }
            //});

            if let Some(dep) = SinkActionDependency::match_action(&action) {
                dependency_nodes.insert(dep, true);
                // loop through the dependency edges and remove any edges that have this action as a dependent
                // and push them to the action queue
                dependency_edges = dependency_edges
                    .into_iter()
                    .filter(|(act, _)| act != &action)
                    .collect();
            }
        } else {
            println!("Dependencies not met for action: {:?}", action);
            let sub_tx = first_txn.begin().await?;
            let move_action = action.clone();
            third_actions.push(async move {
                move_action
                    .execute(&sub_tx, use_space_queries)
                    .await
                    .unwrap();
                sub_tx.commit().await
            });
        }
    }

    println!("Handling third actions...\n");
    if let Err(err) = try_join_all(third_actions).await {
        println!("Error handling sink actions: {:?}", err);
        return Err(err.into());
    } else {
        println!("Handled third actions!");
    }

    first_txn.commit().await?;
    if !dependency_edges.is_empty() {
        return Err(format_err!("Dependency edges remaining"));
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

async fn read_package(input: &str) -> Result<Package, anyhow::Error> {
    if input.starts_with("http") {
        return read_http_package(input).await;
    }

    let content =
        std::fs::read(input).context(format_err!("read package from file '{}'", input))?;
    Package::decode(content.as_ref()).context("decode command")
}

async fn read_http_package(input: &str) -> Result<Package, anyhow::Error> {
    let body = reqwest::get(input).await?.bytes().await?;

    Package::decode(body).context("decode command")
}

fn update_space_and_author(triple: &mut ActionTriple, new_space: String, new_author: String) {
    match triple {
        ActionTriple::CreateEntity { space, author, .. } => {
            *space = new_space;
            *author = new_author;
        }
        ActionTriple::CreateTriple { space, author, .. } => {
            *space = new_space;
            *author = new_author;
        }

        ActionTriple::DeleteTriple { space, author, .. } => {
            *space = new_space;
            *author = new_author;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use entity::cursors;
    use tokio::{sync::mpsc, task};

    use crate::triples::ActionTriple;

    use super::*;

    // the cid we are testing for
    const IPFS_CID: &str = "QmZoQVLafegfRqkmM7Px5FuTbEArBhGec3TVRA5b5VQrun";

    #[tokio::test]
    async fn test_entry() {
        dotenv().ok();

        let max_connections = 100;
        let database_url = std::env::var("DATABASE_URL").unwrap();

        let mut connection_options = ConnectOptions::new(database_url);
        connection_options.max_connections(max_connections);
        connection_options.connect_timeout(Duration::from_secs(60));
        connection_options.idle_timeout(Duration::from_secs(60));

        let db: Arc<DatabaseConnection> =
            Arc::new(Database::connect(connection_options).await.unwrap());

        let space = "0xe3d08763498e3247ec00a481f199b018f2148723";
        let author = "0x66703c058795b9cb215fbcc7c6b07aee7d216f24";

        // we should always have the ipfs data cached locally, so we can just read the data from a file
        let file = std::fs::read_to_string(format!("../ipfs-data/{}.json", IPFS_CID)).unwrap();

        let mut action: Action = serde_json::from_slice(file.as_bytes()).unwrap();
        for triple in action.actions.iter_mut() {
            update_space_and_author(triple, space.to_string(), author.to_string());
        }

        let sink_actions = action.get_sink_actions();

        let txn = db.begin().await.unwrap();
        //handle_sink_actions(sink_actions, &txn, true, max_connections as usize)
        //.await
        //.unwrap();
        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_all_entries() {
        dotenv().ok();

        let max_connections = 199;
        let database_url = std::env::var("DATABASE_URL").unwrap();

        let mut connection_options = ConnectOptions::new(database_url);
        connection_options.max_connections(max_connections);
        connection_options.min_connections(max_connections);
        connection_options.connect_timeout(Duration::from_secs(60));
        connection_options.idle_timeout(Duration::from_secs(60));

        let db: Arc<DatabaseConnection> =
            Arc::new(Database::connect(connection_options).await.unwrap());

        let entries = std::fs::read_dir("../action_cache").unwrap();
        let mut entries: Vec<_> = entries.filter_map(Result::ok).collect();

        println!("Sorting entries");
        entries.sort_by(|a, b| {
            let a = a.file_name();
            let b = b.file_name();

            let a = a.to_str().unwrap();
            let b = b.to_str().unwrap();

            let a = a.split("_").collect::<Vec<_>>();
            let b = b.split("_").collect::<Vec<_>>();

            let a_block = a[0].parse::<usize>().unwrap();
            let b_block = b[0].parse::<usize>().unwrap();

            if a_block == b_block {
                a[1].parse::<usize>()
                    .unwrap()
                    .cmp(&b[1].parse::<usize>().unwrap())
            } else {
                a_block.cmp(&b_block)
            }
        });

        bootstrap(db.clone()).await.unwrap();

        for entry in entries.into_iter() {
            let entry_data = std::fs::read(&entry.path()).unwrap();
            let path_str = entry.path().to_str().unwrap().to_string();
            let split_path = path_str.split("_").collect::<Vec<_>>();

            let space = split_path[3].to_string();
            let author = split_path[4].trim_end_matches(".json").to_string();

            let mut action: Action = serde_json::from_slice(&entry_data).unwrap();
            action.space = space.clone();
            action.author = author.clone();

            for triple in action.actions.iter_mut() {
                update_space_and_author(triple, space.clone(), author.clone());
            }

            println!("Processing action");
            handle_action(action, db.clone(), true, max_connections as usize)
                .await
                .unwrap();
            println!("Done w action");
        }
    }
}
