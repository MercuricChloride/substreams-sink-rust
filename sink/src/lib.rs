use crate::pb::schema::EntriesAdded;
use crate::sink_actions::{handle_sink_actions, ActionDependencies};
use crate::triples::Action;
use anyhow::{format_err, Context, Error};
use clap::Parser;
use commands::Args;
use dotenv::dotenv;
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
use sink_actions::{SinkAction, SinkActionDependencies};
use std::collections::{HashMap, VecDeque};
use std::{sync::Arc, time::Duration};
use substreams::SubstreamsEndpoint;
use substreams_stream::{BlockResponse, SubstreamsStream};
use tokio::{
    sync::mpsc::{channel, Sender},
    time::Instant,
    try_join,
};

pub mod actions;
pub mod commands;
pub mod constants;
pub mod models;
pub mod pb;
pub mod persist;
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
    let txn = db.begin().await?;
    bootstrap(&txn).await?;
    txn.commit().await?;

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
    let (tx, mut rx) = channel::<Action>(10000);

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

            handle_action(
                action,
                db.clone(),
                use_space_queries,
                max_connections as usize,
            )
            .await?;

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
                            println!("Processing block {}", block_number);
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

    #[cfg(not(feature = "no_db_sync"))]
    let res = try_join!(stream_task, receiver_task);

    #[cfg(feature = "no_db_sync")]
    let res = stream_task.await;

    match res {
        Ok(_) => Ok(()),
        Err(err) => Err(err.into()),
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
    let actions = join_all(entries.iter().map(|entry| async move {
        (
            Action::decode_from_entry(entry).await,
            entry.author.clone(),
            entry.space.clone(),
        )
    }))
    .await;
    println!("Actions retrieved");

    for (i, (action, author, space)) in actions.into_iter().enumerate() {
        match action {
            Ok(action) => {
                #[cfg(feature = "no_db_sync")]
                {
                    let file_path =
                        format!("./action_cache/{block_number}_{i}_{space}_{author}.json");
                    std::fs::write(&file_path, serde_json::to_string(&action).unwrap()).unwrap();
                    println!("Wrote action to file");
                }

                #[cfg(not(feature = "no_db_sync"))]
                sender.send(action).await?
            }
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
async fn handle_action(
    action: Action,
    db: Arc<DatabaseConnection>,
    use_space_queries: bool,
    max_connections: usize,
) -> Result<(), Error> {
    action
        .execute_action_triples(&db, use_space_queries, max_connections)
        .await?;
    println!("Action triples added to db");

    action.add_author_to_db(&db).await?;
    println!("Author added to db");

    let sink_actions = if use_space_queries {
        action.get_sink_actions()
    } else {
        action.get_global_sink_actions()
    };

    let txn = db.begin().await?;
    try_action(sink_actions, txn, use_space_queries).await?;
    Ok(())
}

async fn try_action(
    actions: Vec<SinkAction<'_>>,
    main: DatabaseTransaction,
    use_space_queries: bool,
) -> Result<(), Error> {
    let mut actions: VecDeque<SinkAction<'_>> = actions.into();
    let initial_len = actions.len();
    let mut waiting_queue: VecDeque<SinkAction<'_>> = VecDeque::new();

    // Contains a map from a Dependency Sink Action (a sink action that is a dependency of another sink action)
    // to a boolean indicating whether or not the dependency has been met
    let mut dependency_nodes: HashMap<SinkAction<'_>, bool> = HashMap::new();
    // Contains tuples of (Action, DependentAction)
    let mut dependency_edges: Vec<(SinkAction<'_>, SinkAction<'_>)> = Vec::new();

    let txn = main.begin().await?;
    while let Some(action) = actions.pop_front() {
        let mut dependencies_met = true;

        if let Some(dependencies) = action.dependencies() {
            println!("\n\n\n with dependencies");
            // check all of the dependencies of the action
            for dep in dependencies {
                println!("\n\t-{dep:?}");
                let exists_in_graph = dependency_nodes.entry(dep).or_insert(false);
                let exists_in_db = dep.check_if_exists(&txn).await.unwrap();

                let exists_in_db = *exists_in_graph || exists_in_db;
                *exists_in_graph = exists_in_db;

                // if the dependency still isn't met, we need to add an edge between the action and the dependency
                // and set the dependencies_met flag to false
                if !exists_in_db {
                    dependency_edges.push((action, dep));
                    dependencies_met = false;
                }
            }
        }

        if dependencies_met {
            println!("\nExecuting action: {:?}", action);
            action.execute(&txn, use_space_queries).await?;

            let as_dep = action.as_dep();
            dependency_nodes.insert(as_dep, true);

            // Create a list of actions that were dependent on the action we just executed
            let mut dependent_actions = Vec::new();
            dependency_edges = dependency_edges
                .into_iter()
                .filter(|(action, dep)| {
                    if dep == &as_dep {
                        dependent_actions.push(*action);
                        false
                    } else {
                        true
                    }
                })
                .collect();

            for action in dependent_actions {
                // if we can't find the action in the dependency edges, we can add it to the waiting queue
                // otherwise, it still has dependencies that need to be met
                if dependency_edges
                    .iter()
                    .find(|(a, _)| a == &action)
                    .is_none()
                {
                    waiting_queue.push_back(action);
                }
            }
        } else {
            //println!("Dependencies not yet met for action: {:?}", action);
            waiting_queue.push_back(action.clone());
        }
    }
    txn.commit().await?;

    let mut count = 0;
    while let Some(action) = waiting_queue.pop_front() {
        if count > 15 {
            &main.commit().await?;
            return Ok(());
        } else {
            let mut dependencies_met = true;

            println!("Checking fallback actions for: {action:?}");
            // Fallback actions are the default actions that should be executed
            if let Some(fallback_actions) = action.fallback() {
                let mut actions_to_take = Vec::new();
                for fallback_action in fallback_actions.into_iter() {
                    let as_dep = fallback_action.as_dep();
                    let exists = dependency_nodes.entry(as_dep).or_insert(false);
                    let exists_in_db = as_dep.check_if_exists(&main).await?;
                    if exists_in_db {
                        *exists = true;
                    }
                    let exists_in_db = *exists || exists_in_db;
                    if !exists_in_db {
                        println!("\t- Fallback action exists: {:?}", fallback_action);
                        actions_to_take.push(fallback_action);
                    }
                }

                for fallback_action in actions_to_take.iter() {
                    fallback_action.execute(&main, use_space_queries).await?;
                    let as_dep = fallback_action.as_dep();
                    dependency_nodes.insert(as_dep.clone(), true);
                }
            }

            if let Some(dependencies) = action.dependencies() {
                // handle the dependencies
                for dep in dependencies {
                    let txn = main.begin().await?;
                    let exists_in_graph = dependency_nodes.entry(dep.clone()).or_insert(false);
                    let exists_in_db = dep.check_if_exists(&txn).await?;
                    let exists_in_db = *exists_in_graph || exists_in_db;
                    *exists_in_graph = exists_in_db;
                    if !exists_in_db {
                        //println!("Dependency {:?} not met for action: {:?}", dep, action);
                        dependencies_met = false;
                    }
                    txn.commit().await?;
                }
            }

            if dependencies_met {
                //println!("Executing waiting list action: {:?}", action);
                action.execute(&main, use_space_queries).await?;

                let as_dep = action.as_dep();
                dependency_nodes.insert(as_dep, true);

                // loop through the dependency edges and remove any edges that have this action as a dependent
                // and push them to the action queue
                dependency_edges = dependency_edges
                    .into_iter()
                    .filter(|(act, dep)| {
                        if act == &action {
                            //waiting_queue.push_back(action.clone());
                            false
                        } else {
                            true
                        }
                    })
                    .collect();
            } else {
                //println!("Dependencies not met for action: {:?}", action);
                waiting_queue.push_back(action.clone());
            }
        }
    }

    main.commit().await?;

    if !dependency_edges.is_empty() {
        let dependency_len = dependency_edges.len();
        println!("Initial Actions {initial_len} actions. \n Actions Left: {dependency_len}.\n\n REMAINING ACTIONS: {}", dependency_edges.iter().map(|e| format!("{:?}", e)).collect::<Vec<_>>().join("\n\n"));
        return Err(format_err!("Dependency edges remaining"));
    }

    Ok(())
}

async fn try_execute(
    sink_actions: &Vec<SinkAction<'_>>,
    txn: DatabaseTransaction,
    use_space_queries: bool,
    max_connections: usize,
    max_failures: i32,
) -> Result<(), Error> {
    let mut failed_count = 0;
    while failed_count < max_failures {
        let txn = txn.begin().await?;
        let result =
            handle_sink_actions(sink_actions, &txn, use_space_queries, max_connections).await;

        if let Err(err) = result {
            println!("Error handling sink actions: {:?}, retrying", err);
            txn.rollback().await?;
            failed_count += 1;
            // sleep a random duration between 1 and 5 seconds
            let sleep_duration = rand::random::<u64>() % 5 + 1;
            tokio::time::sleep(Duration::from_secs(sleep_duration)).await;
            continue;
        } else {
            txn.commit().await.unwrap();
            break;
        }
    }

    if failed_count == max_failures {
        txn.rollback().await.unwrap();
        Err(format_err!("Too many failures"))
    } else {
        txn.commit().await.unwrap();
        println!("Deafult actions handled");
        Ok(())
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use entity::cursors;

    use crate::triples::ActionTriple;

    use super::*;

    // the cid we are testing for
    const IPFS_CID: &str = "QmZoQVLafegfRqkmM7Px5FuTbEArBhGec3TVRA5b5VQrun";

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
        // this test will:
        // 1. Start counting up from the start_block
        // 2. For each block, get the cached_actions from the action_cache/ directory
        // 3. For those actions, sort them by their index, and then execute them in order
        // 4. Repeat until we reach the stop_block
        let blocks_with_data_file = std::fs::read_to_string("./blocks_with_data.json").ok();
        let mut blocks_with_data = if let Some(file) = &blocks_with_data_file {
            serde_json::from_str::<HashMap<u64, bool>>(file).unwrap()
        } else {
            HashMap::new()
        };

        let mut block_data_vector = match blocks_with_data_file {
            Some(_) => Some(blocks_with_data.keys().cloned().collect::<Vec<_>>()),
            None => None,
        };
        if let Some(block_data_vector) = &mut block_data_vector {
            block_data_vector.sort();
            block_data_vector.reverse();
        }

        dotenv().ok();

        let max_connections = 400;
        let database_url = std::env::var("DATABASE_URL").unwrap();

        let mut connection_options = ConnectOptions::new(database_url);
        connection_options.max_connections(max_connections);
        connection_options.min_connections(max_connections);
        connection_options.connect_timeout(Duration::from_secs(60));
        connection_options.idle_timeout(Duration::from_secs(60));

        let db: Arc<DatabaseConnection> =
            Arc::new(Database::connect(connection_options).await.unwrap());

        let mut start_block = if let Some(block_data_vector) = &mut block_data_vector {
            block_data_vector.pop().unwrap()
            //39251292
        } else {
            36472425
            //39251292
        };
        //let start_block = 37673931;

        let stop_block: u64 = cursor::get_block_number(&db)
            .await
            .unwrap()
            .unwrap()
            .parse()
            .unwrap();
        let entries = std::fs::read_dir("../action_cache").unwrap();
        let mut entries: Vec<_> = entries.filter_map(Result::ok).collect();

        // we need to sort the matching files by their index (the number after the prefix)
        // all of the files are of the form: {block_number}_{index}_{space}_{author}.json
        let mut block_map: HashMap<u64, bool> = HashMap::new();
        for entry in entries.iter() {
            if let Some(filename) = entry.path().file_name() {
                if let Some(filename_str) = filename.to_str() {
                    let split_path = filename_str.split("_").collect::<Vec<_>>();
                    let block_number = split_path[0].parse::<u64>().unwrap();
                    block_map.insert(block_number, true);
                }
            }
        }

        let mut blocks_with_data = block_map.into_keys().collect::<Vec<_>>();
        blocks_with_data.sort();

        blocks_with_data = blocks_with_data
            .into_iter()
            .filter(|block| block >= &start_block)
            .collect::<Vec<_>>();

        //let mut blocks_with_data = std::fs::read_to_string("./blocks_with_data.json").ok();

        let txn = db.begin().await.unwrap();
        bootstrap(&txn).await.unwrap();
        txn.commit().await.unwrap();

        //while start_block < stop_block {
        for block_with_data in blocks_with_data.into_iter() {
            let prefix = format!("{}_", block_with_data);
            // Filter out the files that start with the desired prefix.
            let mut matching_files: Vec<_> = entries
                .iter()
                .filter(|entry| {
                    if let Some(filename) = entry.path().file_name() {
                        if let Some(filename_str) = filename.to_str() {
                            return filename_str.starts_with(&prefix);
                        }
                    }
                    false
                })
                .map(|entry| entry.path())
                .collect();

            println!(
                "\n\n\n\n\nFound {} matching files for block {}\n\n\n\n\n\n",
                matching_files.len(),
                block_with_data
            );

            // we need to sort the matching files by their index (the number after the prefix)
            // all of the files are of the form: {block_number}_{index}_{space}_{author}.json
            matching_files.sort_by(|a, b| {
                let a = a.file_name().unwrap().to_str().unwrap();
                let b = b.file_name().unwrap().to_str().unwrap();

                let a = a.split("_").collect::<Vec<_>>();
                let b = b.split("_").collect::<Vec<_>>();

                a[1].parse::<usize>()
                    .unwrap()
                    .cmp(&b[1].parse::<usize>().unwrap())
            });

            let actions: Vec<Action> = matching_files
                .into_iter()
                .map(|path| {
                    let split_path = path.to_str().unwrap().split("_").collect::<Vec<_>>();
                    let space = split_path[3].to_string();
                    let author = split_path[4].trim_end_matches(".json").to_string();

                    let action = &std::fs::read_to_string(&path).unwrap();
                    let mut action = serde_json::from_slice::<Action>(action.as_bytes())
                        .unwrap_or_else(|err| {
                            panic!(
                                "Error deserializing action: {:?}, {}",
                                err,
                                path.to_str().unwrap()
                            )
                        });
                    action.space = space.clone();
                    action.author = author.clone();

                    action.actions.iter_mut().for_each(|triple| {
                        update_space_and_author(triple, space.clone(), author.clone());
                    });
                    action
                })
                .collect();

            for action in actions.into_iter() {
                println!("Processing action");
                handle_action(action, db.clone(), false, max_connections as usize)
                    .await
                    .unwrap();
                println!("Done w action");
            }
        }

        //std::fs::write(
        //"./blocks_with_data.json",
        //serde_json::to_string(&blocks_with_data).unwrap()).unwrap();
    }
}
