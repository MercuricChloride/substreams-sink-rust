use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[clap(subcommand)]
    /// Substreams command
    pub command: Commands,
    /// Substreams endpoint
    #[arg(short, long, env = "SUBSTREAMS_ENDPOINT")]
    pub substreams_endpoint: String,

    /// Path or link to spkg
    #[arg(short, long, default_value = "substream.spkg")]
    pub spkg: String,

    /// Module name
    #[arg(short, long, default_value = "map_entries_added")]
    pub module: String,

    /// Postgres host
    #[arg(short, long, env = "POSTGRES_HOST")]
    pub host: String,

    /// Postgres username
    #[arg(short, long, env = "POSTGRES_USER")]
    pub username: String,

    /// Postgres password
    #[arg(short, long, env = "POSTGRES_PASSWORD")]
    pub password: String,

    /// Postgres database
    #[arg(short, long, env = "POSTGRES_DATABASE")]
    pub database: String,

    /// Postgres port, default 5432
    //#[arg(short, long, default_value = "5432")]
    //pub port: String,

    /// Substreams API token, if not provided, SUBSTREAMS_API_TOKEN environment variable will be used
    #[arg(short, long, env = "SUBSTREAMS_API_TOKEN")]
    pub token: String,

    /// Whether or not to use the GUI
    #[arg(short, long)]
    pub gui: bool,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Deploy {
        #[arg(short, long)]
        #[clap(index = 1)]
        /// Space addresses to listen to. Note this will index all subspaces of the provided spaces.
        spaces: Vec<String>,
    },
    DeployGlobal {
        #[arg(
            short,
            long,
            default_value = "0x170b749413328ac9a94762031a7a05b00c1d2e34"
        )]
        #[clap(index = 1)]
        /// The root space address
        root_space_address: String,
    },
}
