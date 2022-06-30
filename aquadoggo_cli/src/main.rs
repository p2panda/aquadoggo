// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::{TryFrom, TryInto};
use std::error::Error;

use anyhow::Result;
use aquadoggo::{Configuration, Node, ReplicationConfiguration};
use structopt::StructOpt;

/// Helper method to parse a single key-value pair.
fn parse_key_val<T, U>(s: &str) -> Result<(T, Vec<U>), Box<dyn Error>>
where
    T: std::str::FromStr,
    T::Err: Error + 'static,
    U: std::str::FromStr,
    U::Err: Error + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{}`", s))?;

    let key = s[..pos].parse()?;
    let values = s[pos + 1..]
        .split(' ')
        .map(|elem| elem.parse())
        .collect::<Result<Vec<_>, _>>()?;

    Ok((key, values))
}

#[derive(StructOpt, Debug)]
#[structopt(name = "aquadoggo Node", about = "Node server for the p2panda network")]
struct Opt {
    /// Path to data folder, $HOME/.local/share/aquadoggo by default on Linux.
    #[structopt(short, long, parse(from_os_str))]
    data_dir: Option<std::path::PathBuf>,

    /// URLs of remote nodes to replicate with.
    #[structopt(short, long)]
    remote_node_addresses: Vec<String>,

    /// A collection of authors and their logs to replicate.
    ///
    /// eg. -A 123abc="1 2 345" -A 456def="6 7"
    /// .. adds the authors:
    /// - "123abc" with log_ids 1, 2, 345
    /// - "456def" with log_ids 6 7
    #[structopt(short = "A", parse(try_from_str = parse_key_val), number_of_values = 1)]
    authors_to_replicate: Vec<(String, Vec<u64>)>,
}

impl TryFrom<Opt> for Configuration {
    type Error = anyhow::Error;

    fn try_from(opt: Opt) -> Result<Self, Self::Error> {
        let mut config = Configuration::new(opt.data_dir)?;

        let authors_to_replicate = opt
            .authors_to_replicate
            .into_iter()
            .map(|elem| elem.try_into())
            .collect::<Result<_>>()?;

        config.replication = ReplicationConfiguration {
            remote_peers: opt.remote_node_addresses,
            authors_to_replicate,
            ..ReplicationConfiguration::default()
        };

        Ok(config)
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    // Parse command line arguments and load configuration
    let opt = Opt::from_args();
    let config = opt.try_into().expect("Could not load configuration");

    // Start p2panda node in async runtime
    let node = Node::start(config).await;

    // Run this until [CTRL] + [C] got pressed or something went wrong
    tokio::select! {
        _ = tokio::signal::ctrl_c() => (),
        _ = node.on_exit() => (),
    }

    // Wait until all tasks are gracefully shut down and exit
    node.shutdown().await;
}
