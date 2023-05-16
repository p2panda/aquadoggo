// SPDX-License-Identifier: AGPL-3.0-or-later

mod behaviour;
mod config;
mod identity;
mod service;
mod swarm;
mod transport;
// @TODO: Remove this as soon as we integrated it into the libp2p swarm
#[allow(dead_code)]
mod replication;

pub use config::NetworkConfiguration;
pub use service::network_service;
