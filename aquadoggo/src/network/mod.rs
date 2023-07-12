// SPDX-License-Identifier: AGPL-3.0-or-later

mod behaviour;
mod config;
pub mod identity;
mod peers;
mod dialer;
mod service;
mod shutdown;
mod swarm;
mod transport;

pub use config::NetworkConfiguration;
pub use peers::Peer;
pub use service::network_service;
pub use shutdown::ShutdownHandler;
