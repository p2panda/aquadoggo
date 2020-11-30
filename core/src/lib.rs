#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;

mod config;
mod task;

pub use crate::config::Configuration;
pub use task::TaskManager;
