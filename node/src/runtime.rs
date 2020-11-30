use p2panda_core::{Configuration, TaskManager};
use p2panda_rpc::{ApiService, RpcServer};

/// Main runtime managing the p2panda node process.
pub struct Runtime {
    task_manager: TaskManager,
    rpc_server: RpcServer,
}

impl Runtime {
    /// Start p2panda node with your configuration. This method can be used to run the node within
    /// other applications.
    pub fn start(config: Configuration) -> Self {
        let mut task_manager = TaskManager::new();

        // Start JSON RPC API servers
        let io_handler = ApiService::io_handler();
        let rpc_server = RpcServer::start(&config, &mut task_manager, io_handler);

        Self {
            task_manager,
            rpc_server,
        }
    }

    /// Close all running concurrent tasks and wait until they are fully shut down.
    pub async fn shutdown(self) {
        self.rpc_server.shutdown();
        self.task_manager.shutdown().await;
    }
}
