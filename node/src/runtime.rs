use p2panda_core::{Configuration, TaskManager};
use p2panda_rpc::{build_rpc_handler, RpcServer};

pub struct Runtime {
    task_manager: TaskManager,
    rpc_server: RpcServer,
}

impl Runtime {
    pub fn start(config: Configuration) -> Self {
        let mut task_manager = TaskManager::new();
        let io_handler = build_rpc_handler();
        let rpc_server = RpcServer::start(&config, &mut task_manager, io_handler);

        Self {
            task_manager,
            rpc_server,
        }
    }

    pub async fn shutdown(self) {
        self.rpc_server.shutdown();
        self.task_manager.shutdown().await;
    }
}
