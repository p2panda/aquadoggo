// SPDX-License-Identifier: AGPL-3.0-or-later

//! Task queue for executing work in the background. Tasks get queued up and eventually get
//! processed in worker pools where one worker executes the task.
//!
//! A task queue allows control over a) order of operations and b) amount of work being done per
//! time c) avoiding duplicate work.
//!
//! A worker can be defined by any sort of async function which returns a result, indicating if it
//! succeeded, failed or crashed critically.
//!
//! Tasks are smaller work units which hold individual input values used as function arguments for
//! the worker. Every dispatched task is moved into a queue (FIFO) where it waits until it gets
//! processed in a worker pool.
//!
//! Tasks can also dispatch subsequent tasks as soon as they finished successfully.
//!
//! The `Factory` struct is the main interface in this module, managing all workers and tasks. It
//! registers worker pools with the regarding worker functions, adds new task to queues, schedules
//! and processes them.
//!
//! This is a simplified overview of how this task queue functions:
//!
//! ```text
//! 1. Register worker pool "square" with two workers
//!
//! --------------------------------------
//! - Name: "square"                     -
//! - Function: (input) => input * input -
//! - Pool Size: 2                       -
//! --------------------------------------
//!
//! This will result in a worker pool named "square", consisting of two workers we call now "a" and
//! "b". The worker function takes an integer to return the square function of it.
//!
//! As soon as we registered the worker pool once, we're ready to go!
//!
//! 2. Queue new tasks
//!
//! --------------------
//! - Id: Task 1       -
//! - Input: 5         -
//! - Worker: "square" -
//! --------------------
//!
//! --------------------
//! - Id: Task 2       -
//! - Input: 8         -
//! - Worker: "square" -
//! --------------------
//!
//! --------------------
//! - Id: Task 3       -
//! - Input: 5         -
//! - Worker: "square" -
//! --------------------
//!
//! --------------------
//! - Id: Task 4       -
//! - Input: 3         -
//! - Worker: "square" -
//! --------------------
//!
//! The internal queue of "square" contains now: [{Task 1}, {Task 2}, {Task 3}, {Task 4}]
//!
//! 3. Process tasks
//!
//! Worker "a" takes Task 1, worker "b" takes Task 2 from the queue. They both get processed
//! concurrently. After one of them finishes, the next free worker will eventually take Task 4 from
//! the queue and process it.
//!
//! Task 1 results in "25", Task 2 in "64", Task 3 in "25", Task 4 in "9".
//! ```
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use deadqueue::unlimited::Queue;
use log::{debug, error, info};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::task;
use triggered::{Listener, Trigger};

/// A task holding a generic input value and the name of the worker which will process it
/// eventually.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Task<IN>(WorkerName, IN);

impl<IN> Task<IN> {
    /// Returns a new task.
    pub fn new(worker_name: &str, input: IN) -> Self {
        Self(worker_name.into(), input)
    }

    /// Returns worker name of task;
    pub fn worker_name(&self) -> &WorkerName {
        &self.0
    }

    /// Returns task input;
    pub fn input(&self) -> &IN {
        &self.1
    }
}

/// Return value of every processed task indicating if it succeeded or failed.
///
/// When a task succeeds it has the option to dispatch subsequent tasks.
pub type TaskResult<IN> = Result<Option<Vec<Task<IN>>>, TaskError>;

/// Possible return values of a failed task.
#[derive(Debug)]
pub enum TaskError {
    /// This tasks failed critically and will cause the whole program to panic.
    Critical(String),

    /// This task failed silently without any further effects.
    Failure(String),
}

/// Enum representing status of a task.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TaskStatus<IN> {
    /// Task just got scheduled and waiting to be processed.
    Pending(Task<IN>),

    /// Task completed successfully.
    Completed(Task<IN>),
}

/// Workers are identified by simple string values.
pub type WorkerName = String;

/// Every registered worker pool is managed by a `WorkerManager` which holds the task queue for
/// this registered work and an index of all current inputs in the task queue.
struct WorkerManager<IN>
where
    IN: Send + Sync + Clone + Hash + Eq + Display + 'static,
{
    /// Index of all current inputs inside the task queue organized in a hash map.
    ///
    /// This allows us to keep track of the number of tasks working on the same problem. Similar to
    /// an atomic reference counter dropping at 0, we can safely inform other layers about when we
    /// are "done" with working on the problem.
    input_index: Arc<Mutex<HashMap<IN, AtomicU64>>>,

    /// FIFO queue of all tasks for this worker pool.
    queue: Arc<Queue<QueueItem<IN>>>,
}

impl<IN> WorkerManager<IN>
where
    IN: Send + Sync + Clone + Hash + Eq + Display + 'static,
{
    /// Returns a new worker manager.
    pub fn new() -> Self {
        Self {
            input_index: Arc::new(Mutex::new(HashMap::new())),
            queue: Arc::new(Queue::new()),
        }
    }
}

/// This trait defines a generic async worker function receiving the task input and shared context
/// and returning a task result
///
/// It is also using the `async_trait` macro as a trick to avoid a more ugly trait signature as
/// working with generic, static, pinned and boxed async functions can look quite messy.
#[async_trait::async_trait]
pub trait Workable<IN, D>
where
    IN: Send + Sync + Clone + 'static,
    D: Send + Sync + 'static,
{
    async fn call(&self, context: D, input: IN) -> TaskResult<IN>;
}

/// Implements our `Workable` trait for a generic async function.
#[async_trait::async_trait]
impl<FN, F, IN, D> Workable<IN, D> for FN
where
    // Function accepting a context and generic input value, returning a future.
    FN: Fn(D, IN) -> F + Sync,
    // Future returning a `TaskResult`.
    F: Future<Output = TaskResult<IN>> + Send + 'static,
    // Generic input type.
    IN: Send + Sync + Clone + 'static,
    // Generic context type.
    D: Send + Sync + 'static,
{
    /// Internal method which calls our generic async function, passing in the context and input
    /// value.
    ///
    /// This gets automatically wrapped in a static, boxed and pinned function signature by the
    /// `async_trait` macro so we don't need to do it ourselves.
    async fn call(&self, context: D, input: IN) -> TaskResult<IN> {
        (self)(context, input).await
    }
}

/// Every queue consists of items which hold an unique identifier and the task input value.
#[derive(Debug)]
pub struct QueueItem<IN>
where
    IN: Send + Sync + Clone + Display + 'static,
{
    /// Unique task identifier.
    id: u64,

    /// Task input values which get passed over to the worker function.
    input: IN,
}

impl<IN> Display for QueueItem<IN>
where
    IN: Send + Sync + Clone + Display + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<QueueItem {} w. {}>", self.id, self.input)
    }
}

impl<IN> QueueItem<IN>
where
    IN: Send + Sync + Clone + Display + 'static,
{
    /// Returns a new queue item.
    pub fn new(id: u64, input: IN) -> Self {
        Self { id, input }
    }

    /// Returns unique identifier of this queue item.
    #[allow(dead_code)]
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Returns generic input values of this queue item.
    pub fn input(&self) -> IN {
        self.input.clone()
    }
}

/// This factory serves as a main entry interface to dispatch, schedule and process tasks.
pub struct Factory<IN, D>
where
    IN: Send + Sync + Clone + Hash + Eq + Debug + Display + 'static,
    D: Send + Sync + Clone + 'static,
{
    /// Shared context between all tasks.
    context: D,

    /// Map of all registered worker pools.
    managers: HashMap<WorkerName, WorkerManager<IN>>,

    /// Broadcast channel to inform worker pools about new tasks.
    tx: Sender<Task<IN>>,

    /// Broadcast channel to inform callbacks about pending or completed tasks.
    tx_status: Sender<TaskStatus<IN>>,

    /// Sender of error signal.
    error_signal: Trigger,

    /// Receiver of error signal.
    ///
    /// This can be used to react to factory errors, for example by quitting the program.
    error_handle: Listener,
}

impl<IN, D> Factory<IN, D>
where
    IN: Send + Sync + Clone + Hash + Eq + Debug + Display + 'static,
    D: Send + Sync + Clone + 'static,
{
    /// Initialises a new factory.
    ///
    /// The capacity argument defines the maximum bound of incoming new tasks which get broadcasted
    /// across all worker pools which accordingly will pick up the task. Use a higher value if your
    /// factory expects a large amount of tasks within short time.
    ///
    /// Factories will panic if the capacity limit was reached as it will cause the workers to miss
    /// incoming tasks.
    pub fn new(context: D, capacity: usize) -> Self {
        let (tx, _) = channel(capacity);
        let (tx_status, _) = channel(capacity);
        let (error_signal, error_handle) = triggered::trigger();

        Self {
            context,
            managers: HashMap::new(),
            tx,
            tx_status,
            error_signal,
            error_handle,
        }
    }

    /// Registers a new worker pool with a dedicated worker function.
    ///
    /// Choose a worker pool size fitting the work and computational resources you have at hand to
    /// conduct it.
    ///
    /// As soon as a worker pool got registered it is ready to receive incoming tasks which get
    /// queued up and eventually processed by the regarding worker function.
    ///
    /// Ideally worker functions should be idempotent: meaning the function won’t cause unintended
    /// effects even if called multiple times with the same arguments.
    pub fn register<W: Workable<IN, D> + Send + Sync + Copy + 'static>(
        &mut self,
        name: &str,
        pool_size: usize,
        work: W,
    ) {
        if self.managers.contains_key(name) {
            panic!("Can not create task manager twice");
        } else {
            let new_manager = WorkerManager::new();
            self.managers.insert(name.into(), new_manager);
        }

        info!("Register {} worker with pool size {}", name, pool_size);

        self.spawn_dispatcher(name);
        self.spawn_workers(name, pool_size, work);
    }

    /// Queues up a new task in the regarding worker queue.
    ///
    /// Tasks with duplicate input values which already exist in the queue will be silently
    /// rejected.
    pub fn queue(&mut self, task: Task<IN>) {
        if let Err(err) = self.tx.send(task) {
            error!("Error while broadcasting task: {}", err);
            self.error_signal.trigger();
        }
    }

    /// Returns true if there are no more tasks given for this worker pool.
    #[allow(dead_code)]
    pub fn is_empty(&self, name: &str) -> bool {
        match self.managers.get(name) {
            Some(manager) => manager.queue.is_empty(),
            None => false,
        }
    }

    /// Future which resolves as soon as factory returned a critical error.
    pub fn on_error(&self) -> Listener {
        self.error_handle.clone()
    }

    /// Subscribe to status changes of tasks.
    pub fn on_task_status_change(&self) -> Receiver<TaskStatus<IN>> {
        self.tx_status.subscribe()
    }

    /// Spawns a task which listens to broadcast channel for incoming new tasks which might be
    /// added to the worker queue.
    fn spawn_dispatcher(&self, name: &str) {
        // At this point we should already have a worker pool with this name
        let manager = self.managers.get(name).expect("Unknown worker name");

        // Subscribe to the broadcast channel
        let mut rx = self.tx.subscribe();

        // Create handle to send task status updates
        let tx_status = self.tx_status.clone();

        // Initialise a new counter to provide unique task ids
        let counter = AtomicU64::new(0);

        // Increment references to move worker data safely into the async task
        let input_index = manager.input_index.clone();
        let name = String::from(name);
        let queue = manager.queue.clone();

        // Create handle for error signal
        let error_signal = self.error_signal.clone();

        task::spawn(async move {
            // Inform status subscribers that we've just scheduled a new task
            let on_pending = |task: Task<IN>| {
                if tx_status.send(TaskStatus::Pending(task)).is_err() {
                    // Silently fail here since an error only occurs here when there are no
                    // subscribers, but we don't mind that.
                }
            };

            loop {
                match rx.recv().await {
                    // A new task got announced in the broadcast channel!
                    Ok(task) => {
                        if task.worker_name() != &name {
                            continue; // This is not for us ..
                        }

                        // Check if a task with the same input values already exists in queue
                        match input_index.lock() {
                            Ok(mut index) => {
                                // Check if we haven't taken note yet of that task yet. Through the
                                // index we're detecting duplicates, making sure that we only keep
                                // track of one
                                let index_value = index.get(&task.1);
                                if index_value.is_none() {
                                    // Trigger status update
                                    on_pending(task.clone());
                                }

                                // Generate a unique id for this new task and add it to queue
                                let next_id = counter.fetch_add(1, Ordering::Relaxed);
                                queue.push(QueueItem::new(next_id, task.1.clone()));

                                // Keep count of how many tasks are duplicates
                                match index_value {
                                    None => {
                                        index.insert(task.1, AtomicU64::new(1));
                                    }
                                    Some(task_count) => {
                                        task_count.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                            Err(err) => {
                                error!("Error while locking input index: {}", err);
                                error_signal.trigger();
                            }
                        }
                    }
                    // The capacity of the broadcast channel is full, we're lagging behind and miss
                    // out on incoming tasks
                    Err(RecvError::Lagged(skipped_messages)) => {
                        error!("Channel lagging behind {} messages", skipped_messages);
                        error_signal.trigger();
                    }
                    // The channel got closed, nothing anymore to do here
                    Err(RecvError::Closed) => (),
                }
            }
        });
    }

    /// Spawns a worker pool of given size with a unique name and worker function.
    ///
    /// Every worker waits for a task inside the queue and processes its input values accordingly
    /// with the given worker function.
    fn spawn_workers<W: Workable<IN, D> + Send + Sync + Copy + 'static>(
        &self,
        name: &str,
        pool_size: usize,
        work: W,
    ) {
        // At this point we should already have a worker pool with this name
        let manager = self.managers.get(name).expect("Unknown worker name");

        // Spawn task for each worker inside the pool
        for _ in 0..pool_size {
            let context = self.context.clone();
            let queue = manager.queue.clone();
            let input_index = manager.input_index.clone();
            let tx = self.tx.clone();
            let name = name.to_string();

            // Create handle for error signal
            let error_signal = self.error_signal.clone();

            // Create handle to send task status updates
            let tx_status = self.tx_status.clone();

            task::spawn(async move {
                // Inform status subscribers that we just completed a task
                let on_complete = |input: IN| {
                    let status = TaskStatus::Completed(Task::new(&name, input));
                    if tx_status.send(status).is_err() {
                        // Silently fail here since an error only occurs here when there are no
                        // subscribers, but we don't mind that.
                    }
                };

                loop {
                    // Wait until there is a new task arriving in the queue
                    let item = queue.pop().await;

                    // Take this task and do work ..
                    let result = work.call(context.clone(), item.input()).await;

                    // Decrease task counter by one. If the counter hits zero we can safely remove
                    // the index.
                    //
                    // This helps us to keep track of if there are still running tasks around
                    // working on the same problem.
                    match input_index.lock() {
                        Ok(mut index) => {
                            let index_value = index.get(&item.input);

                            match index_value {
                                Some(task_count) => {
                                    task_count.fetch_sub(1, Ordering::Relaxed);

                                    if task_count.load(Ordering::Relaxed) == 0 {
                                        index.remove(&item.input);

                                        // Trigger removing the task from the task store
                                        on_complete(item.input());
                                    }
                                }
                                None => (),
                            }
                        }
                        Err(err) => {
                            error!(
                                "Error while locking input index in worker {} for task {:?}: {}",
                                name, item, err
                            );

                            error_signal.trigger();
                        }
                    }

                    // Check the result
                    match result {
                        Ok(Some(list)) => {
                            // Tasks succeeded and dispatches new, subsequent tasks
                            for task in list {
                                if let Err(err) = tx.send(task) {
                                    error!("Error while broadcasting task: {}", err);
                                    error_signal.trigger();
                                }
                            }
                        }
                        Err(TaskError::Critical(err)) => {
                            // Something really horrible happened, we need to crash!
                            error!(
                                "Critical error in worker {} with task {}: {}",
                                name, item, err
                            );

                            error_signal.trigger();
                        }
                        Err(TaskError::Failure(err)) => {
                            debug!(
                                "Silently failing worker {} with task {}: {}",
                                name, item, err
                            );
                        }
                        _ => (), // Task succeeded, but nothing to dispatch
                    }
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fmt::Display;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use rand::seq::SliceRandom;
    use rand::Rng;

    use super::{Factory, Task, TaskError, TaskResult, TaskStatus};

    #[tokio::test]
    async fn factory() {
        type Input = usize;
        type Data = Arc<Mutex<Vec<String>>>;

        // Test database which stores a list of strings
        let database = Arc::new(Mutex::new(Vec::new()));

        // Initialise factory
        let mut factory = Factory::<Input, Data>::new(database.clone(), 1024);

        // Define two workers
        async fn first(database: Data, input: Input) -> TaskResult<Input> {
            let mut db = database
                .lock()
                .map_err(|err| TaskError::Critical(err.to_string()))?;
            db.push(format!("first-{}", input));
            Ok(None)
        }

        // .. the second worker dispatches a task for "first" at the end
        async fn second(database: Data, input: Input) -> TaskResult<Input> {
            let mut db = database
                .lock()
                .map_err(|err| TaskError::Critical(err.to_string()))?;
            db.push(format!("second-{}", input));
            Ok(Some(vec![Task::new("first", input)]))
        }

        // Register both workers
        factory.register("first", 2, first);
        factory.register("second", 2, second);

        // Queue a couple of tasks
        for i in 0..4 {
            factory.queue(Task::new("second", i));
        }

        // Wait until work was done ..
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(database.lock().unwrap().len(), 8);
        assert!(factory.is_empty("first"));
        assert!(factory.is_empty("second"));
    }

    #[tokio::test]
    async fn on_task_status_change_subscription() {
        type Input = usize;
        type Data = usize;

        // Initialise factory
        let mut factory = Factory::<Input, Data>::new(1, 1024);

        // Record all status changes in this array
        let messages: Arc<Mutex<Vec<TaskStatus<Input>>>> = Arc::new(Mutex::new(Vec::new()));

        // Subscribe to updates and record them
        let mut on_task_status_change = factory.on_task_status_change();
        let messages_clone = messages.clone();
        tokio::task::spawn(async move {
            loop {
                let message = on_task_status_change.recv().await.unwrap();
                messages_clone.lock().unwrap().push(message);
            }
        });

        // Define workers and register them
        factory.register("one", 1, |_, input: Input| async move {
            Ok(Some(vec![Task::new("two", input)]))
        });
        factory.register("two", 1, |_, _| async { Ok(None) });

        // Queue a couple of tasks
        for i in 0..3 {
            factory.queue(Task::new("one", i));
        }

        // Wait until work was done ..
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(factory.is_empty("one"));

        // We expect a total of 12 recorded status messages:
        // - 3x "one" and 3x "two" tasks have been scheduled
        // - 3x "one" and 3x "two" tasks have been completed
        assert_eq!(messages.lock().unwrap().len(), 12);
    }

    #[tokio::test]
    async fn jigsaw() {
        // This test solves multiple jigsaw puzzles with our task queue implementation.
        //
        // The idea here is that we have a random, mixed "box" of puzzle pieces of multiple
        // jigsaws. We pick one puzzle piece at a time, deciding each time if we can connect this
        // piece to other fitting pieces we already know about. If not, we put the piece "aside"
        // and look at it later.
        //
        // We repeat these steps until the box is empty, eventually we will end up with a couple of
        // solved jigsaw puzzles!

        // This is the puzzle piece with an unique id and a list of other pieces which fit to this
        // one, identified by their id.
        #[derive(Hash, PartialEq, Eq, Clone, Debug)]
        struct JigsawPiece {
            id: usize,
            relations: Vec<usize>,
        }

        impl Display for JigsawPiece {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{} {:?}", self.id, self.relations)
            }
        }

        // This is a whole puzzle, which is simply a list of puzzle pieces. It has a "complete"
        // flag, which turns true as soon as we finished the puzzle!
        #[derive(Hash, Clone, Debug)]
        struct JigsawPuzzle {
            id: usize,
            piece_ids: Vec<usize>,
            complete: bool,
        }

        // Our "database" containing all pieces we've collected and puzzles we've completed
        struct Jigsaw {
            pieces: HashMap<usize, JigsawPiece>,
            puzzles: HashMap<usize, JigsawPuzzle>,
        }

        type Data = Arc<Mutex<Jigsaw>>;

        let database = Arc::new(Mutex::new(Jigsaw {
            pieces: HashMap::new(),
            puzzles: HashMap::new(),
        }));

        let mut factory = Factory::<JigsawPiece, Data>::new(database.clone(), 1024);

        // This tasks "picks" a single piece out of the box and sorts it into the database
        async fn pick(database: Data, input: JigsawPiece) -> TaskResult<JigsawPiece> {
            let mut db = database
                .lock()
                .map_err(|err| TaskError::Critical(err.to_string()))?;

            // 1. Take incoming puzzle piece from box and move it into the database first
            db.pieces.insert(input.id, input.clone());

            // 2. For every existing related other puzzle piece, dispatch a find task
            let tasks: Vec<Task<JigsawPiece>> = input
                .relations
                .iter()
                .filter_map(|id| {
                    db.pieces
                        .get(id)
                        .map(|piece| Task::new("find", piece.clone()))
                })
                .collect();

            Ok(Some(tasks))
        }

        // This task finds fitting pieces and tries to combine them to a puzzle
        async fn find(database: Data, input: JigsawPiece) -> TaskResult<JigsawPiece> {
            let mut db = database
                .lock()
                .map_err(|err| TaskError::Critical(err.to_string()))?;

            // 1. Merge all known and related pieces into one large list
            let mut ids: Vec<usize> = Vec::new();
            let mut candidates: Vec<usize> = input.relations.clone();

            loop {
                // Iterate over all relations until there is none
                if candidates.is_empty() {
                    break;
                }

                // Add another piece to list of ids. Unwrap as we know the list is not empty.
                let id = candidates.pop().unwrap();
                ids.push(id);

                // Get all related pieces of this piece
                match db.pieces.get(&id) {
                    Some(piece) => {
                        for relation_id in &piece.relations {
                            // Check if we have already visited all relations of this piece,
                            // otherwise add them to list
                            if !ids.contains(relation_id) && !candidates.contains(relation_id) {
                                candidates.push(*relation_id);
                            }
                        }
                    }
                    None => continue,
                };
            }

            // The future puzzle which will contain this list of pieces. We still need to find out
            // which puzzle exactly it will be ..
            let mut puzzle_id: Option<usize> = None;

            for (_, puzzle) in db.puzzles.iter_mut() {
                // 2. Find out if we already have a piece belonging to a puzzle and just take any
                //    of them as the future puzzle!
                if puzzle_id.is_none() {
                    for id in &ids {
                        if puzzle.piece_ids.contains(id) {
                            puzzle_id = Some(puzzle.id);
                        }
                    }
                }

                // 3. Remove all these pieces from all puzzles first as we don't know if we
                //    accidentially sorted them into separate puzzles even though they belong
                //    together at one point.
                puzzle.piece_ids.retain(|&id| !ids.contains(&id));
            }

            // 4. Finally move all pieces into one puzzle
            match puzzle_id {
                None => {
                    // If there is no puzzle yet, create a new one
                    let id = match db.puzzles.keys().max() {
                        None => 1,
                        Some(id) => id + 1,
                    };

                    db.puzzles.insert(
                        id,
                        JigsawPuzzle {
                            id,
                            piece_ids: ids.to_vec(),
                            complete: false,
                        },
                    );
                }
                Some(id) => {
                    // Add all pieces to existing puzzle. Unwrap as we know that item exists.
                    let puzzle = db.puzzles.get_mut(&id).unwrap();
                    puzzle.piece_ids.extend_from_slice(&ids);
                }
            };

            Ok(Some(vec![Task::new("finish", input)]))
        }

        // This task checks if a puzzle was completed
        async fn finish(database: Data, input: JigsawPiece) -> TaskResult<JigsawPiece> {
            let mut db = database
                .lock()
                .map_err(|err| TaskError::Critical(err.to_string()))?;

            // 1. Identify unfinished puzzle related to this piece
            let puzzle: Option<JigsawPuzzle> = db
                .puzzles
                .values()
                .find(|item| item.piece_ids.contains(&input.id) && !item.complete)
                .cloned();

            // 2. Check if all piece dependencies are met
            match puzzle {
                None => Err(TaskError::Failure("No puzzle given".into())),
                Some(mut puzzle) => {
                    for piece_id in &puzzle.piece_ids {
                        match db.pieces.get(piece_id) {
                            None => {
                                return Err(TaskError::Failure("Dependencies are not met".into()))
                            }
                            Some(piece) => {
                                for relation_piece_id in &piece.relations {
                                    if !puzzle.piece_ids.contains(relation_piece_id) {
                                        return Err(TaskError::Failure(
                                            "Dependencies are not met".into(),
                                        ));
                                    }
                                }
                            }
                        };
                    }

                    // Mark puzzle as complete! We are done here!
                    puzzle.complete = true;
                    db.puzzles.insert(puzzle.id, puzzle);
                    Ok(None)
                }
            }
        }

        // Register workers
        factory.register("pick", 3, pick);
        factory.register("find", 3, find);
        factory.register("finish", 3, finish);

        // Generate a number of puzzles to solve
        let puzzles_count = 3;
        let min_size = 3;
        let max_size = 10;

        let mut pieces: Vec<JigsawPiece> = Vec::new();
        let mut offset: isize = 0;

        for _ in 0..puzzles_count {
            // Every puzzle has a random, square dimension of x * x pieces
            let size = rand::thread_rng().gen_range(min_size..max_size);

            // Every piece is identified by an unique number
            let mut id: isize = 0;

            // Create all pieces for this square puzzle and connect neighboring pieces, so that an
            // puzzle with the size of 3 * 3 would look like that:
            //
            // [1] [2] [3]
            // [4] [5] [6]
            // [7] [8] [9]
            //
            // Piece 1 would be connected to 2 and 4, Piece 2 would be connected to 1, 3 and 5 and
            // so on .., the relations for all pieces would become:
            //
            // 1: 2, 4
            // 2: 1, 3, 5
            // 3: 2, 6
            // 4: 1, 5, 6
            // 5: 2, 4, 6, 8
            // 6: 3, 5, 9
            // 7: 4, 8
            // 8: 5, 7, 9
            // 9: 6, 8
            for _ in 0..size {
                for _ in 0..size {
                    let mut relations: Vec<usize> = Vec::new();

                    id += 1;

                    if id % size != 0 {
                        // Add related piece to the right
                        relations.push((offset + id + 1) as usize);
                    }

                    if id % size != 1 {
                        // Add related piece to the left
                        relations.push((offset + id - 1) as usize);
                    }

                    if id + size <= size * size {
                        // Add related piece to the bottom
                        relations.push((offset + id + size) as usize);
                    }

                    if id - size > 0 {
                        // Add related piece to the top
                        relations.push((offset + id - size) as usize);
                    }

                    pieces.push(JigsawPiece {
                        id: (offset + id) as usize,
                        relations,
                    });
                }
            }

            offset += size * size;
        }

        // Mix all puzzle pieces to a large chaotic pile
        let mut rng = rand::thread_rng();
        pieces.shuffle(&mut rng);

        for piece in pieces {
            factory.queue(Task::new("pick", piece));

            // Add a little bit of a random delay between dispatching tasks
            let random_delay = rand::thread_rng().gen_range(1..5);
            tokio::time::sleep(Duration::from_millis(random_delay)).await;
        }

        // Check if all puzzles have been solved correctly

        assert_eq!(
            database
                .lock()
                .unwrap()
                .puzzles
                .values()
                .filter(|puzzle| puzzle.complete)
                .cloned()
                .count(),
            puzzles_count
        );
    }
}
