// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam_queue::SegQueue;
use tokio::sync::broadcast::{channel, Sender};
use tokio::task;

#[derive(Debug, Clone)]
pub struct Task<IN>(WorkerName, IN);

impl<IN> Task<IN> {
    pub fn new(name: &str, input: IN) -> Self {
        Self(name.into(), input)
    }
}

pub type WorkerName = String;

pub enum TaskError {
    Critical,
    Failure,
}

pub type TaskResult<IN> = Result<Option<Vec<Task<IN>>>, TaskError>;

pub struct Context<D: Send + Sync + 'static>(Arc<D>);

impl<D: Send + Sync + 'static> Clone for Context<D> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

struct WorkerManager<IN>
where
    IN: Send + Sync + Clone + Hash + Eq + 'static,
{
    input_index: Arc<Mutex<HashSet<IN>>>,
    queue: Arc<SegQueue<QueueItem<IN>>>,
}

impl<IN> WorkerManager<IN>
where
    IN: Send + Sync + Clone + Hash + Eq + 'static,
{
    pub fn new() -> Self {
        Self {
            input_index: Arc::new(Mutex::new(HashSet::new())),
            queue: Arc::new(SegQueue::new()),
        }
    }
}

#[async_trait::async_trait]
pub trait Workable<IN, D>
where
    IN: Send + Sync + Clone + 'static,
    D: Send + Sync + 'static,
{
    async fn call(&self, context: Context<D>, input: IN) -> TaskResult<IN>;
}

#[async_trait::async_trait]
impl<FN, F, IN, D> Workable<IN, D> for FN
where
    FN: Fn(Context<D>, IN) -> F + Sync,
    F: Future<Output = TaskResult<IN>> + Send + 'static,
    IN: Send + Sync + Clone + 'static,
    D: Sync + Send + 'static,
{
    async fn call(&self, context: Context<D>, input: IN) -> TaskResult<IN> {
        (self)(context, input).await
    }
}

#[derive(Debug)]
pub struct QueueItem<IN>
where
    IN: Send + Sync + Clone + 'static,
{
    id: u64,
    input: IN,
}

impl<IN> QueueItem<IN>
where
    IN: Send + Sync + Clone + 'static,
{
    pub fn new(id: u64, input: IN) -> Self {
        Self { id, input }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn input(&self) -> IN {
        self.input.clone()
    }
}

pub struct Factory<IN, D>
where
    IN: Send + Sync + Clone + Hash + Eq + Debug + 'static,
    D: Send + Sync + 'static,
{
    context: Context<D>,
    managers: HashMap<WorkerName, WorkerManager<IN>>,
    tx: Sender<Task<IN>>,
}

impl<IN, D> Factory<IN, D>
where
    IN: Send + Sync + Clone + Hash + Eq + Debug + 'static,
    D: Send + Sync + 'static,
{
    pub fn new(data: D) -> Self {
        let (tx, _) = channel(1024);

        Self {
            context: Context(Arc::new(data)),
            managers: HashMap::new(),
            tx,
        }
    }

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

        self.spawn_dispatcher(name);
        self.spawn_workers(name, pool_size, work);
    }

    pub fn queue(&mut self, task: Task<IN>) {
        self.tx
            .send(task)
            .expect("Critical system error: Cant broadcast task");
    }

    pub fn is_empty(&self, name: &str) -> bool {
        match self.managers.get(name) {
            Some(manager) => manager.queue.is_empty(),
            None => false,
        }
    }

    fn spawn_dispatcher(&self, name: &str) {
        let manager = self.managers.get(name).expect("Unknown worker name");

        let input_index = manager.input_index.clone();
        let mut rx = self.tx.subscribe();
        let name = String::from(name);
        let queue = manager.queue.clone();
        let counter = AtomicU64::new(0);

        task::spawn(async move {
            while let Ok(task) = rx.recv().await {
                if task.0 != name {
                    continue; // This is not for us
                }

                let mut input_index = input_index.lock().unwrap();

                if input_index.contains(&task.1) {
                    continue; // Task already exists
                }

                let next_id = counter.fetch_add(1, Ordering::Relaxed);
                queue.push(QueueItem::new(next_id, task.1.clone()));
                input_index.insert(task.1);
            }
        });
    }

    fn spawn_workers<W: Workable<IN, D> + Send + Sync + Copy + 'static>(
        &self,
        name: &str,
        pool_size: usize,
        work: W,
    ) {
        let manager = self.managers.get(name).expect("Unknown worker name");

        for _ in 0..pool_size {
            let context = self.context.clone();
            let queue = manager.queue.clone();
            let input_index = manager.input_index.clone();
            let tx = self.tx.clone();
            let name = String::from(name);

            task::spawn(async move {
                loop {
                    match queue.pop() {
                        Some(item) => {
                            // Do work ..
                            let result = work.call(context.clone(), item.input()).await;
                            println!("Finished {} w. {:?}", name, item.input());

                            match result {
                                Ok(Some(list)) => {
                                    // Dispatch new tasks
                                    for task in list {
                                        tx.send(task)
                                            .expect("Critical system error: Cant broadcast task");
                                    }
                                }
                                Err(TaskError::Critical) => {
                                    // Something really horrible happened, we need to crash!
                                    //
                                    // @TODO: Does this only crash within the thread or the whole
                                    // program? We want the latter ..
                                    panic!("Critical system error: Task {:?} failed", item.id(),);
                                }
                                Err(TaskError::Failure) => {
                                    // Silently fail .. maybe write something to the log or retry?
                                }
                                _ => (), // Nothing to dispatch
                            }

                            // Remove input index from queue
                            let mut input_index = input_index.lock().unwrap();
                            input_index.remove(&item.input());
                        }
                        None => task::yield_now().await,
                    }
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use rand::seq::SliceRandom;
    use rand::Rng;

    use super::{Context, Factory, Task, TaskResult};

    #[tokio::test]
    async fn factory() {
        type Input = usize;
        type Data = Arc<Mutex<Vec<String>>>;

        // Test database which stores a list of strings
        let database = Arc::new(Mutex::new(Vec::new()));

        // Initialise factory
        let mut factory = Factory::<Input, Data>::new(database.clone());

        // Define two workers
        async fn first(database: Context<Data>, input: Input) -> TaskResult<Input> {
            let mut db = database.0.lock().unwrap();
            db.push(format!("first-{}", input));
            Ok(None)
        }

        // .. the second worker dispatches a task for "first" at the end
        async fn second(database: Context<Data>, input: Input) -> TaskResult<Input> {
            let mut db = database.0.lock().unwrap();
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
    async fn jigsaw() {
        #[derive(Hash, PartialEq, Eq, Clone, Debug)]
        struct JigsawPiece {
            id: usize,
            relations: Vec<usize>,
        }

        #[derive(Hash, Clone, Debug)]
        struct JigsawPuzzle {
            id: usize,
            piece_ids: Vec<usize>,
            complete: bool,
        }

        struct Jigsaw {
            pieces: HashMap<usize, JigsawPiece>,
            puzzles: HashMap<usize, JigsawPuzzle>,
        }

        type Data = Arc<Mutex<Jigsaw>>;

        let database = Arc::new(Mutex::new(Jigsaw {
            pieces: HashMap::new(),
            puzzles: HashMap::new(),
        }));

        let mut factory = Factory::<JigsawPiece, Data>::new(database.clone());

        async fn pick(database: Context<Data>, input: JigsawPiece) -> TaskResult<JigsawPiece> {
            let mut db = database.0.lock().unwrap();

            // 1. Take incoming puzzle piece from box and move it into the database first
            db.pieces.insert(input.id, input.clone());

            // 2. For every existing related other puzzle piece, dispatch a find task
            let tasks: Vec<Task<JigsawPiece>> = input
                .relations
                .iter()
                .filter_map(|id| match db.pieces.get(&id) {
                    Some(piece) => {
                        println!("found relation {:?}", id);
                        Some(Task::new("find", piece.clone()))
                    }
                    None => None,
                })
                .collect();

            Ok(Some(tasks))
        }

        async fn find(database: Context<Data>, input: JigsawPiece) -> TaskResult<JigsawPiece> {
            let mut db = database.0.lock().unwrap();

            // 1. Identify which related pieces we already have for this puzzle piece
            let ids: Vec<usize> = input
                .relations
                .iter()
                .filter_map(|id| match db.pieces.get(id) {
                    Some(_) => Some(id.clone()),
                    None => None,
                })
                .collect();

            println!("-- input {:?}", ids);

            // The puzzle which will contain these pieces
            let mut puzzle_id: Option<usize> = None;

            for (_, puzzle) in db.puzzles.iter_mut() {
                // 2. Find out if we already have a piece belonging to a puzzle and just take any
                //    of them as the future puzzle id
                if puzzle_id.is_none() {
                    for id in &ids {
                        if puzzle.piece_ids.contains(&id) {
                            puzzle_id = Some(puzzle.id);
                        }
                    }
                }

                // 3. Remove all these pieces from all puzzles first as we don't know if we
                //    accidentially sorted them into separate puzzles even though they belong
                //    together at one point
                println!("-- before {:?}", puzzle);
                puzzle.piece_ids.retain(|&id| !ids.contains(&id));
                println!("-- after {:?}", puzzle);
            }

            // 4. Finally move all pieces into one puzzle
            match puzzle_id {
                None => {
                    // If there is no puzzle yet, create a new one
                    let id = match db.puzzles.keys().last() {
                        None => 1,
                        Some(id) => id + 1,
                    };

                    println!("---- create new puzzle {:?}", id);
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
                    // Add to existing puzzle
                    let puzzle = db.puzzles.get_mut(&id).unwrap();
                    puzzle.piece_ids.extend_from_slice(&ids);
                    println!("---- add to puzzle {:?}", puzzle);
                }
            };

            if input.relations.len() == ids.len() {
                Ok(Some(vec![Task::new("finish", input)]))
            } else {
                Ok(None)
            }
        }

        async fn finish(database: Context<Data>, input: JigsawPiece) -> TaskResult<JigsawPiece> {
            let mut db = database.0.lock().unwrap();

            // 1. Identify unfinished puzzle related to this piece
            let puzzle: Option<JigsawPuzzle> = db
                .puzzles
                .values()
                .find(|item| item.piece_ids.contains(&input.id) && !item.complete)
                .map(|item| item.clone());

            // 2. Check if all piece dependencies are met
            match puzzle {
                None => {
                    println!("could not find puzzle");
                    Ok(None)
                }
                Some(mut puzzle) => {
                    println!("-------- {:?}", puzzle);

                    for piece_id in &puzzle.piece_ids {
                        match db.pieces.get(&piece_id) {
                            None => {
                                println!("could not find piece");
                                return Ok(None);
                            }
                            Some(piece) => {
                                for relation_piece_id in &piece.relations {
                                    if !puzzle.piece_ids.contains(&relation_piece_id) {
                                        println!("Missing relation");
                                        return Ok(None);
                                    }
                                }
                            }
                        };
                    }

                    // Mark puzzle as complete! We are done here!
                    puzzle.complete = true;
                    db.puzzles.insert(puzzle.id, puzzle.clone());
                    println!("------------- DONE!! {:?}", puzzle);

                    Ok(None)
                }
            }
        }

        factory.register("pick", 2, pick);
        factory.register("find", 2, find);
        factory.register("finish", 2, finish);

        let puzzles_count = 1;
        let mut pieces: Vec<JigsawPiece> = Vec::new();
        let mut offset: isize = 0;

        for _ in 0..puzzles_count {
            let size = 3; // rand::thread_rng().gen_range(3..10);
            let mut id: isize = 0;

            for _ in 0..size {
                for _ in 0..size {
                    id = id + 1;

                    let mut relations: Vec<usize> = Vec::new();

                    if id % size != 0 {
                        relations.push((offset + id + 1) as usize);
                    }

                    if id % size != 1 {
                        relations.push((offset + id - 1) as usize);
                    }

                    if id + size <= size * size {
                        relations.push((offset + id + size) as usize);
                    }

                    if id - size > 0 {
                        relations.push((offset + id - size) as usize);
                    }

                    let piece_id = (offset + id) as usize;

                    pieces.push(JigsawPiece {
                        id: piece_id,
                        relations,
                    })
                }
            }

            offset = offset + (size * size);
        }

        // Mix all puzzle pieces to a large chaotic pile
        let mut rng = rand::thread_rng();
        pieces.shuffle(&mut rng);

        for piece in pieces {
            factory.queue(Task::new("pick", piece));
        }

        tokio::time::sleep(Duration::from_millis(3000)).await;
    }
}
