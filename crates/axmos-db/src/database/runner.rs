use std::{
    error::Error,
    io::{Error as IoError, ErrorKind},
    sync::mpsc,
};

use crate::{
    database::{
        SharedCatalog,
        errors::{BoxError, TaskError, TaskResult},
    },
    io::pager::SharedPager,
    transactions::{TransactionCoordinator, accessor::RcPageAccessor, threadpool::ThreadPool},
};

/// Context provided to each task, containing thread-local resources.
pub(crate) struct TaskContext {
    accessor: RcPageAccessor,
    pager: SharedPager,
    coordinator: TransactionCoordinator,
    catalog: SharedCatalog,
}

impl TaskContext {
    fn new(
        pager: SharedPager,
        coordinator: TransactionCoordinator,
        catalog: SharedCatalog,
    ) -> Self {
        Self {
            accessor: RcPageAccessor::new(pager.clone()),
            coordinator,
            pager,
            catalog,
        }
    }

    pub fn accessor(&self) -> RcPageAccessor {
        self.accessor.clone()
    }

    pub fn coordinator(&self) -> TransactionCoordinator {
        self.coordinator.clone()
    }

    pub fn catalog(&self) -> SharedCatalog {
        self.catalog.clone()
    }

    pub fn pager(&self) -> SharedPager {
        self.pager.clone()
    }
}

/// Handles task execution on a thread pool with per-task accessors.
pub struct TaskRunner {
    pool: ThreadPool,
    pager: SharedPager,
    catalog: SharedCatalog,
    coordinator: TransactionCoordinator,
}

impl TaskRunner {
    pub fn new(
        pool_size: usize,
        pager: SharedPager,
        catalog: SharedCatalog,
        coordinator: TransactionCoordinator,
    ) -> Self {
        Self {
            pool: ThreadPool::new(pool_size),
            pager,
            catalog,
            coordinator,
        }
    }

    /// Spawns a task on the thread pool (fire and forget).
    pub fn spawn<F>(&self, task: F) -> TaskResult<()>
    where
        F: FnOnce(&TaskContext) -> Result<(), Box<dyn Error>> + Send + 'static,
    {
        let pager = self.pager.clone();
        let catalog = self.catalog.clone();
        let coordinator = self.coordinator.clone();

        self.pool.execute(move || {
            let ctx = TaskContext::new(pager, coordinator, catalog);
            task(&ctx)
        })?;

        Ok(())
    }

    /// Runs a task and waits for completion (no return value).
    pub fn run<F>(&self, task: F) -> TaskResult<()>
    where
        F: FnOnce(&TaskContext) -> Result<(), BoxError> + Send + Sync + 'static,
    {
        let (tx, rx) = mpsc::channel();
        let pager = self.pager.clone();
        let catalog = self.catalog.clone();
        let coordinator = self.coordinator.clone();

        self.pool.execute(move || {
            let ctx = TaskContext::new(pager, coordinator, catalog);
            let result = task(&ctx);
            let _ = tx.send(result);
            Ok(())
        })?;

        rx.recv()
            .map_err(|_| {
                TaskError::Io(IoError::new(
                    ErrorKind::BrokenPipe,
                    "Task channel closed unexpectedly",
                ))
            })?
            .map_err(|e| TaskError::TaskFailed(e.to_string()))
    }

    /// Runs a task and returns the result.
    /// Runs a task and returns the result.
    pub fn run_with_result<F, T>(&self, task: F) -> Result<T, TaskError>
    where
        F: FnOnce(&TaskContext) -> Result<T, BoxError> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = mpsc::channel::<Result<T, BoxError>>();
        let pager = self.pager.clone();
        let catalog = self.catalog.clone();
        let coordinator = self.coordinator.clone();

        self.pool.execute(move || {
            let ctx = TaskContext::new(pager, coordinator, catalog);
            let result = task(&ctx);
            let _ = tx.send(result);
            Ok(())
        })?;

        let task_result = rx.recv().map_err(|_| {
            TaskError::Io(IoError::new(
                ErrorKind::BrokenPipe,
                "Task channel closed unexpectedly",
            ))
        })?;

        task_result.map_err(|e| TaskError::TaskFailed(e.to_string()))
    }

    /// Spawns multiple tasks and waits for all to complete.
    pub fn run_all<F>(&self, tasks: Vec<F>) -> Result<(), TaskError>
    where
        F: FnOnce(&TaskContext) -> Result<(), BoxError> + Send + 'static,
    {
        let (tx, rx) = mpsc::channel::<Result<(), BoxError>>();
        let task_count = tasks.len();

        for task in tasks {
            let tx = tx.clone();
            let pager = self.pager.clone();
            let catalog = self.catalog.clone();
            let coordinator = self.coordinator.clone();

            self.pool.execute(move || {
                let ctx = TaskContext::new(pager, coordinator, catalog);
                let result = task(&ctx);
                let _ = tx.send(result);
                Ok(())
            })?;
        }

        drop(tx);

        let mut errors = Vec::new();
        for result in rx.iter().take(task_count) {
            if let Err(e) = result {
                errors.push(e.to_string());
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(TaskError::TaskFailed(errors.join("; ")))
        }
    }

    /// Spawns multiple tasks and collects all results.
    pub fn run_all_with_results<F, T>(&self, tasks: Vec<F>) -> Result<Vec<T>, TaskError>
    where
        F: FnOnce(&TaskContext) -> Result<T, BoxError> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = mpsc::channel::<(usize, Result<T, BoxError>)>();
        let task_count = tasks.len();

        for (idx, task) in tasks.into_iter().enumerate() {
            let tx = tx.clone();
            let pager = self.pager.clone();
            let catalog = self.catalog.clone();
            let coordinator = self.coordinator.clone();

            self.pool.execute(move || {
                let ctx = TaskContext::new(pager, coordinator, catalog);
                let result = task(&ctx);
                let _ = tx.send((idx, result));
                Ok(())
            })?;
        }

        drop(tx);

        let mut results: Vec<Option<T>> = (0..task_count).map(|_| None).collect();
        let mut errors = Vec::new();

        for (idx, result) in rx.iter().take(task_count) {
            match result {
                Ok(value) => results[idx] = Some(value),
                Err(e) => errors.push(e.to_string()),
            }
        }

        if errors.is_empty() {
            Ok(results.into_iter().map(|r| r.unwrap()).collect())
        } else {
            Err(TaskError::TaskFailed(errors.join("; ")))
        }
    }
}
