use std::{
    collections::VecDeque,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{database::errors::ThreadPoolError, types::WorkerId};
use parking_lot::{Condvar, Mutex};

// Simplistic thread safe queue implementation.
// Can be improved with crossbeam but for now it is kept like this for more flexibility and to reduce the number of dependencies we rely on.
pub(crate) struct JobQueue<T> {
    queue: Mutex<VecDeque<T>>,
    condvar: Condvar,
}

impl<T> JobQueue<T> {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
        }
    }

    pub fn push(&self, value: T) {
        self.queue.lock().push_back(value);
        self.condvar.notify_one();
    }

    pub fn pop(&self) -> Option<T> {
        self.queue.lock().pop_front()
    }

    /// Blocking pop (waits until a job is available or timeout)
    pub fn pop_blocking(&self, timeout: Duration) -> Option<T> {
        let mut guard = self.queue.lock();
        loop {
            if let Some(value) = guard.pop_front() {
                return Some(value);
            }

            let result = self.condvar.wait_for(&mut guard, timeout);
            if result.timed_out() {
                return guard.pop_front();
            }
        }
    }

    /// Blocking pop that can be interrupted by a running flag
    pub fn pop_interruptible(&self, timeout: Duration, running: &AtomicBool) -> Option<T> {
        let mut guard = self.queue.lock();
        loop {
            if let Some(value) = guard.pop_front() {
                return Some(value);
            }

            if !running.load(Ordering::Relaxed) {
                return None;
            }

            let result = self.condvar.wait_for(&mut guard, timeout);
            if result.timed_out() && !running.load(Ordering::Relaxed) {
                return None;
            }
        }
    }

    pub fn notify_all(&self) {
        self.condvar.notify_all();
    }

    pub fn len(&self) -> usize {
        self.queue.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.lock().is_empty()
    }
}

impl<T> Default for JobQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) enum Job {
    Task(Box<dyn FnOnce() -> Result<(), Box<dyn Error>> + Send + 'static>),
    Shutdown,
}

struct Worker {
    id: WorkerId,
    thread: Option<JoinHandle<()>>,
}

impl Worker {
    fn new(id: WorkerId, job_queue: Arc<JobQueue<Job>>, running: Arc<AtomicBool>) -> Worker {
        let thread = thread::spawn(move || {
            while let Some(job) = job_queue.pop_interruptible(Duration::from_millis(100), &running)
            {
                match job {
                    Job::Task(task) => {
                        let _ = task();
                    }
                    Job::Shutdown => break,
                }
            }
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}

pub(crate) struct ThreadPool {
    workers: Vec<Worker>,
    job_queue: Arc<JobQueue<Job>>,
    running: Arc<AtomicBool>,
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        let job_queue = Arc::new(JobQueue::new());
        let running = Arc::new(AtomicBool::new(true));
        let mut workers = Vec::with_capacity(size);

        for _ in 0..size {
            workers.push(Worker::new(
                WorkerId::new(),
                Arc::clone(&job_queue),
                Arc::clone(&running),
            ));
        }

        ThreadPool {
            workers,
            job_queue,
            running,
        }
    }

    pub fn execute<F>(&self, f: F) -> Result<(), ThreadPoolError>
    where
        F: FnOnce() -> Result<(), Box<dyn Error>> + Send + 'static,
    {
        if !self.running.load(Ordering::Acquire) {
            return Err(ThreadPoolError::PoolShutdown);
        }

        self.job_queue.push(Job::Task(Box::new(f)));
        Ok(())
    }

    pub fn shutdown(&mut self, timeout: Duration) -> Result<(), ThreadPoolError> {
        let start = Instant::now();

        // Signal workers to stop
        self.running.store(false, Ordering::Release);

        // Wake up all waiting threads
        self.job_queue.notify_all();

        // Wait for workers to finish
        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                let remaining = timeout
                    .checked_sub(start.elapsed())
                    .unwrap_or(Duration::ZERO);

                if remaining.is_zero() {
                    return Err(ThreadPoolError::ShutdownTimeout);
                }

                if thread.join().is_err() {
                    return Err(ThreadPoolError::ThreadJoinError(format!(
                        "Worker {} failed to join",
                        worker.id
                    )));
                }
            }
        }

        if start.elapsed() > timeout {
            Err(ThreadPoolError::ShutdownTimeout)
        } else {
            Ok(())
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        if !self.workers.is_empty() && self.running.load(Ordering::Acquire) {
            let _ = self.shutdown(Duration::from_secs(2));
        }
    }
}

#[cfg(test)]
mod threadpool_tests {
    use super::*;
    use std::{
        sync::{Arc, atomic::AtomicUsize},
        time::Duration,
    };

    #[test]
    fn test_task_execution() {
        let pool = ThreadPool::new(4);
        let counter = Arc::new(AtomicUsize::new(0));

        let num_tasks = 100;
        for _ in 0..num_tasks {
            let counter = Arc::clone(&counter);
            pool.execute(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        }

        // Give tasks time to complete
        thread::sleep(Duration::from_millis(500));

        assert_eq!(counter.load(Ordering::SeqCst), num_tasks);
    }

    #[test]
    fn test_graceful_shutdown() {
        let mut pool = ThreadPool::new(4);
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..50 {
            let counter = Arc::clone(&counter);
            pool.execute(move || {
                thread::sleep(Duration::from_millis(10));
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        }

        let result = pool.shutdown(Duration::from_secs(5));
        assert!(result.is_ok());

        // All tasks should have completed before shutdown finished
        assert_eq!(counter.load(Ordering::SeqCst), 50);
    }

    #[test]
    fn test_failure_after_shutdown() {
        let mut pool = ThreadPool::new(2);

        pool.shutdown(Duration::from_secs(1)).unwrap();

        let result = pool.execute(|| Ok(()));
        assert!(matches!(result, Err(ThreadPoolError::PoolShutdown)));
    }

    #[test]
    fn test_concurrency() {
        let pool = ThreadPool::new(4);
        let concurrent_count = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));

        let num_tasks = 8;
        for _ in 0..num_tasks {
            let concurrent = Arc::clone(&concurrent_count);
            let max = Arc::clone(&max_concurrent);

            pool.execute(move || {
                // Increment concurrent counter
                let current = concurrent.fetch_add(1, Ordering::SeqCst) + 1;

                // Update max if this is the highest we've seen
                let mut old_max = max.load(Ordering::SeqCst);
                while current > old_max {
                    match max.compare_exchange(old_max, current, Ordering::SeqCst, Ordering::SeqCst)
                    {
                        Ok(_) => break,
                        Err(x) => old_max = x,
                    }
                }

                // Simulate work
                thread::sleep(Duration::from_millis(100));

                // Decrement concurrent counter
                concurrent.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        }

        // Wait for all tasks to complete
        thread::sleep(Duration::from_millis(500));

        // If tasks run sequentially, `max_concurrent` would never exceed 1 because each task finishes before the next starts.
        // But with true parallelism, multiple tasks overlap during their sleep, so we see [`max_concurrent > 1`].
        // We should have seen multiple tasks running concurrently
        let max_seen = max_concurrent.load(Ordering::SeqCst);
        assert!(
            max_seen > 1,
            "Expected concurrent execution, but max concurrent was {}",
            max_seen
        );
    }
}
