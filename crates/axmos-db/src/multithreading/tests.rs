// Threadpool tests.
use crate::{
    multithreading::threadpool::{ThreadPool, ThreadPoolError},
    param_tests, param2_tests,
};

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

fn test_task_execution(pool_size: usize, num_tasks: usize) {
    let pool = ThreadPool::new(pool_size);
    let counter = Arc::new(AtomicUsize::new(0));

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

fn test_graceful_shutdown(pool_size: usize, num_iters: usize) {
    let mut pool = ThreadPool::new(pool_size);
    let counter = Arc::new(AtomicUsize::new(0));

    for _ in 0..num_iters {
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
    assert_eq!(counter.load(Ordering::SeqCst), num_iters);
}

fn test_failure_after_shutdown(pool_size: usize) {
    let mut pool = ThreadPool::new(pool_size);

    pool.shutdown(Duration::from_secs(1)).unwrap();

    let result = pool.execute(|| Ok(()));
    assert!(matches!(result, Err(ThreadPoolError::PoolShutdown)));
}

fn test_concurrency(num_threads: usize, num_tasks: usize) {
    let pool = ThreadPool::new(num_threads);
    let concurrent_count = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));

    for _ in 0..num_tasks {
        let concurrent = Arc::clone(&concurrent_count);
        let max = Arc::clone(&max_concurrent);

        pool.execute(move || {
            // Increment concurrent counter
            let current = concurrent.fetch_add(1, Ordering::SeqCst) + 1;

            // Update max if this is the highest we've seen
            let mut old_max = max.load(Ordering::SeqCst);
            while current > old_max {
                match max.compare_exchange(old_max, current, Ordering::SeqCst, Ordering::SeqCst) {
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

param2_tests!(test_concurrency, num_threads, num_tasks => [
    (4, 64),
    (8, 256),
    (16, 256),
    (20, 256),
    (2, 100)
]);

param2_tests!(test_task_execution, num_threads, num_tasks => [
    (4, 64),
    (8, 256),
    (16, 256),
    (20, 256),
    (2, 100)
]);

param2_tests!(test_graceful_shutdown, pool_size, num_iters => [
    (4, 64),
    (8, 256),
    (16, 256),
    (20, 256),
    (2, 100)
]);

param_tests!(test_failure_after_shutdown, pool_size => [
    4, 8, 10, 12, 16, 32, 64
]);
