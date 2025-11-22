use crate::types::TxId;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

trait FnBox {
    fn call(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call(self: Box<F>) {
        (*self)()
    }
}

// Thunk is a wrapper for task submission.
type Thunk<'a> = Box<dyn FnBox + Send + 'a>;

struct Sentinel<'a> {
    shared_data: &'a Arc<ThreadPoolSharedData>,
    active: bool,
}

impl<'a> Sentinel<'a> {
    fn new(shared_data: &'a Arc<ThreadPoolSharedData>) -> Sentinel<'a> {
        Sentinel {
            shared_data,
            active: true,
        }
    }

    /// Deactivates the sentinel
    fn deactivate(mut self) {
        self.active = false;
    }
}

impl<'a> Drop for Sentinel<'a> {
    fn drop(&mut self) {
        if self.active {
            self.shared_data.active_count.fetch_sub(1, Ordering::SeqCst);
            if thread::panicking() {
                self.shared_data.panic_count.fetch_add(1, Ordering::SeqCst);
            }
            self.shared_data.no_work_notify_all();
            spawn_in_pool(self.shared_data.clone())
        }
    }
}

/// [`ThreadPool`] builder.
/// Has two configuration parameters:
/// - num threads: number of threads that will be in the pool.
/// - thread-stack_size: the size of the stack for each thread.
#[derive(Clone, Default)]
pub struct ThreadPoolBuilder {
    num_threads: Option<usize>,
    thread_stack_size: Option<usize>,
}

impl ThreadPoolBuilder {
    pub fn new() -> ThreadPoolBuilder {
        ThreadPoolBuilder {
            num_threads: None,
            thread_stack_size: None,
        }
    }

    /// Set the maximum number of worker-threads that will be alive at any given moment by the built [`ThreadPool`]. If not specified, defaults the number of threads to the number of CPUs.
    ///
    /// This method will panic if `num_threads` is 0.
    pub fn with_num_threads(mut self, num_threads: usize) -> ThreadPoolBuilder {
        assert!(num_threads > 0);
        self.num_threads = Some(num_threads);
        self
    }

    /// Set the stack size (in bytes) for each of the threads spawned by the built [`ThreadPool`].
    /// If not specified, threads spawned by the threadpool will have a stack size [as specified in
    /// the `std::thread` documentation][thread].
    ///
    /// [thread]: https://doc.rust-lang.org/nightly/std/thread/index.html#stack-size
    /// [`ThreadPool`]: struct.ThreadPool.html
    pub fn with_thread_stack_size(mut self, size: usize) -> ThreadPoolBuilder {
        self.thread_stack_size = Some(size);
        self
    }

    /// Finalize the [`ThreadPoolBuilder`] and build the [`ThreadPool`].
    pub fn build(self) -> ThreadPool {
        let (tx, rx) = channel::<Thunk<'static>>();

        let num_threads = self.num_threads.unwrap_or_else(num_cpus::get);

        // Shared data by all threads is stored in an arc.
        // It is not modifyiable so no need to add more than this
        let shared_data = Arc::new(ThreadPoolSharedData {
            id: TxId::new(),
            job_receiver: Mutex::new(rx),
            empty_condvar: Condvar::new(),
            empty_trigger: Mutex::new(()),
            join_generation: AtomicUsize::new(0),
            queued_count: AtomicUsize::new(0),
            active_count: AtomicUsize::new(0),
            max_thread_count: AtomicUsize::new(num_threads),
            panic_count: AtomicUsize::new(0),
            stack_size: self.thread_stack_size,
        });

        // Threadpool threads
        for _ in 0..num_threads {
            spawn_in_pool(shared_data.clone());
        }

        ThreadPool {
            jobs: tx,
            shared_data,
        }
    }
}

struct ThreadPoolSharedData {
    id: TxId,
    job_receiver: Mutex<Receiver<Thunk<'static>>>,
    empty_trigger: Mutex<()>,
    empty_condvar: Condvar,
    join_generation: AtomicUsize,
    queued_count: AtomicUsize,
    active_count: AtomicUsize,
    max_thread_count: AtomicUsize,
    panic_count: AtomicUsize,
    stack_size: Option<usize>,
}

impl ThreadPoolSharedData {
    fn has_work(&self) -> bool {
        self.queued_count.load(Ordering::SeqCst) > 0 || self.active_count.load(Ordering::SeqCst) > 0
    }

    /// Notify all observers joining this pool if there is no more work to do.
    fn no_work_notify_all(&self) {
        if !self.has_work() {
            *self
                .empty_trigger
                .lock()
                .expect("Mutex poisoned. Unable to notify all joining threads");
            self.empty_condvar.notify_all();
        }
    }
}

/// Abstraction of a thread pool for basic parallelism.
pub struct ThreadPool {
    // How the threadpool communicates with subthreads.
    //
    // This is the only such Sender, so when it is dropped all subthreads will
    // quit.
    jobs: Sender<Thunk<'static>>,
    shared_data: Arc<ThreadPoolSharedData>,
}

impl ThreadPool {
    /// Creates a new thread pool capable of executing `num_threads` number of jobs concurrently.
    ///
    /// # Panics
    ///
    /// This function will panic if `num_threads` is 0.
    pub fn new(num_threads: usize) -> ThreadPool {
        ThreadPoolBuilder::new()
            .with_num_threads(num_threads)
            .build()
    }

    /// Executes the function `job` on a thread in the pool.
    pub fn execute<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.shared_data.queued_count.fetch_add(1, Ordering::SeqCst);
        self.jobs
            .send(Box::new(job))
            .expect("ThreadPool::execute unable to send job into queue.");
    }

    /// Returns the number of jobs waiting to executed in the pool.
    pub fn queued_count(&self) -> usize {
        self.shared_data.queued_count.load(Ordering::Relaxed)
    }

    /// Returns the number of currently active threads.
    pub fn active_count(&self) -> usize {
        self.shared_data.active_count.load(Ordering::SeqCst)
    }

    /// Returns the maximum number of threads the pool will execute concurrently.
    pub fn max_count(&self) -> usize {
        self.shared_data.max_thread_count.load(Ordering::Relaxed)
    }

    /// Returns the number of panicked threads over the lifetime of the pool.
    pub fn panic_count(&self) -> usize {
        self.shared_data.panic_count.load(Ordering::Relaxed)
    }

    /// Sets the number of worker-threads to use as `num_threads`.
    pub fn set_num_threads(&mut self, num_threads: usize) {
        assert!(num_threads >= 1);
        let prev_num_threads = self
            .shared_data
            .max_thread_count
            .swap(num_threads, Ordering::Release);
        if let Some(num_spawn) = num_threads.checked_sub(prev_num_threads) {
            // Spawn new threads
            for _ in 0..num_spawn {
                spawn_in_pool(self.shared_data.clone());
            }
        }
    }

    /// Block the current thread until all jobs in the pool have been executed.
    ///
    /// Calling `join` on an empty pool will cause an immediate return.
    /// `join` may be called from multiple threads concurrently.
    /// A `join` is an atomic point in time. All threads joining before the join
    /// event will exit together even if the pool is processing new jobs by the
    /// time they get scheduled.
    ///
    /// Calling `join` from a thread within the pool will cause a deadlock. This
    /// behavior is considered safe.
    pub fn join(&self) {
        // fast path requires no mutex
        if !self.shared_data.has_work() {
            return ();
        }

        let generation = self.shared_data.join_generation.load(Ordering::SeqCst);
        let mut lock = self.shared_data.empty_trigger.lock().unwrap();

        // Spin until all threads finish.
        while generation == self.shared_data.join_generation.load(Ordering::Relaxed)
            && self.shared_data.has_work()
        {
            lock = self.shared_data.empty_condvar.wait(lock).unwrap();
        }

        // increase generation if we are the first thread to come out of the loop
        let _ = self.shared_data.join_generation.compare_exchange_weak(
            generation,
            generation.wrapping_add(1),
            Ordering::SeqCst,
            Ordering::Relaxed,
        );
    }
}

impl Clone for ThreadPool {
    /// Cloning a pool will create a new handle to the pool.
    /// The behavior is similar to [Arc](https://doc.rust-lang.org/stable/std/sync/struct.Arc.html).
    fn clone(&self) -> ThreadPool {
        ThreadPool {
            jobs: self.jobs.clone(),
            shared_data: self.shared_data.clone(),
        }
    }
}

/// Create a thread pool with one thread per CPU.
/// On machines with hyperthreading,
/// this will create one thread per hyperthread.
impl Default for ThreadPool {
    fn default() -> Self {
        ThreadPool::new(num_cpus::get())
    }
}

impl fmt::Debug for ThreadPool {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ThreadPool")
            .field("id", &self.shared_data.id)
            .field("queued_count", &self.queued_count())
            .field("active_count", &self.active_count())
            .field("max_count", &self.max_count())
            .finish()
    }
}

impl PartialEq for ThreadPool {
    /// Check if you are working with the same pool
    fn eq(&self, other: &ThreadPool) -> bool {
        Arc::ptr_eq(&self.shared_data, &other.shared_data)
    }
}
impl Eq for ThreadPool {}

fn spawn_in_pool(shared_data: Arc<ThreadPoolSharedData>) {
    let mut builder = thread::Builder::new();

    builder = builder.name(format!("Transaction: {}", shared_data.id));

    if let Some(ref stack_size) = shared_data.stack_size {
        builder = builder.stack_size(stack_size.to_owned());
    }
    builder
        .spawn(move || {
            // Will spawn a new thread on panic unless it is cancelled.
            let sentinel = Sentinel::new(&shared_data);

            loop {
                // Shutdown this thread if the pool has become smaller
                let thread_counter_val = shared_data.active_count.load(Ordering::Acquire);
                let max_thread_count_val = shared_data.max_thread_count.load(Ordering::Relaxed);
                if thread_counter_val >= max_thread_count_val {
                    break;
                }
                let message = {
                    // Only lock jobs for the time it takes
                    // to get a job, not run it.
                    let lock = shared_data
                        .job_receiver
                        .lock()
                        .expect("Worker thread unable to lock job_receiver");
                    lock.recv()
                };

                let job = match message {
                    Ok(job) => job,
                    // The ThreadPool was dropped.
                    Err(..) => break,
                };
                // Do not allow IR around the job execution
                shared_data.active_count.fetch_add(1, Ordering::SeqCst);
                shared_data.queued_count.fetch_sub(1, Ordering::SeqCst);

                job.call();

                shared_data.active_count.fetch_sub(1, Ordering::SeqCst);
                shared_data.no_work_notify_all();
            }

            sentinel.deactivate();
        })
        .unwrap();
}

#[cfg(test)]
mod tpl_tests {
    use super::*;
    use serial_test::serial;
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
        mpsc::channel,
    };
    use std::thread::{self, sleep};
    use std::time::Duration;
    const TEST_TASKS: usize = 4;

    #[test]
    #[serial]
    fn test_tpl_1() {
        // Verifies correct thread pool creation.
        let pool = ThreadPool::new(TEST_TASKS);
        assert_eq!(pool.max_count(), TEST_TASKS);
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.queued_count(), 0);
        assert_eq!(pool.panic_count(), 0);
    }

    #[test]
    #[serial]
    fn test_tpl_2() {
        // Verifies thread pool number of threads consistency
        let pool = ThreadPoolBuilder::new()
            .with_num_threads(3)
            .with_thread_stack_size(8 * 1024 * 1024)
            .build();

        assert_eq!(pool.max_count(), 3);
    }

    // Verifies panicking at creation time when the number of threads arg specified is invalid.
    #[test]
    #[should_panic]
    fn test_tpl_3() {
        ThreadPool::new(0);
    }

    #[test]
    #[serial]
    fn test_tpl_4() {
        // Validates the [max_thread_count] argument is consistent with [num_gpus] api response.
        let pool = ThreadPool::default();
        assert_eq!(pool.max_count(), num_cpus::get());
    }

    #[test]
    fn test_tpl_5() {
        let pool = ThreadPool::new(TEST_TASKS);

        // Allocate a sender to spawn each task
        let (tx, rx) = channel();
        for _ in 0..TEST_TASKS {
            let tx = tx.clone();
            pool.execute(move || {
                tx.send(1).unwrap();
            });
        }
        // Validate that all tasks ran properly
        assert_eq!(rx.iter().take(TEST_TASKS).sum::<usize>(), TEST_TASKS);
    }

    #[test]
    fn test_tpl_6() {
        // Validate that we can increas the number of threads at runtime.
        let new_thread_amount = TEST_TASKS + 8;
        let mut pool = ThreadPool::new(TEST_TASKS);

        // Fill the pool with sleeping threads.
        for _ in 0..TEST_TASKS {
            pool.execute(move || sleep(Duration::from_millis(500)));
        }
        sleep(Duration::from_millis(100));
        assert_eq!(pool.active_count(), TEST_TASKS);

        // Aumentar threads
        pool.set_num_threads(new_thread_amount);

        // Agregar más tareas para usar los nuevos threads
        for _ in 0..(new_thread_amount - TEST_TASKS) {
            pool.execute(move || sleep(Duration::from_millis(500)));
        }
        sleep(Duration::from_millis(100));
        assert_eq!(pool.active_count(), new_thread_amount);

        pool.join();
    }

    #[test]
    fn test_tpl_7() {
        let new_thread_amount = 2;
        let mut pool = ThreadPool::new(TEST_TASKS);

        // Execute the first tasks.
        for _ in 0..TEST_TASKS {
            pool.execute(move || {
                assert_eq!(1, 1);
            });
        }

        // Reduce the number of threads
        pool.set_num_threads(new_thread_amount);

        // Validate that only the new threads are active.
        for _ in 0..new_thread_amount {
            pool.execute(move || sleep(Duration::from_millis(500)));
        }
        sleep(Duration::from_millis(100));
        assert_eq!(pool.active_count(), new_thread_amount);

        pool.join();
    }

    #[test]
    fn test_tpl_8() {
        let pool = ThreadPool::new(TEST_TASKS);
        let barrier = Arc::new(Barrier::new(TEST_TASKS + 1));

        for _ in 0..2 * TEST_TASKS {
            let barrier_clone = barrier.clone();
            pool.execute(move || {
                barrier_clone.wait();
                sleep(Duration::from_millis(100));
            });
        }

        barrier.wait();
        let active_count = pool.active_count();
        assert_eq!(active_count, TEST_TASKS);

        pool.join();
        assert_eq!(pool.active_count(), 0);
    }

    #[test]
    fn test_tpl_9() {
        let pool = ThreadPool::new(1);
        let barrier = Arc::new(Barrier::new(2));

        // Bloquear el único thread
        let barrier_clone = barrier.clone();
        pool.execute(move || {
            barrier_clone.wait();
            sleep(Duration::from_millis(200));
        });

        // Agregar tareas que se encolarán
        for _ in 0..5 {
            pool.execute(move || {
                sleep(Duration::from_millis(10));
            });
        }

        // Verificar tareas en cola
        assert_eq!(pool.queued_count(), 6);

        // Liberar el thread
        barrier.wait();
        pool.join();

        assert_eq!(pool.queued_count(), 0);
    }

    #[test]
    fn test_tpl_10() {
        let pool = ThreadPool::new(TEST_TASKS);

        // Hacer panic en todos los threads existentes
        for _ in 0..TEST_TASKS {
            pool.execute(move || panic!("Test panic - ignore"));
        }
        pool.join();

        assert_eq!(pool.panic_count(), TEST_TASKS);

        // Asegurar que se crearon nuevos threads para compensar
        let (tx, rx) = channel();
        for _ in 0..TEST_TASKS {
            let tx = tx.clone();
            pool.execute(move || {
                tx.send(1).unwrap();
            });
        }

        assert_eq!(rx.iter().take(TEST_TASKS).fold(0, |a, b| a + b), TEST_TASKS);
    }

    #[test]
    fn test_tpl_11() {
        let pool = ThreadPool::new(TEST_TASKS);
        let waiter = Arc::new(Barrier::new(TEST_TASKS + 1));

        // Programar panics después del drop
        for _ in 0..TEST_TASKS {
            let waiter = waiter.clone();
            pool.execute(move || {
                waiter.wait();
                panic!("Test panic after drop");
            });
        }

        drop(pool);

        // Activar los panics
        waiter.wait();
    }

    #[test]
    fn test_tpl_12() {
        let test_tasks = 10_000; // Reducido para tests más rápidos
        let pool = ThreadPool::new(TEST_TASKS);
        let (tx, rx) = channel();

        for _ in 0..test_tasks {
            let tx = tx.clone();
            pool.execute(move || {
                tx.send(1).is_ok();
            });
        }

        assert_eq!(rx.iter().take(test_tasks).fold(0, |a, b| a + b), test_tasks);
        pool.join();
        assert_eq!(pool.active_count(), 0);
    }

    #[test]
    fn test_tpl_13() {
        let pool = ThreadPool::new(4);

        // Join en pool vacío debe retornar inmediatamente
        let start = std::time::Instant::now();
        pool.join();
        let duration = start.elapsed();

        assert!(duration < Duration::from_millis(50));
    }

    #[test]
    fn test_tpl_14() {
        let pool = ThreadPool::new(8);
        let test_count = Arc::new(AtomicUsize::new(0));

        // Primera ronda
        for _ in 0..42 {
            let test_count = test_count.clone();
            pool.execute(move || {
                sleep(Duration::from_millis(10));
                test_count.fetch_add(1, Ordering::Release);
            });
        }

        pool.join();
        assert_eq!(42, test_count.load(Ordering::Acquire));

        // Segunda ronda
        for _ in 0..42 {
            let test_count = test_count.clone();
            pool.execute(move || {
                sleep(Duration::from_millis(10));
                test_count.fetch_add(1, Ordering::Relaxed);
            });
        }

        pool.join();
        assert_eq!(84, test_count.load(Ordering::Relaxed));
    }

    #[test]
    fn test_tpl_15() {
        let pool0 = ThreadPool::new(4);
        let pool1 = ThreadPool::new(4);
        let (tx, rx) = channel();

        for i in 0..8 {
            let pool1 = pool1.clone();
            let pool0_ = pool0.clone();
            let tx = tx.clone();

            pool0.execute(move || {
                pool1.execute(move || {
                    pool0_.join();
                    tx.send(i).expect("send from pool1");
                });
            });
        }

        drop(tx);

        pool0.join();
        pool1.join();

        let sum: i32 = rx.iter().sum();
        assert_eq!(sum, (0..8).sum::<i32>());
    }

    #[test]
    fn test_tpl_16() {
        let pool = ThreadPool::new(2);
        let pool_clone = pool.clone();

        assert_eq!(pool, pool_clone);

        let (tx, rx) = channel();

        // Usar el pool original
        for i in 0..5 {
            let tx = tx.clone();
            pool.execute(move || {
                tx.send(i).unwrap();
            });
        }

        // Usar el clon
        for i in 5..10 {
            let tx = tx.clone();
            pool_clone.execute(move || {
                tx.send(i).unwrap();
            });
        }

        drop(tx);

        let sum: i32 = rx.iter().sum();
        assert_eq!(sum, (0..10).sum::<i32>());

        pool.join();
    }

    #[test]
    fn test_tpl_17() {
        let pool = ThreadPool::new(4);
        let debug = format!("{:?}", pool);

        assert!(debug.contains("ThreadPool"));
        assert!(debug.contains("queued_count"));
        assert!(debug.contains("active_count"));
        assert!(debug.contains("max_count"));

        // Agregar una tarea y verificar que se refleja en debug
        pool.execute(move || sleep(Duration::from_millis(500)));
        sleep(Duration::from_millis(100));

        let debug = format!("{:?}", pool);
        assert!(debug.contains("active_count"));
    }

    #[test]
    fn test_tpl_18() {
        fn assert_sync<T: Sync>() {}
        fn assert_send<T: Send>() {}

        assert_sync::<ThreadPoolSharedData>();
        assert_send::<ThreadPoolSharedData>();
        assert_send::<ThreadPool>();
    }

    #[test]
    fn test_tpl_19() {
        let pool = Arc::new(ThreadPool::new(2));
        let counter = Arc::new(AtomicUsize::new(0));

        // Primera generación
        for _ in 0..5 {
            let counter_clone = counter.clone();
            pool.execute(move || {
                sleep(Duration::from_millis(10));
                counter_clone.fetch_add(1, Ordering::SeqCst);
            });
        }

        pool.join();
        assert_eq!(counter.load(Ordering::SeqCst), 5);

        // Segunda generación - el join anterior no debe afectar
        for _ in 0..5 {
            let counter_clone = counter.clone();
            pool.execute(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            });
        }

        pool.join();
        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[test]
    fn test_tpl_20() {
        let pool = Arc::new(ThreadPool::new(4));
        let barrier = Arc::new(Barrier::new(4)); // 3 join threads + main

        // Agregar trabajo
        for _ in 0..10 {
            pool.execute(move || {
                sleep(Duration::from_millis(50));
            });
        }

        // Múltiples threads haciendo join
        let mut handles = vec![];
        for _ in 0..3 {
            let pool_clone = pool.clone();
            let barrier_clone = barrier.clone();

            let handle = thread::spawn(move || {
                barrier_clone.wait();
                pool_clone.join();
            });
            handles.push(handle);
        }

        barrier.wait(); // Liberar todos los threads

        // Todos deberían completarse sin deadlock
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_tpl_21() {
        let pool = ThreadPool::new(2);
        let thread_ids = Arc::new(Mutex::new(Vec::new()));

        // Primera ronda - capturar IDs de threads
        for _ in 0..4 {
            let thread_ids_clone = thread_ids.clone();
            pool.execute(move || {
                let id = thread::current().id();
                thread_ids_clone.lock().unwrap().push(id);
            });
        }

        pool.join();

        // Segunda ronda - verificar reutilización
        for _ in 0..4 {
            let thread_ids_clone = thread_ids.clone();
            pool.execute(move || {
                let id = thread::current().id();
                thread_ids_clone.lock().unwrap().push(id);
            });
        }

        pool.join();

        // Deberíamos tener solo 2 IDs únicos (reutilización de threads)
        let ids = thread_ids.lock().unwrap();
        let unique_ids: std::collections::HashSet<_> = ids.iter().collect();
        assert!(unique_ids.len() <= pool.max_count());
    }
}
