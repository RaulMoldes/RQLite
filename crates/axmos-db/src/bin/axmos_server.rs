//! AxmosDB Server Binary
use axmosdb::{
    Database,
    common::DBConfig,
    runtime::QueryResult,
    tcp::{Request, Response, TcpError, recv_request, send_response, session::Session},
};

use std::{
    env,
    io::{BufReader, BufWriter},
    net::{TcpListener, TcpStream, SocketAddr},
    path::PathBuf,
    process,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
};

use parking_lot::RwLock;

const VERSION: &str = env!("CARGO_PKG_VERSION");

struct ServerConfig {
    port: u16,
    db_path: Option<PathBuf>,
    db_config: DBConfig,
}

impl ServerConfig {
    fn from_args() -> Result<Self, String> {
        let args: Vec<String> = env::args().collect();
        let mut port = 5432u16;
        let mut db_path = None;

        let default_config = DBConfig::default();
        let mut page_size = default_config.page_size;
        let mut cache_size = default_config.cache_size;
        let mut pool_size = default_config.pool_size;
        let mut min_keys = default_config.min_keys_per_page;
        let mut siblings = default_config.num_siblings_per_side;

        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "-p" | "--port" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("Missing port value".into());
                    }
                    port = args[i].parse().map_err(|_| "Invalid port number")?;
                }
                "-f" | "--file" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("Missing file path".into());
                    }
                    db_path = Some(PathBuf::from(&args[i]));
                }
                "--page-size" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("Missing page size value".into());
                    }
                    page_size = args[i].parse().map_err(|_| "Invalid page size")?;
                    if !page_size.is_power_of_two() {
                        return Err("Page size must be a power of 2".into());
                    }
                    if page_size < 4096 || page_size > 65536 {
                        return Err("Page size must be between 4096 and 65536".into());
                    }
                }
                "--cache-size" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("Missing cache size value".into());
                    }
                    cache_size = args[i].parse().map_err(|_| "Invalid cache size")?;
                }
                "--pool-size" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("Missing pool size value".into());
                    }
                    pool_size = args[i].parse().map_err(|_| "Invalid pool size")?;
                    if pool_size == 0 {
                        return Err("Pool size must be at least 1".into());
                    }
                }
                "--min-keys" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("Missing min keys value".into());
                    }
                    min_keys = args[i].parse().map_err(|_| "Invalid min keys value")?;
                    if min_keys < 2 {
                        return Err("Min keys must be at least 2".into());
                    }
                }
                "--siblings" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("Missing siblings value".into());
                    }
                    siblings = args[i].parse().map_err(|_| "Invalid siblings value")?;
                }
                "-h" | "--help" => {
                    print_usage();
                    process::exit(0);
                }
                "-v" | "--version" => {
                    println!("axmos-server {}", VERSION);
                    process::exit(0);
                }
                arg => {
                    return Err(format!("Unknown argument: {}", arg));
                }
            }
            i += 1;
        }

        let db_config = DBConfig::new(page_size, cache_size, pool_size, min_keys, siblings);

        Ok(Self {
            port,
            db_path,
            db_config,
        })
    }
}

fn print_usage() {
    eprintln!("AxmosDB Server v{}", VERSION);
    eprintln!();
    eprintln!("Usage: axmos-server [OPTIONS]");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  -p, --port <PORT>         Port to listen on (default: 5432)");
    eprintln!("  -f, --file <PATH>         Database file to open on startup");
    eprintln!();
    eprintln!("Database Configuration:");
    eprintln!("  --page-size <SIZE>        Page size in bytes (default: 4096)");
    eprintln!("                            Must be power of 2 between 4096 and 65536");
    eprintln!("  --cache-size <PAGES>      Cache size in pages (default: 10000)");
    eprintln!("  --pool-size <N>           Thread pool size (default: num CPUs)");
    eprintln!("  --min-keys <N>            Minimum keys per B+tree page (default: 3)");
    eprintln!("  --siblings <N>            Siblings per side for balancing (default: 2)");
    eprintln!();
    eprintln!("  -v, --version             Show version information");
    eprintln!("  -h, --help                Show this help message");
}

struct ClientContext {
    session: Option<Session>,
}

struct SharedState {
    db: RwLock<Option<Database>>,
    db_config: DBConfig,
    shutdown: AtomicBool,
}

impl SharedState {
    fn new(db: Option<Database>, db_config: DBConfig) -> Self {
        Self {
            db: RwLock::new(db),
            db_config,
            shutdown: AtomicBool::new(false),
        }
    }
}

struct Server {
    listener: TcpListener,
    state: Arc<SharedState>,
}

impl Server {
    fn new(config: ServerConfig) -> Result<Self, String> {
        let addr = format!("0.0.0.0:{}", config.port);
        let listener =
            TcpListener::bind(&addr).map_err(|e| format!("Failed to bind to {}: {}", addr, e))?;

        println!("╔════════════════════════════════════════╗");
        println!("║       AxmosDB Server v{}          ║", VERSION);
        println!("╚════════════════════════════════════════╝");
        println!();
        println!("Listening on: {}", addr);
        println!();
        println!("Configuration:");
        println!("  Page size:    {} bytes", config.db_config.page_size);
        println!("  Cache size:   {} pages", config.db_config.cache_size);
        println!("  Pool size:    {} workers", config.db_config.pool_size);
        println!("  Min keys:     {}", config.db_config.min_keys_per_page);
        println!("  Siblings:     {}", config.db_config.num_siblings_per_side);

        let db = if let Some(path) = config.db_path {
            println!();
            println!("Opening database: {}", path.display());
            match Database::open_or_create(&path, config.db_config) {
                Ok(db) => {
                    println!("Database ready");
                    Some(db)
                }
                Err(e) => {
                    eprintln!("Warning: Failed to open database: {}", e);
                    None
                }
            }
        } else {
            println!();
            println!("No database specified. Use CREATEDB or OPENDB commands.");
            None
        };

        let state = Arc::new(SharedState::new(db, config.db_config));

        println!();
        Ok(Self { listener, state })
    }

    fn run(&mut self) {
        println!("Ready to accept connections. Use SHUTDOWN command or Ctrl+C to stop.");
        println!();

        self.listener
            .set_nonblocking(true)
            .expect("Failed to set non-blocking mode");

        let mut client_handles: Vec<JoinHandle<()>> = Vec::new();

        loop {
            if self.state.shutdown.load(Ordering::Relaxed) {
                println!("Shutdown requested...");
                break;
            }

            match self.listener.accept() {
                Ok((stream, addr)) => {
                    println!("[{}] Connected", addr);

                    let state = Arc::clone(&self.state);
                    let handle = thread::spawn(move || {
                        handle_client(stream, addr, state);
                    });

                    client_handles.push(handle);
                    client_handles.retain(|h| !h.is_finished());
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(e) => {
                    eprintln!("Accept error: {}", e);
                }
            }
        }

        println!("Waiting for {} client(s) to disconnect...", client_handles.len());
        for handle in client_handles {
            let _ = handle.join();
        }

        if let Some(db) = self.state.db.write().take() {
            println!("Closing database...");
            drop(db);
        }

        println!("Server stopped.");
    }
}

fn handle_client(stream: TcpStream, addr: SocketAddr, state: Arc<SharedState>) {
    if let Err(e) = run_client_loop(stream, &state) {
        eprintln!("[{}] Client error: {}", addr, e);
    }
    println!("[{}] Disconnected", addr);
}

fn run_client_loop(stream: TcpStream, state: &Arc<SharedState>) -> Result<(), TcpError> {
    let mut ctx = ClientContext { session: None };

    stream.set_nonblocking(false).map_err(TcpError::Io)?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(300)))
        .ok();
    stream
        .set_write_timeout(Some(std::time::Duration::from_secs(60)))
        .ok();

    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);

    loop {
        if state.shutdown.load(Ordering::Relaxed) {
            send_response(&mut writer, &Response::ShuttingDown)?;
            break;
        }

        let request = match recv_request(&mut reader) {
            Ok(req) => req,
            Err(TcpError::ConnectionClosed) => break,
            Err(TcpError::Io(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => {
                eprintln!("Request error: {}", e);
                break;
            }
        };

        let response = process_request(request, state, &mut ctx);

        if matches!(response, Response::ShuttingDown) {
            send_response(&mut writer, &response)?;
            break;
        }

        send_response(&mut writer, &response)?;
    }

    if let Some(mut session) = ctx.session.take() {
        let _ = session.abort_transaction();
    }

    Ok(())
}

fn process_request(
    request: Request,
    state: &Arc<SharedState>,
    ctx: &mut ClientContext,
) -> Response {
    match request {
        Request::Ping => Response::Pong,
        Request::Create(path) => handle_create(state, path),
        Request::Open(path) => handle_open(state, path),
        Request::Close => handle_close(state),
        Request::Sql(sql) => handle_sql(state, sql, ctx),
        Request::Explain(sql) => handle_explain(state, sql),
        Request::Vacuum => handle_vacuum(state),
        Request::Analyze { sample_rate, max_sample_rows } => {
            handle_analyze(state, sample_rate, max_sample_rows)
        }
        Request::Begin => handle_begin(state, ctx),
        Request::Commit => handle_commit(ctx),
        Request::Rollback => handle_rollback(ctx),
        Request::Shutdown => {
            state.shutdown.store(true, Ordering::Relaxed);
            Response::ShuttingDown
        }
    }
}

fn handle_begin(state: &SharedState, ctx: &mut ClientContext) -> Response {
    if ctx.session.is_some() {
        return Response::Error("Transaction already in progress".into());
    }

    let db_guard = state.db.read();
    let db = match db_guard.as_ref() {
        Some(db) => db,
        None => return Response::Error("No database open".into()),
    };

    match db.session() {
        Ok(session) => {
            ctx.session = Some(session);
            Response::SessionStarted
        }
        Err(e) => Response::Error(format!("Failed to create session: {}", e)),
    }
}

fn handle_commit(ctx: &mut ClientContext) -> Response {
    let Some(mut session) = ctx.session.take() else {
        return Response::Error("No active transaction".into());
    };

    match session.commit_transaction() {
        Ok(_) => Response::SessionEnd,
        Err(e) => Response::Error(format!("COMMIT failed: {}", e)),
    }
}

fn handle_rollback(ctx: &mut ClientContext) -> Response {
    let Some(mut session) = ctx.session.take() else {
        return Response::Error("No active transaction".into());
    };

    match session.abort_transaction() {
        Ok(_) => Response::SessionEnd,
        Err(e) => Response::Error(format!("ROLLBACK failed: {}", e)),
    }
}

fn handle_sql(state: &SharedState, sql: String, ctx: &mut ClientContext) -> Response {
    // If client has an active session, use it (session holds its own references)
    if let Some(session) = ctx.session.as_mut() {
        return match session.execute(&sql) {
            Ok(res) => query_result_to_response(res),
            Err(e) => Response::Error(format!("Query failed: {}", e)),
        };
    }

    // Otherwise, execute in auto-commit mode using the shared database
    let db_guard = state.db.read();
    let db = match db_guard.as_ref() {
        Some(db) => db,
        None => return Response::Error("No database open".into()),
    };

    match db.execute(&sql) {
        Ok(res) => query_result_to_response(res),
        Err(e) => Response::Error(format!("Query failed: {}", e)),
    }
}

fn handle_create(state: &SharedState, path: String) -> Response {
    let mut db_guard = state.db.write();

    if db_guard.is_some() {
        return Response::Error("Database already open. Use CLOSE first.".into());
    }

    let db_path = PathBuf::from(&path);
    match Database::create(&db_path, state.db_config) {
        Ok(db) => {
            *db_guard = Some(db);
            Response::Ok(format!("Database created: {}", path))
        }
        Err(e) => Response::Error(format!("Failed to create database: {}", e)),
    }
}

fn handle_open(state: &SharedState, path: String) -> Response {
    let mut db_guard = state.db.write();

    if db_guard.is_some() {
        return Response::Error("Database already open. Use CLOSE first.".into());
    }

    let db_path = PathBuf::from(&path);
    match Database::open_or_create(&db_path, state.db_config) {
        Ok(db) => {
            *db_guard = Some(db);
            Response::Ok(format!("Database opened: {}", path))
        }
        Err(e) => Response::Error(format!("Failed to open database: {}", e)),
    }
}

fn handle_explain(state: &SharedState, sql: String) -> Response {
    let db_guard = state.db.read();
    let db = match db_guard.as_ref() {
        Some(db) => db,
        None => return Response::Error("No database open".into()),
    };

    match db.explain(&sql) {
        Ok(plan) => Response::Explain(plan),
        Err(e) => Response::Error(format!("EXPLAIN failed: {}", e)),
    }
}

fn handle_vacuum(state: &SharedState) -> Response {
    let db_guard = state.db.read();
    let db = match db_guard.as_ref() {
        Some(db) => db,
        None => return Response::Error("No database open".into()),
    };

    match db.vacuum() {
        Ok(stats) => Response::VacuumComplete {
            tables_vacuumed: stats.tables_vacuumed,
            bytes_freed: stats.total_bytes_freed,
            transactions_cleaned: stats.transactions_cleaned,
        },
        Err(e) => Response::Error(format!("VACUUM failed: {}", e)),
    }
}

fn handle_analyze(state: &SharedState, sample_rate: f64, max_sample_rows: usize) -> Response {
    let db_guard = state.db.read();
    let db = match db_guard.as_ref() {
        Some(db) => db,
        None => return Response::Error("No database open".into()),
    };

    match db.analyze(sample_rate, max_sample_rows) {
        Ok(_) => Response::Ok("ANALYZE complete".into()),
        Err(e) => Response::Error(format!("ANALYZE failed: {}", e)),
    }
}

fn handle_close(state: &SharedState) -> Response {
    let mut db_guard = state.db.write();

    if db_guard.take().is_some() {
        Response::Goodbye
    } else {
        Response::Error("No database open".into())
    }
}

fn query_result_to_response(result: QueryResult) -> Response {
    match result {
        QueryResult::Rows(rows) => {
            let columns = if rows.is_empty() {
                vec![]
            } else {
                (0..rows.num_columns())
                    .map(|i| rows.column(i).expect("Column not found").to_string())
                    .collect()
            };

            let data: Vec<Vec<String>> = rows
                .iterrows()
                .map(|row| row.iter().map(|v| v.to_string()).collect())
                .collect();

            Response::Rows { columns, data }
        }
        QueryResult::RowsAffected(count) => Response::RowsAffected(count),
        QueryResult::Ddl(outcome) => {
            let msg = format!("{:?}", outcome);
            Response::Ddl(msg)
        }
    }
}

fn main() {
    let config = match ServerConfig::from_args() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {}", e);
            eprintln!();
            print_usage();
            process::exit(1);
        }
    };

    let mut server = match Server::new(config) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to start server: {}", e);
            process::exit(1);
        }
    };

    server.run();
}
