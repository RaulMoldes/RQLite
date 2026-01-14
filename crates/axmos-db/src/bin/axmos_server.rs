//! AxmosDB Server Binary

use axmosdb::{
    Database, DatabaseError,
    common::DBConfig,
    runtime::QueryResult,
    tcp::{Request, Response, TcpError, recv_request, send_response, session::Session},
};

use std::{
    env,
    io::{BufReader, BufWriter},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

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

struct Server {
    listener: TcpListener,
    db: Option<Database>,
    db_config: DBConfig,
    shutdown: Arc<AtomicBool>,
}

impl Server {
    fn handle_begin(&mut self, ctx: &mut ClientContext) -> Response {
        if ctx.session.is_some() {
            return Response::Error("Transaction already in progress".into());
        }

        let db = match &self.db {
            Some(db) => db,
            None => return Response::Error("No database open".into()),
        };

        if let Ok(session) = db.session() {
            ctx.session = Some(session);
        } else {
            return Response::Error("Failed to create session".into());
        };

        Response::SessionStarted
    }

    fn handle_commit(&mut self, ctx: &mut ClientContext) -> Response {
        let Some(mut session) = ctx.session.take() else {
            return Response::Error("No active transaction".into());
        };

        match session.commit_transaction() {
            Ok(_) => Response::SessionEnd,
            Err(e) => Response::Error(format!("COMMIT failed: {}", e)),
        }
    }

    fn handle_rollback(&mut self, ctx: &mut ClientContext) -> Response {
        let Some(mut session) = ctx.session.take() else {
            return Response::Error("No active transaction".into());
        };

        match session.abort_transaction() {
            Ok(_) => Response::SessionEnd,
            Err(e) => Response::Error(format!("ROLLBACK failed: {}", e)),
        }
    }

    fn handle_sql(&mut self, sql: String, ctx: &mut ClientContext) -> Response {
        let db = match &self.db {
            Some(db) => db,
            None => return Response::Error("No database open".into()),
        };

        let result = match ctx.session.as_mut() {
            Some(session) => session.execute(&sql).map_err(|e| DatabaseError::from(e)),
            None => db.execute(&sql),
        };

        match result {
            Ok(res) => query_result_to_response(res),
            Err(e) => Response::Error(format!("Query failed: {}", e)),
        }
    }

    fn handle_create(&mut self, path: String) -> Response {
        if self.db.is_some() {
            return Response::Error("Database already open. Use CLOSE first.".into());
        }

        let db_path = PathBuf::from(&path);
        match Database::create(&db_path, self.db_config) {
            Ok(db) => {
                self.db = Some(db);
                Response::Ok(format!("Database created: {}", path))
            }
            Err(e) => Response::Error(format!("Failed to create database: {}", e)),
        }
    }

    fn handle_open(&mut self, path: String) -> Response {
        if self.db.is_some() {
            return Response::Error("Database already open. Use CLOSE first.".into());
        }

        let db_path = PathBuf::from(&path);
        match Database::open_or_create(&db_path, self.db_config) {
            Ok(db) => {
                self.db = Some(db);
                Response::Ok(format!("Database opened: {}", path))
            }
            Err(e) => Response::Error(format!("Failed to open database: {}", e)),
        }
    }

    fn handle_explain(&mut self, sql: String, _ctx: &ClientContext) -> Response {
        let db = match &self.db {
            Some(db) => db,
            None => return Response::Error("No database open".into()),
        };

        match db.explain(&sql) {
            Ok(plan) => Response::Explain(plan),
            Err(e) => Response::Error(format!("EXPLAIN failed: {}", e)),
        }
    }

    fn handle_vacuum(&mut self) -> Response {
        let db = match &mut self.db {
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

    fn handle_analyze(&mut self, sample_rate: f64, max_sample_rows: usize) -> Response {
        let db = match &mut self.db {
            Some(db) => db,
            None => return Response::Error("No database open".into()),
        };

        match db.analyze(sample_rate, max_sample_rows) {
            Ok(_) => Response::Ok("ANALYZE complete".into()),
            Err(e) => Response::Error(format!("ANALYZE failed: {}", e)),
        }
    }

    fn handle_close(&mut self) -> Response {
        if self.db.take().is_some() {
            Response::Goodbye
        } else {
            Response::Error("No database open".into())
        }
    }

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

        let mut server = Self {
            listener,
            db: None,
            db_config: config.db_config,
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        if let Some(path) = config.db_path {
            println!();
            println!("Opening database: {}", path.display());
            match Database::open_or_create(&path, server.db_config) {
                Ok(db) => {
                    server.db = Some(db);
                    println!("Database ready");
                }
                Err(e) => {
                    eprintln!("Warning: Failed to open database: {}", e);
                }
            }
        } else {
            println!();
            println!("No database specified. Use CREATEDB or OPENDB commands.");
        }

        println!();
        Ok(server)
    }

    fn run(&mut self) {
        println!("Ready to accept connections. Use SHUTDOWN command or Ctrl+C to stop.");
        println!();

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                println!("Shutdown requested...");
                break;
            }

            match self.listener.accept() {
                Ok((stream, addr)) => {
                    println!("[{}] Connected", addr);

                    if let Err(e) = self.handle_client(stream) {
                        eprintln!("Client error: {}", e);
                    }

                    println!("[{}] Disconnected", addr);
                }
                Err(e) => {
                    eprintln!("Accept error: {}", e);
                }
            }
        }

        if let Some(db) = self.db.take() {
            println!("Closing database...");
            drop(db);
        }

        println!("Server stopped.");
    }

    fn handle_client(&mut self, stream: TcpStream) -> Result<(), TcpError> {
        let mut ctx = ClientContext { session: None };

        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(300)))
            .ok();
        stream
            .set_write_timeout(Some(std::time::Duration::from_secs(60)))
            .ok();

        let mut reader = BufReader::new(stream.try_clone()?);
        let mut writer = BufWriter::new(stream);

        loop {
            let request = match recv_request(&mut reader) {
                Ok(req) => req,
                Err(TcpError::ConnectionClosed) => break,
                Err(e) => {
                    eprintln!("Request error: {}", e);
                    break;
                }
            };

            let response = match request {
                Request::Ping => Response::Pong,
                Request::Create(path) => self.handle_create(path),
                Request::Open(path) => self.handle_open(path),
                Request::Close => self.handle_close(),
                Request::Sql(sql) => self.handle_sql(sql, &mut ctx),
                Request::Explain(sql) => self.handle_explain(sql, &ctx),
                Request::Vacuum => self.handle_vacuum(),
                Request::Analyze {
                    sample_rate,
                    max_sample_rows,
                } => self.handle_analyze(sample_rate, max_sample_rows),
                Request::Begin => self.handle_begin(&mut ctx),
                Request::Commit => self.handle_commit(&mut ctx),
                Request::Rollback => self.handle_rollback(&mut ctx),
                Request::Shutdown => {
                    self.shutdown.store(true, Ordering::Relaxed);
                    send_response(&mut writer, &Response::ShuttingDown)?;
                    break;
                }
            };

            send_response(&mut writer, &response)?;
        }

        if let Some(mut session) = ctx.session.take() {
            let _ = session.abort_transaction();
        }

        Ok(())
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
