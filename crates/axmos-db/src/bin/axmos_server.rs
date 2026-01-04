//! AxmosDB Server Binary

use axmosdb::{
    Database,
    common::DBConfig,
    io::pager::BtreeBuilder,
    runtime::QueryResult,
    sql::{binder::binder::Binder, parser::Parser, planner::CascadesOptimizer},
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

struct ServerConfig {
    port: u16,
    db_path: Option<PathBuf>,
}

impl ServerConfig {
    fn from_args() -> Result<Self, String> {
        let args: Vec<String> = env::args().collect();
        let mut port = 5432u16;
        let mut db_path = None;

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
                "-h" | "--help" => {
                    print_usage();
                    process::exit(0);
                }
                arg => {
                    return Err(format!("Unknown argument: {}", arg));
                }
            }
            i += 1;
        }

        Ok(Self { port, db_path })
    }
}

fn print_usage() {
    eprintln!("AxmosDB Server v0.1.0");
    eprintln!();
    eprintln!("Usage: axmos-server [OPTIONS]");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  -p, --port <PORT>    Port to listen on (default: 5432)");
    eprintln!("  -f, --file <PATH>    Database file to open on startup");
    eprintln!("  -h, --help           Show this help message");
}

struct Server {
    listener: TcpListener,
    db: Option<Database>,
    db_config: DBConfig,
    shutdown: Arc<AtomicBool>,
}

impl Server {
    fn new(config: ServerConfig) -> Result<Self, String> {
        let addr = format!("0.0.0.0:{}", config.port);
        let listener =
            TcpListener::bind(&addr).map_err(|e| format!("Failed to bind to {}: {}", addr, e))?;

        println!("╔════════════════════════════════════════╗");
        println!("║         AxmosDB Server v0.1.0          ║");
        println!("╚════════════════════════════════════════╝");
        println!();
        println!("Listening on: {}", addr);

        let db_config = DBConfig::default();
        let mut server = Self {
            listener,
            db: None,
            db_config,
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        if let Some(path) = config.db_path {
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
            println!("No database specified. Use CREATE or OPEN command.");
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
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(300)))
            .ok();
        stream
            .set_write_timeout(Some(std::time::Duration::from_secs(60)))
            .ok();

        let mut reader = BufReader::new(stream.try_clone()?);
        let mut writer = BufWriter::new(stream);

        // Create a session for this client connection
        let mut client_session: Option<Session> = self.db.as_ref().map(|db| db.session());

        loop {
            let request = match recv_request(&mut reader) {
                Ok(req) => req,
                Err(TcpError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(TcpError::Io(e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => {
                    let response = Response::Error(format!("Protocol error: {}", e));
                    send_response(&mut writer, &response)?;
                    continue;
                }
            };

            let request_type = match &request {
                Request::Create(_) => "CREATE",
                Request::Open(_) => "OPEN",
                Request::Sql(_) => "SQL",
                Request::SessionSql(_) => "SESSION SQL",
                Request::Explain(_) => "EXPLAIN",
                Request::Analyze { .. } => "ANALYZE",
                Request::Vacuum { force } => {
                    if *force { "VACUUM FORCE" } else { "VACUUM" }
                }
                Request::Close => "CLOSE",
                Request::Ping => "PING",
                Request::Shutdown => "SHUTDOWN",
            };
            println!("  -> {}", request_type);

            let response = self.process_request(request, &mut client_session);

            let should_close = matches!(response, Response::Goodbye | Response::ShuttingDown);

            send_response(&mut writer, &response)?;

            if should_close {
                break;
            }
        }

        // Session dropped here - any uncommitted transaction is rolled back
        Ok(())
    }

    fn process_request(
        &mut self,
        request: Request,
        client_session: &mut Option<Session>,
    ) -> Response {
        match request {
            Request::Create(path) => {
                let response = self.handle_create(path);
                // Recreate session for new database
                *client_session = self.db.as_ref().map(|db| db.session());
                response
            }
            Request::Open(path) => {
                let response = self.handle_open(path);
                // Recreate session for opened database
                *client_session = self.db.as_ref().map(|db| db.session());
                response
            }
            Request::Sql(sql) => self.handle_sql(sql),
            Request::SessionSql(sql) => self.handle_session_sql(sql, client_session),
            Request::Explain(sql) => self.handle_explain(sql),
            Request::Analyze {
                sample_rate,
                max_sample_rows,
            } => self.handle_analyze(sample_rate, max_sample_rows),
            Request::Vacuum { force } => self.handle_vacuum(force),
            Request::Close => {
                *client_session = None;
                self.handle_close()
            }
            Request::Ping => Response::Pong,
            Request::Shutdown => {
                self.shutdown.store(true, Ordering::Relaxed);
                Response::ShuttingDown
            }
        }
    }

    fn handle_session_sql(&mut self, sql: String, session: &mut Option<Session>) -> Response {
        let Some(session) = session.as_mut() else {
            return Response::Error("No database open. Use OPEN or CREATE first.".into());
        };

        match session.execute(&sql) {
            Ok(result) => query_result_to_response(result),
            Err(e) => Response::Error(format!("Query failed: {}", e)),
        }
    }

    fn handle_vacuum(&mut self, force: bool) -> Response {
        let Some(db) = &self.db else {
            return Response::Error("No database open. Use OPEN or CREATE first.".into());
        };

        let result = if force { db.vacuum() } else { db.vacuum_safe() };

        match result {
            Ok(stats) => Response::VacuumComplete {
                tables_vacuumed: stats.tables_vacuumed,
                bytes_freed: stats.total_freed(),
                transactions_cleaned: stats.transactions_cleaned,
            },
            Err(e) => Response::Error(format!("VACUUM failed: {}", e)),
        }
    }

    fn handle_create(&mut self, path: String) -> Response {
        let path = PathBuf::from(path);

        match Database::create(&path, self.db_config) {
            Ok(db) => {
                self.db = Some(db);
                Response::Ok(format!("Database created: {}", path.display()))
            }
            Err(e) => Response::Error(format!("Failed to create database: {}", e)),
        }
    }

    fn handle_open(&mut self, path: String) -> Response {
        let path = PathBuf::from(path);

        match Database::open(&path, self.db_config) {
            Ok(db) => {
                self.db = Some(db);
                Response::Ok(format!("Database opened: {}", path.display()))
            }
            Err(e) => Response::Error(format!("Failed to open database: {}", e)),
        }
    }

    fn handle_sql(&mut self, sql: String) -> Response {
        let Some(db) = &self.db else {
            return Response::Error("No database open. Use OPEN or CREATE first.".into());
        };

        match db.execute(&sql) {
            Ok(result) => query_result_to_response(result),
            Err(e) => Response::Error(format!("Query failed: {}", e)),
        }
    }

    fn handle_explain(&mut self, sql: String) -> Response {
        let Some(db) = &self.db else {
            return Response::Error("No database open. Use OPEN or CREATE first.".into());
        };

        let mut parser = Parser::new(&sql);
        let stmt = match parser.parse() {
            Ok(s) => s,
            Err(e) => return Response::Error(format!("Parse error: {}", e)),
        };

        let handle = match db.coordinator().begin() {
            Ok(h) => h,
            Err(e) => return Response::Error(format!("Transaction error: {}", e)),
        };

        let snapshot = handle.snapshot();

        let tree_builder = {
            let p = db.pager().read();
            BtreeBuilder::new(p.min_keys_per_page(), p.num_siblings_per_side())
                .with_pager(db.pager().clone())
        };

        let mut binder = Binder::new(db.catalog().clone(), tree_builder.clone(), snapshot.clone());
        let bound = match binder.bind(&stmt) {
            Ok(b) => b,
            Err(e) => return Response::Error(format!("Bind error: {}", e)),
        };

        let mut optimizer =
            CascadesOptimizer::with_defaults(db.catalog().clone(), tree_builder, snapshot);

        let plan = match optimizer.optimize(&bound) {
            Ok(p) => p,
            Err(e) => return Response::Error(format!("Planner error: {}", e)),
        };

        let _ = handle.abort();

        Response::Explain(plan.explain())
    }

    fn handle_analyze(&mut self, sample_rate: f64, max_sample_rows: usize) -> Response {
        let Some(db) = &self.db else {
            return Response::Error("No database open. Use OPEN or CREATE first.".into());
        };

        match db.analyze(sample_rate, max_sample_rows) {
            Ok(()) => Response::Ok("ANALYZE completed successfully".into()),
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
