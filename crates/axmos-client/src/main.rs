//! AxmosDB Client

use axmosdb::tcp::{Request, Response, TcpError, recv_response, send_request};

use std::{
    env,
    io::{self, BufRead, BufReader, BufWriter, Write},
    net::TcpStream,
    process,
    time::Instant,
};

struct ClientConfig {
    host: String,
    port: u16,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 5432,
        }
    }
}

impl ClientConfig {
    fn from_args() -> Result<Self, String> {
        let args: Vec<String> = env::args().collect();
        let mut config = Self::default();

        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "-h" | "--host" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("Missing host value".into());
                    }
                    config.host = args[i].clone();
                }
                "-p" | "--port" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("Missing port value".into());
                    }
                    config.port = args[i].parse().map_err(|_| "Invalid port number")?;
                }
                "--help" => {
                    print_usage();
                    process::exit(0);
                }
                arg => {
                    return Err(format!("Unknown argument: {}", arg));
                }
            }
            i += 1;
        }

        Ok(config)
    }

    fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

fn print_usage() {
    eprintln!("AxmosDB Client v0.1.0");
    eprintln!();
    eprintln!("Usage: axmos-client [OPTIONS]");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  -h, --host <HOST>    Server host (default: 127.0.0.1)");
    eprintln!("  -p, --port <PORT>    Server port (default: 5432)");
    eprintln!("  --help               Show this help message");
    eprintln!();
    eprintln!("Interactive Commands:");
    eprintln!("  CREATEDB <path>              Create a new database");
    eprintln!("  OPENDB <path>                Open an existing database");
    eprintln!("  CLOSE                        Close current database");
    eprintln!("  BEGIN                        Start a transaction (use with session mode)");
    eprintln!("  COMMIT                       Commit transaction");
    eprintln!("  ROLLBACK                     Rollback transaction");
    eprintln!("  EXPLAIN <query>              Show query execution plan");
    eprintln!("  ANALYZE [rate] [max_rows]    Update table statistics");
    eprintln!("  VACUUM [FORCE]               Clean up old MVCC versions");
    eprintln!("  PING                         Check server health");
    eprintln!("  QUIT, EXIT                   Disconnect and exit");
    eprintln!("  SHUTDOWN                     Shutdown the server");
    eprintln!("  <sql>                        Execute SQL statement");
    eprintln!();
    eprintln!("Session Mode:");
    eprintln!("  Use BEGIN to start a transaction. All subsequent SQL");
    eprintln!("  statements run in session context until COMMIT or ROLLBACK.");
}

struct Client {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
    in_transaction: bool,
}

impl Client {
    fn connect(config: &ClientConfig) -> Result<Self, String> {
        let addr = config.address();
        let stream = TcpStream::connect(&addr)
            .map_err(|e| format!("Failed to connect to {}: {}", addr, e))?;

        let reader = BufReader::new(stream.try_clone().map_err(|e| e.to_string())?);
        let writer = BufWriter::new(stream);

        Ok(Self {
            reader,
            writer,
            in_transaction: false,
        })
    }

    fn send(&mut self, request: Request) -> Result<Response, TcpError> {
        send_request(&mut self.writer, &request)?;
        recv_response(&mut self.reader)
    }

    fn run_interactive(&mut self) {
        let stdin = io::stdin();
        let mut stdout = io::stdout();

        loop {
            // Print prompt with transaction indicator
            if self.in_transaction {
                print!("axmos*> ");
            } else {
                print!("axmos> ");
            }
            stdout.flush().ok();

            let mut line = String::new();
            match stdin.lock().read_line(&mut line) {
                Ok(0) => {
                    println!();
                    break;
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Read error: {}", e);
                    break;
                }
            }

            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            match self.execute_command(line) {
                CommandResult::Continue => {}
                CommandResult::Exit => break,
                CommandResult::ServerShutdown => {
                    println!("Server is shutting down.");
                    break;
                }
            }
        }
    }

    fn execute_command(&mut self, input: &str) -> CommandResult {
        let parts: Vec<&str> = input.splitn(2, char::is_whitespace).collect();
        let cmd = parts[0].to_uppercase();
        let arg = parts.get(1).map(|s| s.trim()).unwrap_or("");

        // Handle local commands
        match cmd.as_str() {
            "QUIT" | "EXIT" | "\\Q" => {
                return CommandResult::Exit;
            }
            "HELP" | "\\?" => {
                print_interactive_help();
                return CommandResult::Continue;
            }
            _ => {}
        }

        // Build request
        let request = match cmd.as_str() {
            "CREATEDB" => {
                if arg.is_empty() {
                    eprintln!("Usage: CREATEDB <database_path>");
                    return CommandResult::Continue;
                }
                Request::Create(arg.to_string())
            }
            "OPENDB" => {
                if arg.is_empty() {
                    eprintln!("Usage: OPENDB <database_path>");
                    return CommandResult::Continue;
                }
                Request::Open(arg.to_string())
            }
            "CLOSE" => Request::Close,
            "PING" => Request::Ping,
            "SHUTDOWN" => Request::Shutdown,
            "EXPLAIN" => {
                if arg.is_empty() {
                    eprintln!("Usage: EXPLAIN <sql_query>");
                    return CommandResult::Continue;
                }
                Request::Explain(arg.to_string())
            }
            "ANALYZE" => {
                let (sample_rate, max_rows) = parse_analyze_args(arg);
                Request::Analyze {
                    sample_rate,
                    max_sample_rows: max_rows,
                }
            }
            "VACUUM" => {
                let force = arg.to_uppercase() == "FORCE";
                Request::Vacuum { force }
            }
            _ => {
                // SQL statement - use session context if in transaction or starting one
                let upper = input.trim().to_uppercase();
                if upper == "BEGIN" || self.in_transaction {
                    Request::SessionSql(input.to_string())
                } else {
                    Request::Sql(input.to_string())
                }
            }
        };

        let start = Instant::now();
        match self.send(request) {
            Ok(response) => {
                let elapsed = start.elapsed();
                self.print_response(response, elapsed)
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                CommandResult::Continue
            }
        }
    }

    fn print_response(&mut self, response: Response, elapsed: std::time::Duration) -> CommandResult {
        match response {
            Response::Ok(msg) => {
                println!("{}", msg);
                println!("({:.3}s)", elapsed.as_secs_f64());
            }
            Response::Error(msg) => {
                eprintln!("ERROR: {}", msg);
            }
            Response::Rows { columns, data } => {
                if data.is_empty() {
                    println!("(0 rows)");
                } else {
                    print_table(&columns, &data);
                    println!(
                        "({} row{}, {:.3}s)",
                        data.len(),
                        if data.len() == 1 { "" } else { "s" },
                        elapsed.as_secs_f64()
                    );
                }
            }
            Response::RowsAffected(count) => {
                println!(
                    "{} row{} affected ({:.3}s)",
                    count,
                    if count == 1 { "" } else { "s" },
                    elapsed.as_secs_f64()
                );
            }
            Response::Ddl(msg) => {
                // Detect transaction state changes from DDL response
                if msg.contains("TransactionStarted") {
                    self.in_transaction = true;
                    println!("BEGIN");
                } else if msg.contains("TransactionCommitted") {
                    self.in_transaction = false;
                    println!("COMMIT");
                } else if msg.contains("TransactionRolledBack") {
                    self.in_transaction = false;
                    println!("ROLLBACK");
                } else {
                    println!("{}", msg);
                }
                println!("({:.3}s)", elapsed.as_secs_f64());
            }
            Response::Explain(plan) => {
                println!("QUERY PLAN");
                println!("{}", "-".repeat(60));
                println!("{}", plan);
                println!("{}", "-".repeat(60));
                println!("({:.3}s)", elapsed.as_secs_f64());
            }
            Response::VacuumComplete {
                tables_vacuumed,
                bytes_freed,
                transactions_cleaned,
            } => {
                println!("VACUUM completed successfully");
                println!("  Tables vacuumed: {}", tables_vacuumed);
                println!("  Bytes freed: {}", bytes_freed);
                println!("  Transactions cleaned: {}", transactions_cleaned);
                println!("({:.3}s)", elapsed.as_secs_f64());
            }
            Response::Pong => {
                println!("PONG ({:.3}s)", elapsed.as_secs_f64());
            }
            Response::Goodbye => {
                println!("Connection closed.");
                return CommandResult::Exit;
            }
            Response::ShuttingDown => {
                return CommandResult::ServerShutdown;
            }
        }
        CommandResult::Continue
    }
}

enum CommandResult {
    Continue,
    Exit,
    ServerShutdown,
}

fn parse_analyze_args(arg: &str) -> (f64, usize) {
    let parts: Vec<&str> = arg.split_whitespace().collect();
    let sample_rate = parts.get(0).and_then(|s| s.parse().ok()).unwrap_or(1.0);
    let max_rows = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(10000);
    (sample_rate, max_rows)
}

fn print_interactive_help() {
    println!("Available commands:");
    println!("  CREATEDB <path>              Create a new database");
    println!("  OPENDB <path>                Open an existing database");
    println!("  CLOSE                        Close current database connection");
    println!("  BEGIN                        Start a transaction");
    println!("  COMMIT                       Commit current transaction");
    println!("  ROLLBACK                     Rollback current transaction");
    println!("  EXPLAIN <query>              Show query execution plan");
    println!("  ANALYZE [rate] [max_rows]    Update table statistics");
    println!("  VACUUM [FORCE]               Clean up old MVCC versions");
    println!("  PING                         Check server health");
    println!("  QUIT, EXIT, \\q               Disconnect and exit");
    println!("  HELP, \\?                     Show this help");
    println!("  SHUTDOWN                     Shutdown the server (admin)");
    println!();
    println!("Any other input is treated as a SQL statement.");
    println!();
    println!("Session Mode:");
    println!("  Use BEGIN to start a transaction. The prompt changes to 'axmos*>'");
    println!("  All SQL runs in the session until COMMIT or ROLLBACK.");
}

fn print_table(columns: &[String], data: &[Vec<String>]) {
    if columns.is_empty() {
        return;
    }

    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in data {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    print_row(columns, &widths);
    print_separator(&widths);

    for row in data {
        print_row(row, &widths);
    }
}

fn print_row(cells: &[String], widths: &[usize]) {
    print!("|");
    for (i, cell) in cells.iter().enumerate() {
        let width = widths.get(i).copied().unwrap_or(cell.len());
        print!(" {:width$} |", cell, width = width);
    }
    println!();
}

fn print_separator(widths: &[usize]) {
    print!("+");
    for width in widths {
        print!("{:-<width$}--+", "", width = width);
    }
    println!();
}

fn main() {
    let config = match ClientConfig::from_args() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {}", e);
            eprintln!();
            print_usage();
            process::exit(1);
        }
    };

    println!("AxmosDB Client v0.1.0");
    println!("Connecting to {}...", config.address());

    let mut client = match Client::connect(&config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{}", e);
            process::exit(1);
        }
    };

    println!("Connected. Type 'HELP' for available commands.");
    println!();

    client.run_interactive();

    println!("Goodbye!");
}
