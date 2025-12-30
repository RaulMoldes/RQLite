//! AxmosDB Client
//!
//! Command-line client for connecting to an AxmosDB server.
//!
//! Usage:
//!   axmos-client -h <host> -p <port>
//!
//! Interactive commands:
//!   CREATEDB <path>     - Create a new database
//!   OPENDB <path>       - Open an existing database
//!   CLOSE             - Close current database
//!   EXPLAIN <query>   - Show query execution plan
//!   ANALYZE [rate] [max_rows] - Update statistics
//!   PING              - Check server health
//!   QUIT / EXIT       - Disconnect and exit
//!   SHUTDOWN          - Shutdown the server
//!   <sql>             - Execute SQL query

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
    eprintln!("  CLOSE                      Close current database");
    eprintln!("  EXPLAIN <query>            Show query execution plan");
    eprintln!("  ANALYZE [rate] [max_rows]  Update table statistics");
    eprintln!("  PING                       Check server health");
    eprintln!("  QUIT, EXIT                 Disconnect and exit");
    eprintln!("  SHUTDOWN                   Shutdown the server");
    eprintln!("  <sql>                      Execute SQL statement");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  axmos-client -h localhost -p 5433");
    eprintln!("  axmos-client --port 8080");
}

struct Client {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl Client {
    fn connect(config: &ClientConfig) -> Result<Self, String> {
        let addr = config.address();
        let stream = TcpStream::connect(&addr)
            .map_err(|e| format!("Failed to connect to {}: {}", addr, e))?;

        let reader = BufReader::new(stream.try_clone().map_err(|e| e.to_string())?);
        let writer = BufWriter::new(stream);

        Ok(Self { reader, writer })
    }

    fn send(&mut self, request: Request) -> Result<Response, TcpError> {
        send_request(&mut self.writer, &request)?;
        recv_response(&mut self.reader)
    }

    fn run_interactive(&mut self) {
        let stdin = io::stdin();
        let mut stdout = io::stdout();

        loop {
            // Print prompt
            print!("axmos> ");
            stdout.flush().ok();

            // Read line
            let mut line = String::new();
            match stdin.lock().read_line(&mut line) {
                Ok(0) => {
                    // EOF
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

            // Parse and execute command
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
        let upper = input.to_uppercase();
        let parts: Vec<&str> = upper.splitn(2, char::is_whitespace).collect();
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
            _ => {
                // Assume it's SQL
                Request::Sql(input.to_string())
            }
        };

        // Send request and handle response
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

    fn print_response(&self, response: Response, elapsed: std::time::Duration) -> CommandResult {
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
                println!("{}", msg);
                println!("({:.3}s)", elapsed.as_secs_f64());
            }
            Response::Explain(plan) => {
                println!("QUERY PLAN");
                println!("{}", "-".repeat(60));
                println!("{}", plan);
                println!("{}", "-".repeat(60));
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
    println!("  CREATE <path>              Create a new database");
    println!("  OPEN <path>                Open an existing database");
    println!("  CLOSE                      Close current database connection");
    println!("  EXPLAIN <query>            Show query execution plan");
    println!("  ANALYZE [rate] [max_rows]  Update table statistics");
    println!("  PING                       Check server health");
    println!("  QUIT, EXIT, \\q             Disconnect and exit");
    println!("  HELP, \\?                   Show this help");
    println!("  SHUTDOWN                   Shutdown the server (admin)");
    println!();
    println!("Any other input is treated as a SQL statement.");
}

fn print_table(columns: &[String], data: &[Vec<String>]) {
    if columns.is_empty() {
        return;
    }

    // Calculate column widths
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in data {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    // Print header
    print_row(columns, &widths);
    print_separator(&widths);

    // Print data rows
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
