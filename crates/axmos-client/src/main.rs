// src/bin/client.rs

mod protocol;

use protocol::{SqlRequest, SqlResponse};
use std::{
    io::{self, BufRead, BufReader, Write},
    net::TcpStream,
};

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 5433;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let host = args.get(1).map(String::as_str).unwrap_or(DEFAULT_HOST);
    let port: u16 = args
        .get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PORT);

    let addr = format!("{}:{}", host, port);

    println!("Connecting to AxmosDB at {}...", addr);

    let stream = match TcpStream::connect(&addr) {
        Ok(s) => {
            println!("Connected successfully!");
            println!("Type SQL queries ending with ';' or :quit to exit\n");
            s
        }
        Err(e) => {
            eprintln!("Failed to connect: {}", e);
            return;
        }
    };

    let mut writer = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to clone stream: {}", e);
            return;
        }
    };

    let mut reader = BufReader::new(stream);
    let stdin = io::stdin();
    let mut query_buffer = String::new();

    loop {
        // Show appropriate prompt
        if query_buffer.is_empty() {
            print!("axmosdb> ");
        } else {
            print!("      -> ");
        }

        if io::stdout().flush().is_err() {
            break;
        }

        let mut input = String::new();
        if stdin.read_line(&mut input).is_err() {
            eprintln!("Failed to read input");
            break;
        }

        let trimmed = input.trim();

        // Handle special commands
        match trimmed.to_lowercase().as_str() {
            ":quit" | ":exit" | "\\q" => {
                println!("Goodbye!");
                break;
            }
            ":help" | "\\h" | "\\?" => {
                print_help();
                continue;
            }
            ":clear" | "\\c" => {
                query_buffer.clear();
                println!("Query buffer cleared.");
                continue;
            }
            "" => {
                continue;
            }
            _ => {}
        }

        // Accumulate query
        if !query_buffer.is_empty() {
            query_buffer.push(' ');
        }
        query_buffer.push_str(trimmed);

        // Check if query is complete (ends with semicolon)
        if !query_buffer.ends_with(';') {
            continue;
        }

        // Remove trailing semicolon
        let query = query_buffer.trim_end_matches(';').trim().to_string();
        query_buffer.clear();

        if query.is_empty() {
            continue;
        }

        // Send request
        let request = SqlRequest { sql: query };

        let json = match serde_json::to_string(&request) {
            Ok(j) => j,
            Err(e) => {
                eprintln!("Failed to serialize request: {}", e);
                continue;
            }
        };

        if let Err(e) = writer.write_all(format!("{}\n", json).as_bytes()) {
            eprintln!("Connection lost: {}", e);
            break;
        }

        if let Err(e) = writer.flush() {
            eprintln!("Failed to flush: {}", e);
            break;
        }

        // Read response
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => {
                println!("Server closed connection.");
                break;
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("Connection error: {}", e);
                break;
            }
        }

        if line.trim().is_empty() {
            println!("(empty response)");
            continue;
        }

        match serde_json::from_str::<SqlResponse>(&line) {
            Ok(resp) => {
                if resp.success {
                    println!("{}", resp.result);
                    if let Some(rows) = resp.row_count {
                        println!("({} rows)", rows);
                    }
                    if let Some(elapsed) = resp.elapsed_ms {
                        println!("Time: {:.2}ms", elapsed);
                    }
                } else {
                    println!("ERROR: {}", resp.result);
                }
            }
            Err(e) => {
                eprintln!("Invalid response: {}. Raw: {}", e, line.trim());
            }
        }

        println!();
    }
}

fn print_help() {
    println!(
        r#"
AxmosDB Client Commands:
========================
  :quit, :exit, \q    Exit the client
  :help, \h, \?       Show this help
  :clear, \c          Clear the query buffer

SQL Examples:
  CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);
  INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
  SELECT * FROM users;
  SELECT * FROM users WHERE age > 25;
  UPDATE users SET age = 31 WHERE name = 'Alice';
  DELETE FROM users WHERE id = 1;

Tips:
  - End queries with semicolon (;)
  - Multi-line queries are supported
"#
    );
}
