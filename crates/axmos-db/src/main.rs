// src/main.rs

mod protocol;

use protocol::{SqlRequest, SqlResponse};
use std::{
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::Arc,
    thread,
    time::Instant,
};

use axmosdb::database::Database;

const DEFAULT_PORT: u16 = 5433;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let port = args
        .get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PORT);

    let db_path = args
        .get(2)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("axmosdb.db"));

    println!("╔═══════════════════════════════════════╗");
    println!("║         AxmosDB Server v0.1           ║");
    println!("╚═══════════════════════════════════════╝");
    println!();
    println!("Database: {}", db_path.display());
    println!("Port:     {}", port);
    println!();

    // Initialize database
    let db = match Database::with_defaults(&db_path) {
        Ok(db) => {
            println!("✓ Database initialized");
            Arc::new(db)
        }
        Err(e) => {
            eprintln!("✗ Failed to initialize database: {}", e);
            std::process::exit(1);
        }
    };

    // Start TCP listener
    let addr = format!("0.0.0.0:{}", port);
    let listener = match TcpListener::bind(&addr) {
        Ok(l) => {
            println!("✓ Listening on {}", addr);
            l
        }
        Err(e) => {
            eprintln!("✗ Failed to bind: {}", e);
            std::process::exit(1);
        }
    };

    println!();
    println!("Ready for connections...");
    println!();

    // Accept connections
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    if let Err(e) = handle_client(stream, &db) {
                        eprintln!("Client error: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
}



fn handle_client(stream: TcpStream, db: &Database) -> std::io::Result<()> {
    let peer_addr = stream.peer_addr()?;
    println!("[+] Client connected: {}", peer_addr);

    let mut writer = stream.try_clone()?;
    let reader = BufReader::new(stream);

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                eprintln!("[{}] Read error: {}", peer_addr, e);
                break;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        // Parse JSON request
        let request: SqlRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                let response = SqlResponse::error(format!("Invalid JSON: {}", e));
                send_response(&mut writer, &response)?;
                continue;
            }
        };

        println!("[{}] Query: {}", peer_addr, request.sql);

        // Execute query
        let start = Instant::now();
        let response = match db.run_query(request.sql) {
            Ok(rows) => {
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                let row_count = rows.len();

                let result = if rows.is_empty() {
                    "OK".to_string()
                } else {
                    rows.iter()
                        .map(|r| r.to_string())
                        .collect::<Vec<_>>()
                        .join("\n")
                };

                SqlResponse::success(result, row_count, elapsed)
            }
            Err(e) => {
                println!("[{}] Error: {}", peer_addr, e);
                SqlResponse::error(e.to_string())
            }
        };

        send_response(&mut writer, &response)?;
    }

    println!("[-] Client disconnected: {}", peer_addr);
    Ok(())
}

fn send_response(writer: &mut TcpStream, response: &SqlResponse) -> std::io::Result<()> {
    let json = serde_json::to_string(response).unwrap_or_else(|_| {
        r#"{"success":false,"result":"Failed to serialize response"}"#.to_string()
    });

    writer.write_all(format!("{}\n", json).as_bytes())?;
    writer.flush()
}
