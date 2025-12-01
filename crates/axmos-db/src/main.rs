use std::{
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use axmosdb::sql::parse_sql;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Request {
    sql: String,
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();
    println!("AxmosDB server running on 127.0.0.1:7878");

    // Shared shutdown flag
    let shutdown = Arc::new(AtomicBool::new(false));

    // Handle Ctrl+C
    let shutdown_signal = shutdown.clone();
    ctrlc::set_handler(move || {
        println!("\nShutdown signal received, closing connections...");
        shutdown_signal.store(true, Ordering::SeqCst);
    })
    .expect("Failed to set Ctrl+C handler");

    // Set listener to non-blocking so we can check shutdown flag
    listener.set_nonblocking(true).unwrap();

    loop {
        if shutdown.load(Ordering::SeqCst) {
            println!("Server shutting down.");
            break;
        }

        match listener.accept() {
            Ok((stream, addr)) => {
                println!("New connection from {addr}");
                let shutdown_clone = shutdown.clone();
                std::thread::spawn(move || handle_connection(stream, shutdown_clone));
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No pending connection, sleep briefly and check shutdown
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => eprintln!("Incoming connection failed: {e:?}"),
        }
    }
}

fn handle_connection(stream: TcpStream, shutdown: Arc<AtomicBool>) {
    let mut writer = stream.try_clone().expect("Failed to clone stream");
    let reader_stream = stream;

    // Set read timeout so we can periodically check shutdown flag
    reader_stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();

    let mut reader = BufReader::new(reader_stream);

    loop {
        // Check if server is shutting down
        if shutdown.load(Ordering::SeqCst) {
            let _ = writer.write_all(b"{\"error\":\"server shutting down\"}\n");
            println!("Connection closed due to shutdown");
            break;
        }

        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => break, // Client disconnected
            Ok(_) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => continue,
            Err(_) => break,
        }

        if line.trim().is_empty() {
            continue;
        }

        println!("RAW MESSAGE: {}", line.trim());

        let parsed: Request = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                let _ = writer.write_all(b"{\"error\":\"invalid JSON\"}\n");
                eprintln!("Invalid JSON: {e:?}");
                continue;
            }
        };

        println!("Parsed SQL: {}", parsed.sql);

        let result = handle_sql(&parsed.sql);
        let response = format!("{{\"result\": \"{result}\"}}\n");
        let _ = writer.write_all(response.as_bytes());
    }
}

fn handle_sql(query: &str) -> String {
    match parse_sql(query) {
        Ok(stmt) => stmt.to_string(),
        Err(e) => format!("ERROR: {e}"),
    }
}
