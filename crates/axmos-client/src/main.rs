mod protocol;

use protocol::{SqlRequest, SqlResponse};
use std::{
    io::{self, BufRead, BufReader, Write},
    net::TcpStream,
};

fn main() {
    println!("Connecting to DB server at 127.0.0.1:7878...");

    let stream = match TcpStream::connect("127.0.0.1:7878") {
        Ok(s) => {
            println!("Connected.\nType SQL queries or :quit");
            s
        }
        Err(e) => {
            eprintln!("Failed to connect: {e}");
            return;
        }
    };

    let mut writer = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to clone stream: {e}");
            return;
        }
    };
    let mut reader = BufReader::new(stream);
    let stdin = io::stdin();

    loop {
        print!("db> ");
        if io::stdout().flush().is_err() {
            break;
        }

        let mut input = String::new();
        if stdin.read_line(&mut input).is_err() {
            eprintln!("Failed to read input");
            break;
        }

        let input = input.trim();
        if input == ":quit" {
            println!("Bye.");
            break;
        }

        if input.is_empty() {
            continue;
        }

        let request = SqlRequest {
            sql: input.to_string(),
        };

        let json = match serde_json::to_string(&request) {
            Ok(j) => j,
            Err(e) => {
                eprintln!("Failed to serialize request: {e}");
                continue;
            }
        };

        let framed = format!("{json}\n");

        // Send to server, handle broken pipe
        if let Err(e) = writer.write_all(framed.as_bytes()) {
            eprintln!("Connection lost: {e}");
            break;
        }

        // Read server response
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => {
                println!("Server closed connection.");
                break;
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("Connection error: {e}");
                break;
            }
        }

        if line.trim().is_empty() {
            println!("-> <empty response>");
            continue;
        }

        match serde_json::from_str::<SqlResponse>(&line) {
            Ok(resp) => {
                // Check if server is shutting down
                if resp.result.contains("server shutting down") {
                    println!("Server is shutting down. Disconnecting.");
                    break;
                }
                println!("{}", resp.result)
            }
            Err(err) => println!("Invalid JSON: {err}. Raw: {}", line.trim()),
        }
    }
}
