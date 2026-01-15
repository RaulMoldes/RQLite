# AxmosDB Installation Guide

This guide explains how to download, install, and run AxmosDB from the official GitHub releases.

## System Requirements

- Linux x86_64 (other platforms coming soon)
- No additional dependencies required (statically linked binaries)

## Download

Download the latest release from [GitHub Releases](https://github.com/RaulMoldes/AxmosDB/releases).

For version 0.1.0 on Linux x86_64:

```bash
# Download server
curl -LO https://github.com/RaulMoldes/AxmosDB/releases/download/v0.1.0/axmos-server-v0.1.0-linux-x86_64.tar.gz

# Download client
curl -LO https://github.com/RaulMoldes/AxmosDB/releases/download/v0.1.0/axmos-client-v0.1.0-linux-x86_64.tar.gz

# Download checksums
curl -LO https://github.com/RaulMoldes/AxmosDB/releases/download/v0.1.0/checksums.txt
```

## Verify Downloads (Optional)

Verify the integrity of downloaded files using SHA256 checksums:

```bash
# Verify server
sha256sum -c checksums.txt --ignore-missing
```

Or manually:

```bash
sha256sum axmos-server-v0.1.0-linux-x86_64.tar.gz
sha256sum axmos-client-v0.1.0-linux-x86_64.tar.gz
# Compare output with checksums.txt
```

## Installation

Extract the binaries:

```bash
# Extract server
tar -xzf axmos-server-v0.1.0-linux-x86_64.tar.gz

# Extract client
tar -xzf axmos-client-v0.1.0-linux-x86_64.tar.gz
```

Optionally, move binaries to a directory in your PATH:

```bash
sudo mv axmos-server-v0.1.0-linux-x86_64/axmos-server /usr/local/bin/
sudo mv axmos-client-v0.1.0-linux-x86_64/axmos-client /usr/local/bin/
```

## Running the Server

Start the server with default settings (port 5432):

```bash
./axmos-server-v0.1.0-linux-x86_64/axmos-server
```

Or if installed to PATH:

```bash
axmos-server
```

### Server Options

```
Usage: axmos-server [OPTIONS]

Options:
  -p, --port <PORT>         Port to listen on (default: 5432)
  -f, --file <PATH>         Database file to open on startup

Database Configuration:
  --page-size <SIZE>        Page size in bytes (default: 4096)
  --cache-size <PAGES>      Cache size in pages (default: 10000)
  --pool-size <N>           Thread pool size (default: num CPUs)
  --min-keys <N>            Minimum keys per B+tree page (default: 3)
  --siblings <N>            Siblings per side for balancing (default: 2)

  -v, --version             Show version information
  -h, --help                Show help message
```

### Examples

Start server on a custom port:

```bash
axmos-server -p 3000
```

Start server with a pre-existing database:

```bash
axmos-server -f /path/to/mydb.axm
```

Start server with custom configuration:

```bash
axmos-server -p 5432 --cache-size 20000 --page-size 8192
```

## Running the Client

Connect to a running server:

```bash
./axmos-client-v0.1.0-linux-x86_64/axmos-client
```

Or if installed to PATH:

```bash
axmos-client
```

### Client Options

```
Usage: axmos-client [OPTIONS]

Options:
  -h, --host <HOST>    Server host (default: 127.0.0.1)
  -p, --port <PORT>    Server port (default: 5432)
  --help               Show help message
```

### Examples

Connect to a server on a custom port:

```bash
axmos-client -p 3000
```

Connect to a remote server:

```bash
axmos-client -h 192.168.1.100 -p 5432
```

## Quick Start Example

1. Start the server in one terminal:

```bash
axmos-server -p 5432
```

2. Connect with the client in another terminal:

```bash
axmos-client -p 5432
```

3. Create a new database:

```
axmos> CREATEDB mydb.axm
```

4. Create a table and insert data:

```sql
axmos> CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT);
axmos> INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
axmos> INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');
axmos> SELECT * FROM users;
```

5. Work with transactions:

```
axmos> BEGIN
axmos*> INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com');
axmos*> COMMIT
```

## Available Client Commands

| Command | Description |
|---------|-------------|
| `CREATEDB <path>` | Create a new database file |
| `OPENDB <path>` | Open an existing database |
| `CLOSE` | Close current database connection |
| `BEGIN` | Start a transaction |
| `COMMIT` | Commit current transaction |
| `ROLLBACK` | Rollback current transaction |
| `EXPLAIN <query>` | Show query execution plan |
| `ANALYZE [rate] [max_rows]` | Update table statistics |
| `VACUUM` | Clean up old MVCC versions |
| `PING` | Check server health |
| `QUIT` or `EXIT` | Disconnect and exit |
| `SHUTDOWN` | Stop the server |
| `HELP` | Show available commands |

Any other input is treated as a SQL statement.

## Files Created

When you create a database, AxmosDB generates the following files:

- `<name>.axm` - Main database file
- `axmos.log` - Write-ahead log for crash recovery

## Troubleshooting

### Connection refused

Make sure the server is running and listening on the correct port:

```bash
# Check if server is running
ps aux | grep axmos-server

# Check if port is in use
netstat -tlnp | grep 5432
```

### Permission denied

Make the binary executable:

```bash
chmod +x axmos-server
chmod +x axmos-client
```

### Port already in use

Use a different port:

```bash
axmos-server -p 5433
axmos-client -p 5433
```

## Uninstallation

Remove the binaries:

```bash
sudo rm /usr/local/bin/axmos-server
sudo rm /usr/local/bin/axmos-client
```

Remove downloaded archives:

```bash
rm -rf axmos-server-v0.1.0-linux-x86_64*
rm -rf axmos-client-v0.1.0-linux-x86_64*
rm checksums.txt
```
