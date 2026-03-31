# Broadcast Server

CLI websocket broadcast server and client implemented in Go for the roadmap.sh project:

https://roadmap.sh/projects/broadcast-server

## Requirements

- Go 1.24+

## Run

Start the server:

```bash
go run . start
```

Connect a client:

```bash
go run . connect
```

Connect a named client:

```bash
go run . connect -name alice
```

Use a custom address or websocket path:

```bash
go run . start -addr localhost:9000 -path /ws
go run . connect -addr localhost:9000 -path /ws -name bob
```

## How It Works

- `start` launches an HTTP server with a websocket endpoint.
- `connect` opens a terminal client that sends stdin lines to the server.
- Any text message received by the server is broadcast to every connected client.
- Client joins, disconnects, and server shutdown are handled gracefully.

## Build

```bash
go build -o broadcast-server .
```

Then run:

```bash
./broadcast-server start
./broadcast-server connect -name alice
```
