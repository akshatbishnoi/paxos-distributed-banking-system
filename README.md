# Paxos-Based Distributed Banking System

This project implements a distributed banking system in Go using a Paxos-style consensus workflow to coordinate replicated state across multiple servers.

It was built as a distributed systems project and focuses on:

- replicated transaction processing across multiple servers
- ballot-based Paxos coordination for agreement on log state
- gRPC communication between replicas
- transaction queuing and replay during partial availability
- batch-driven testing using CSV transaction inputs

## Architecture

- 5 server replicas
- gRPC services defined in `paxos.proto`
- client driver in `main.go`
- replica logic and consensus flow in `server.go`
- generated protobuf bindings in `paxos/`

## Repository Structure

- `main.go`: client entrypoint and batch transaction driver
- `server.go`: server state, consensus logic, transaction processing, and metrics
- `paxos.proto`: service and message definitions
- `paxos/`: generated protobuf Go files
- `sample_transactions.csv`: optional sample batch input for local experimentation

## Running the Project

### Prerequisites

- Go 1.23+

### Start the servers

```bash
go run server.go
```

### Run the client

In a separate terminal:

```bash
go run main.go
```

The client is configured to read from `sample_transactions.csv`. In this public version, you can add your own file in that format if you want to run custom batches.

## Notes

- This public version is shared as a portfolio project to showcase distributed systems and consensus implementation work.
- The code is preserved close to the original academic project, with only light cleanup for public presentation.
