# Non-Blocking Consensus-Backed Transaction Coordination For Financial Microservices
> A High-Performance Middleware Layer for Non-Blocking, Dependency-Aware Distributed Transaction Coordination in Financial Microservices.

## Overview
The Hybrid Consensus Coordinator (HCC) elevates distributed concurrency control from the physical database disk to an in-memory middleware layer. Designed specifically for financial microservice architectures (e.g., core banking, payment gateways, ledger settlements), HCC acts as a concurrency shield that decouples high-throughput transaction ingestion from physical database limitations.

By inspecting payload read/write sets prior to database dispatch, HCC dynamically bifurcates traffic:
1. Fast Path (Independent Execution): Conflict-free transactions bypass heavy Raft-style consensus entirely and execute in parallel across participant databases.Slow Path
2. (Consensus Serialization): Conflicting "hot-key" transactions are quarantined in middleware RAM and serialized deterministically using Raft-compliant log entries.

## The Problem: 2PC & Locking Storms
Traditional Two-Phase Commit (2PC) implementations are data-blind. During high-contention events (e.g., flash sales, payroll runs, market open spikes), hundreds of concurrent requests target the same database rows (e.g., a popular merchant account).

- Without Middleware Shielding: Downstream databases attempt to grant local row locks to all incoming connections simultaneously.
- The Result: Massive HTTP 409 Conflict spams, exhausted database TCP connection pools, thread starvation, and catastrophic system-wide "Locking Storms" where throughput collapses to $\sim0.4$ TPM.

HCC solves this by enforcing an $O(1)$ constant-time sharded dependency analysis in middleware memory, resolving conflicts at microsecond latencies before opening a single database TCP socket.

##Key Features
- Pre-Execution Sharded Dependency Analyzer: Hashes operational keys into a 256-shard FNV-1a hash map, isolated by sync.RWMutex primitives to eliminate global lock contention bottlenecks.
- Dynamic Traffic Bifurcation: Bypasses Raft consensus for non-interfering transactions, cutting network round-trips and reducing physical Write-Ahead Log (WAL) disk I/O by 40%.Asynchronous Parallel 2PC Dispatcher: Broadcasts PREPARE and COMMIT phases in parallel using Go goroutines, bounding transaction latency strictly to the single slowest participant node regardless of whether the cluster contains 2 or 100 databases.
- Bounded Non-Blocking Execution: Enforces strict 30-second context timeouts (context.WithTimeout) and cooperative load-shedding to prevent goroutine memory leaks.
- Sub-Millisecond Crash Recovery (MTTR): Reconstructs the entire sharded in-memory lock state from an append-only, thread-safe WAL (w.file.Sync()) in $< 520 \mu s$ upon process assassination.

## System Architecture

![Architecture Diagram]([https://dummyimage.com/468x300?text=App+Screenshot+Here](https://github.com/lohithgsk/transaction-coordinator/blob/main/images/Architecture.png))

## Algorithmic Core
The framework evaluates lock availability in constant time $O(\vert{}K\vert{})$, where $\vert{}K\vert{}$ is the number of keys in the transaction payload:
```
Algorithm 1: Hybrid Routing and Concurrency Control (HR-CC)
=================================================================================
Input : Transaction Payload T with Key Set K
Output: Transaction Status (Committed / Aborted)

1:  for each key k in K do
2:      ShardIndex = FNV-1a(k) mod 256
3:      Acquire Read Lock on LockShard[ShardIndex]
4:  end for
5:  
6:  if IsIndependent(T) == True then
7:      // FAST PATH: Leaderless Execution
8:      Write WAL Entry: "FAST_COMMIT [TxnID] [Keys]" -> w.file.Sync()
9:      Broadcast 2PC Prepare/Commit asynchronously to Participants in Parallel
10:     return Status: Committed
11: else
12:     // SLOW PATH: Deterministic Consensus Serialization
13:     Flush Intent via Raft: RAFT_PROPOSE(T)
14:     Enqueue T into Middleware Memory Queue with 30s Timeout
15:     Poll lock state every 50ms until exclusive locks acquired or context expires
16:     if Timed Out then
17:         Trigger Cooperative Abort -> return Status: Aborted
18:     end if
19:     Execute serialized 2PC transaction -> return Status: Committed
20: end if
=================================================================================
```

## Empirical Performance & Results
Tested against traditional data-blind 2PC baselines under severe Zipfian skew ($80/20$ hot-key distribution):

| Evaluation Metric | Baseline Strict 2PC | Proposed Hybrid Coordinator | Impact / Optimization |
|-------------------------------|-------------------------|-------------------------------|-----------------------------------------------|
| Throughput (100% Contention) | 0.4 TPM | 19.8 TPM | ~400% sustained throughput boost |
| Active DB Network Connections | 50 simultaneous hits | 1 active hit | 98% reduction in DB locking storms |
| Spike Resolution Time (50 Txns) | 150.0 seconds | 12.1 seconds | Resolved contention 12× faster |
| Physical Disk I/O (WAL) | 100 physical writes | 60 physical writes | 40% reduction in storage bottleneck |
| Cluster Scalability Latency | Linear degradation | Constant ~3.0 s | O(1) scaling (100 nodes = 2 nodes) |
| Mean Time To Recovery (MTTR) | Manual / Broken State | 519.5 μs | Sub-millisecond state reconstruction |

## Project Directory Structure

```text
.
├── main.go               # Dynamic CLI entry point & mode multiplexer
├── coordinator.go        # API Gateway, Transaction Manager, and 2PC Dispatcher
├── analyzer.go           # 256-shard FNV-1a Memory Lock Graph & Dependency Engine
├── wal.go                # Thread-safe Write-Ahead Logging & OS buffer flushing
├── recovery.go           # Sub-millisecond state reconstruction engine
├── participant.go        # Simulated database node with localized row locks
├── loadtest.go           # Multi-threaded Zipfian load generator (Low, High, Mixed)
├── go.mod                # Module definitions
└── README.md             # Project documentation
```

---

## Getting Started

### Prerequisites

- **Go (Golang):** Version **1.22.0** or higher.
- **Operating System:** Linux, macOS, or Windows.
- **Terminal:** Bash or PowerShell.

### Installation

1. Clone the repository:

```bash
git clone https://github.com/your-username/hybrid-consensus-coordinator.git
cd hybrid-consensus-coordinator
```

2. Verify your Go installation:

```bash
go version
```

---

## Running the Application

The project is compiled as a **single multiplexed binary**, with behavior controlled using the `-mode` flag.

### 1. Start the Coordinator

Launches the middleware transaction coordinator and API gateway on port `8082`.

```bash
go run . -mode=coordinator -port=8082
```

### 2. Start Participant Database Nodes

Launch simulated distributed database participants.

```bash
go run . -mode=participant -port=8083 &
go run . -mode=participant -port=8084 &
```

### 3. Launch the Entire Cluster

Starts the coordinator together with multiple participant nodes automatically.

```bash
go run . -mode=cluster
```

---

## Running Workload Simulations

The built-in load generator reproduces common distributed transaction workloads.

### Low Contention Workload

Uniform traffic where most transactions are independent and execute through the Fast Path.

```bash
go run . -mode=loadtest -workload=low
```

### High Contention Workload

Simulates a flash-sale scenario where hundreds of transactions compete for the same hot key.

```bash
go run . -mode=loadtest -workload=high
```

### Mixed Contention Workload

Represents realistic production traffic following the 80/20 rule, combining independent transactions with highly contended keys.

```bash
go run . -mode=loadtest -workload=mixed
```

### Chaos Failure Injection

Abruptly terminates the coordinator (`kill -9`) during execution to benchmark recovery performance and MTTR.

```bash
go run . -mode=loadtest -workload=chaos
```

---

### API Reference

#### `POST /txn`

Primary transaction submission endpoint.

#### Request Body

```json
{
  "txn_id": "TXN_99482A",
  "keys": [
    "ACC_USER_101",
    "ACC_USER_202"
  ],
  "participants": [
    "http://localhost:8083",
    "http://localhost:8084"
  ]
}
```

#### Responses

| Status Code | Description |
|-------------|-------------|
| **200 OK** | Transaction committed successfully using either the Fast Path or Slow Path. |
| **409 Conflict** | Transaction aborted after contention queue timeout through cooperative load shedding. |
| **500 Internal Server Error** | One or more downstream participant nodes failed during transaction execution. |
