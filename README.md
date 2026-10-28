# Distributed Checkpointing and Fault Recovery

## Overview

This project implements checkpointing and fault-recovery mechanisms for a distributed word-counting system as part of **Lab 5 of CS744 (DECS), Autumn 2026, at IIT Bombay**.

**Most of the foundational code was provided as part of the lab starter code**, including the basic word-counting utilities, networking and socket communication, worker process management, message definitions, debugging utilities, crash-injection support, and output checker. The main objective of this implementation was to complete the missing logic for coordinated shutdown, consistent checkpointing, and recovery from worker failures.

The system consists of multiple mapper and reducer processes managed by a central coordinator. It periodically saves the state of all workers so that, when a worker crashes, the system can restore a consistent state and resume execution without restarting the entire job from the beginning.

## Architecture

The application follows a distributed MapReduce architecture consisting of three components: **mappers, reducers, and a coordinator**.

```text
                       Coordinator
                     /      |       \
           UDP control   UDP control  UDP control
                 /          |           \
             Mapper 0    Mapper 1    Reducer 0 ...
                 \          |           /
                  \--- TCP data channels ---/
                    Word counts and markers
```

### Communication

- **UDP control channel:** Used for coordinator-worker communication, including heartbeats, checkpoint requests, recovery commands, acknowledgements, and exit messages.
- **TCP data channel:** Used by mappers to send word counts and checkpoint markers to reducers.
- **Checkpoint storage:** Worker state is saved in the `ckpt/` directory, with separate files for each worker and checkpoint ID.

Each mapper processes a subset of the input files, while each reducer maintains aggregate counts for a subset of the words. The coordinator manages the execution lifecycle and ensures that checkpointing and recovery are coordinated across workers.

## Components and Responsibilities

### 1. Mapper (`mapper.c`)

The mapper processes input files and sends partial word counts to the appropriate reducers.

Responsibilities implemented and integrated:

- Process input files and maintain an offset representing the number of fully processed files.
- Save the current offset during checkpointing.
- Forward checkpoint markers to every reducer after sending preceding word-count messages.
- Acknowledge checkpoint completion to the coordinator.
- Restore the saved offset during recovery.
- Reconnect to reducers after recovery and resume processing from the restored position.
- Send a completion notification when all assigned files have been processed.

### 2. Reducer (`reducer.c`)

Each reducer receives partial word counts from multiple mappers and maintains the aggregate count for each word.

Responsibilities implemented and integrated:

- Process incoming word-count messages and update the aggregate counts.
- Track checkpoint markers received from individual mappers.
- Save the word-count state only after receiving markers from all mappers, ensuring a consistent checkpoint.
- Send checkpoint acknowledgements to the coordinator.
- Restore the saved word-count map during recovery.
- Close stale connections and reset marker-tracking state before accepting new data.
- Reject messages belonging to an older recovery generation.

### 3. Coordinator (`coordinator.c`)

The coordinator manages the overall execution of the distributed job.

Responsibilities implemented and integrated:

- Track worker heartbeats and completion notifications.
- Initiate periodic checkpoints at the configured interval.
- Track checkpoint acknowledgements from all mappers and reducers.
- Coordinate the final checkpoint before shutting down the system.
- Detect worker failures through missed heartbeats.
- Restart only the failed worker and select the latest checkpoint common to all workers.
- Coordinate recovery by restoring reducers first and mappers afterward.
- Use recovery IDs to distinguish messages from different execution generations.
- Resume normal execution after successful recovery and shut down gracefully when the job completes.

## Implemented Tasks

### Task 1: Handling Distributed Exit

The initial implementation could finish processing input files but remain running because the distributed shutdown protocol was incomplete.

The implemented protocol:

1. Waits for all mappers to finish processing their assigned files.
2. Requests a final checkpoint with checkpoint ID `0`.
3. Ensures that mappers forward markers and reducers save their final word counts.
4. Waits for acknowledgements from every mapper and reducer.
5. Sends exit messages to all workers and waits for the worker processes to terminate.

Checkpoint ID `0` is reserved for the final checkpoint.

### Task 2: Consistent Checkpointing

Periodic checkpointing was added to preserve worker state while the job is running.

- The coordinator initiates checkpoints at the configured interval `-t`.
- Periodic checkpoint IDs begin at `1` and increase monotonically.
- Each mapper saves its file offset and forwards markers to all reducers.
- Each reducer waits until it receives markers from every mapper before saving its word-count state.
- The coordinator tracks acknowledgements and marks a checkpoint complete only when all workers have acknowledged it.
- A new periodic checkpoint is not initiated while another is in progress.

This ensures that the saved mapper offsets and reducer counts correspond to a consistent global checkpoint.

### Task 3: Failure Detection and Recovery

The recovery protocol handles mapper or reducer failures during execution.

1. **Failure detection:** The coordinator detects a worker failure when its heartbeat is missing beyond the configured timeout.
2. **Checkpoint selection:** The coordinator selects the minimum latest-checkpoint ID acknowledged by all workers, ensuring that the restore point is common to the entire system.
3. **Worker restart:** Only the failed worker process is restarted.
4. **Reducer recovery:** All reducers restore their saved word-count state and acknowledge recovery.
5. **Mapper recovery:** All mappers restore their file offsets, reconnect to reducers, and acknowledge recovery.
6. **Resume execution:** Once both recovery phases finish, normal processing and periodic checkpointing resume.

A recovery ID (`rid`) acts as a logical generation counter. It is incremented on each recovery so that stale messages from a previous execution can be rejected.

## Building and Running

The project is intended to run on **Linux**, because the worker-management code uses Linux-specific process functionality.

### Prerequisites

- GCC or a compatible C compiler
- GNU Make
- Linux environment
- The input directories supplied with the lab starter code

### Build

From the project root:

```bash
make clean && make
```

This builds the `wcjob` executable and the `check` verification utility. The clean command also removes previous checkpoint files to prevent stale state from interfering with testing.

### Task 1: Distributed Exit

```bash
WC_DEBUG=1 ./wcjob -m 3 -r 2 -i inputs/small -t 0
./check inputs/small ckpt
```

The `-t 0` option disables periodic checkpointing for the Task 1 test.

### Task 2: Periodic Checkpointing

```bash
WC_DEBUG=1 ./wcjob -m 3 -r 2 -i inputs/medium \
  -t 500 -s 100000

./check --checkpoints inputs/medium ckpt
```

Here, `-t 500` sets a 500 ms checkpoint interval, and `-s 100000` adds a 100,000-microsecond processing delay per file.

### Task 3: Recovery Testing

Each crash scenario should be run separately after a clean build.

**One mapper crashes:**

```bash
WC_DEBUG=1 ./wcjob -m 3 -r 2 -i inputs/large \
  -t 500 -s 150000 -k 1 --chaos mapper --seed 1
```

**One reducer crashes:**

```bash
WC_DEBUG=1 ./wcjob -m 3 -r 2 -i inputs/large \
  -t 500 -s 150000 -k 1 --chaos reducer --seed 3
```

**A mapper and reducer crash together:**

```bash
WC_DEBUG=1 ./wcjob -m 3 -r 2 -i inputs/large \
  -t 500 -s 150000 -k 1 --chaos both --seed 2
```

**All workers crash:**

```bash
WC_DEBUG=1 ./wcjob -m 3 -r 2 -i inputs/large \
  -t 500 -s 150000 -k 1 --chaos all --seed 4
```

**Repeated worker crashes:**

```bash
WC_DEBUG=1 ./wcjob -m 3 -r 2 -i inputs/large \
  -t 500 -s 150000 -k 3 --chaos any --seed 5
```

**A reducer crashes while checkpoint alignment is in progress:**

```bash
WC_DEBUG=1 ./wcjob -m 3 -r 2 -i inputs/large \
  -t 500 -s 150000 -k 2 --kill-blocked --seed 6
```

After each recovery test, verify the output and checkpoint consistency:

```bash
./check --checkpoints inputs/large ckpt
```

A successful scenario should complete the job and produce `PASS` with zero inconsistent checkpoints.

## Source Files

| File | Purpose |
|---|---|
| `mapper.c` | Mapper processing, checkpointing, and recovery |
| `reducer.c` | Count aggregation, marker alignment, checkpointing, and recovery |
| `coordinator.c` | Checkpoint coordination, failure detection, and recovery orchestration |
| `common.h` | Shared message types, structures, and configuration |
| `worker.c` | Worker process creation, restarting, and heartbeats |
| `net.c` | Socket communication and networking utilities |
| `files.c` | File and checkpoint utility functions |
| `wcmap.c` | Word-count map operations and persistence |
| `tokenize.c` | Text tokenization utilities |
| `main.c` | Command-line parsing and job startup |
| `chaos.c` | Fault-injection support for testing worker failures |
| `check.c` | Verification of final output and checkpoint consistency |
| `Makefile` | Build and cleanup commands |

## Conclusion

This project extends the provided distributed word-counting starter system with coordinated shutdown, consistent periodic checkpointing, and worker-failure recovery.

It demonstrates how a coordinator, mappers, and reducers can cooperate to preserve consistent application state, recover from worker crashes, reject stale messages, and resume distributed execution without restarting the entire job.
