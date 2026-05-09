# Multithreaded Process Manager Simulator

A C-based simulation of an operating system process-management subsystem using POSIX threads (`pthreads`) and synchronization primitives.

This project models how a shared process table behaves when multiple worker threads concurrently create, terminate, wait on, and inspect simulated processes.

## Project Goal

The simulator demonstrates safe concurrent access to a global Process Table and correct process lifecycle handling using PCB (Process Control Block) entries instead of real OS processes.

## Core System Model

### 1) Global Process Table
- Shared structure with capacity for **up to 64 active simulated processes**.
- Stores all active PCBs (terminated/reaped processes are removed).

### 2) Process Control Block (PCB)
Each process entry includes at least:
- `pid` (unique process ID)
- `ppid` (parent PID)
- `state`
- `exit_status` (valid after exit)
- child process tracking (list/collection)
- synchronization objects for wait/wakeup behavior

### 3) Process States
- **RUNNING / ACTIVE**: process exists and is active
- **BLOCKED / WAITING**: parent is waiting for a child
- **ZOMBIE**: child exited but not yet collected by parent
- **TERMINATED / REAPED**: child collected and removed

Typical legal transitions:
- `RUNNING -> ZOMBIE` (on `pm_exit`)
- `RUNNING -> WAITING` (on `pm_wait` when no matching exited child exists)
- `WAITING -> RUNNING` (child exit wakes parent)
- `ZOMBIE -> TERMINATED/REAPED` (parent successfully waits and collects)

## Process Manager Operations

- `pm_fork(parent_pid)`
  - Creates a child under `parent_pid`
  - Allocates next PID
  - Adds PCB to process table
  - Records parent-child relation

- `pm_exit(pid, status)`
  - Marks process as exited
  - Stores `exit_status`
  - Wakes waiting parent (if applicable)

- `pm_wait(parent_pid, child_pid)`
  - Waits for specific child (`child_pid >= 0`) or first exiting child (`child_pid = -1`)
  - If no matching child exists, returns trivially
  - If child already zombie, returns immediately and reaps child
  - Otherwise blocks parent until eligible child exits

- `pm_kill(pid)`
  - Sends termination request to process (implemented as managed simulated termination)

- `pm_ps()`
  - Prints current snapshot of active process table with columns:
    - `PID`
    - `PPID`
    - `STATE`
    - `EXIT_STATUS` (`-` if not exited)

## Concurrency Model

Multiple worker threads execute command scripts concurrently and operate on the same shared process table.

Implementation must correctly synchronize scenarios such as:
- simultaneous forks from different threads
- one thread waiting while another exits/kills a child
- concurrent operations on the same PID
- table printing while updates are happening

Use mutexes/condition variables/semaphores (as appropriate) to avoid races and inconsistent state.

## Worker Scripts

Run program with one or more script files:

```bash
./pm_sim thread0.txt thread1.txt thread2.txt
```

- Number of worker threads = number of script files provided
- Thread IDs are assigned sequentially from `0`
- Each worker reads commands line-by-line and executes in order

Supported script commands:

```text
fork <parent_pid>
exit <pid> <status>
wait <parent_pid> <child_pid>   # child_pid can be -1
kill <pid>
sleep <milliseconds>
```

## Process Table Monitor Thread

A dedicated monitor thread should:
1. Sleep until notified of a process table modification
2. Print a fresh process-table snapshot
3. Repeat for subsequent updates

The monitor should **not poll continuously**.
All snapshots are written to `snapshots.txt`, separated by newlines.

## Expected Behavior

A correct implementation should:
- maintain process-table consistency under concurrency
- preserve parent-child relationships
- correctly handle `fork`, `exit`, `wait`, `kill`
- correctly implement zombie/reaping semantics
- produce clear, consistent snapshots after updates

## Program Structure

Recommended module split:
- **Process manager module**: PCB/table logic + synchronization + initial process (`PID=1`, `PPID=0`)
- **Script interpreter**: parse command files and dispatch operations
- **Worker threads**: execute scripts
- **Monitor thread**: snapshot updates to `snapshots.txt`

## Academic Context

This repository corresponds to the *Multithreaded Process Manager Simulator* assignment, focused on applying pthread-based synchronization to OS-style process-table behavior in a simulated environment.
