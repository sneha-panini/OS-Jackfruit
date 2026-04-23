# Supervised Multi-Container Runtime with Kernel Memory Monitor

## 1. Team Information

* Name:
* SRN:
* 


---

## 2. Project Overview

This project implements a **lightweight container runtime in C** along with a **Linux kernel module for memory monitoring**.

The system consists of:

### 🔹 User-Space Runtime (`engine.c`)

* Supervisor-based architecture
* Multi-container support
* CLI commands (`start`, `run`, `ps`, `logs`, `stop`)
* Logging system using producer-consumer model
* Namespace isolation (PID, UTS, mount)

### 🔹 Kernel Module (`monitor.c`)

* Tracks container processes
* Enforces memory limits
* Uses periodic timer-based monitoring
* Communicates via `ioctl`

---

## 3. Features

* Container isolation using Linux namespaces
* Supervisor-controlled lifecycle
* Per-container logging system
* Memory enforcement:

  * Soft limit → warning
  * Hard limit → process termination
* Scheduling experiments using `nice` values
* Clean teardown (no zombie processes)

---

## 4. Setup Instructions

### 🔧 Install dependencies

```bash
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r)
```

---

### 📦 Prepare root filesystem

```bash
mkdir rootfs-base
wget https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz
tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs-base

cp -a rootfs-base rootfs-alpha
cp -a rootfs-base rootfs-beta
```

---

### 🔨 Build project

```bash
make clean
make
```

---

### 🔌 Load kernel module

```bash
sudo insmod monitor.ko
ls -l /dev/container_monitor
```

---

### 🚀 Start supervisor

```bash
sudo ./engine supervisor ./rootfs-base
```

---

## 5. Running Containers

### ▶️ Start container

```bash
sudo ./engine start alpha ./rootfs-alpha /bin/sh
```

---

### ▶️ Run foreground container

```bash
sudo ./engine run beta ./rootfs-beta /bin/sh
```

---

### 📊 List containers

```bash
sudo ./engine ps
```

---

### 📜 View logs

```bash
sudo ./engine logs alpha
```

---

### 🛑 Stop container

```bash
sudo ./engine stop alpha
```

---

## 6. Logging System

* Each container writes output via pipe
* Supervisor uses:

  * Producer threads → read container output
  * Consumer thread → write logs
* Logs stored in:

```bash
logs/<container_id>.log
```

---

## 7. Memory Monitoring

The kernel module:

* Tracks processes via linked list
* Runs periodic checks using timer
* Uses RSS (resident set size)

### Behavior

| Condition        | Action             |
| ---------------- | ------------------ |
| RSS > soft limit | Warning in `dmesg` |
| RSS > hard limit | Process killed     |

---

## 8. Testing Memory Limits

```bash
cp memory_hog rootfs-alpha/

sudo ./engine run mem ./rootfs-alpha /memory_hog --soft-mib 20 --hard-mib 40
```

Check logs:

```bash
dmesg | tail
```

---

## 9. Scheduling Experiment

```bash
sudo ./engine start c1 ./rootfs-alpha /cpu_hog --nice 0
sudo ./engine start c2 ./rootfs-alpha /cpu_hog --nice 10
```

### Observation

* Lower nice value → more CPU share
* Higher nice value → slower execution

---

## 10. Engineering Analysis

### 1. Isolation Mechanisms

Containers use Linux namespaces:

* **PID namespace** → isolates process IDs so containers have their own process tree
* **UTS namespace** → allows each container to have its own hostname
* **Mount namespace** → provides isolated filesystem view

`chroot` is used to restrict filesystem access to the container rootfs.

However, all containers still share:

* the same kernel
* the same physical memory
* the same scheduler

---

### 2. Supervisor and Process Lifecycle

A long-running supervisor process:

* spawns containers using `clone()`
* tracks metadata (PID, state, limits)
* handles `SIGCHLD` to avoid zombie processes
* performs cleanup and state transitions

This ensures centralized lifecycle control.

---

### 3. IPC and Communication

Communication between CLI and supervisor is implemented using:

* **UNIX domain sockets** (control plane)

Logging path uses:

* **pipes** from container → supervisor

This separation ensures control and data paths do not interfere.

---

### 4. Logging System Design

Logging uses a **producer-consumer model**:

* Producer threads read from container pipes
* Consumer thread writes logs to files

Synchronization is handled using:

* mutexes → protect shared buffer
* condition variables → manage full/empty buffer

This prevents:

* race conditions
* blocking I/O
* log loss

---

### 5. Memory Management and Enforcement

The kernel module monitors **RSS (Resident Set Size)**:

* represents actual physical memory used
* excludes swapped memory

Two thresholds:

* **Soft limit** → warning (logged via `printk`)
* **Hard limit** → process is killed using `SIGKILL`

Kernel-space enforcement is required because user-space cannot reliably control memory usage.

---

### 6. Scheduling Behavior

Experiments demonstrate:

* CPU-bound processes compete for CPU
* `nice` values influence scheduling priority
* I/O-bound processes remain responsive

Linux scheduler dynamically balances fairness and responsiveness.

---

## 11. Design Decisions

### Namespace Isolation

* PID → process isolation
* UTS → hostname isolation
* Mount → filesystem isolation

---

### Supervisor Model

* Centralized control
* Handles lifecycle + signals
* Prevents zombie processes

---

### Logging Design

* Bounded buffer prevents blocking
* Thread-safe using mutex + condition variables

---

### Kernel Module

* Strong enforcement (cannot bypass)
* Uses timer instead of polling loop

---

## 11. Limitations

* No cgroups (manual memory tracking)
* Basic filesystem isolation (no pivot_root)
* No network namespace isolation
* Single supervisor = single point of failure

---

## 12. Repository Structure

```bash
.
├── engine.c
├── monitor.c
├── monitor_ioctl.h
├── Makefile
├── rootfs-base/
├── rootfs-alpha/
├── rootfs-beta/
├── logs/
├── cpu_hog.c
├── memory_hog.c
├── io_pulse.c
└── README.md
```

---

## 13. Cleanup

```bash
sudo ./engine stop alpha
sudo rmmod monitor
```

---
