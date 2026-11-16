# Reliable UDP Transport Protocol

## Overview

A custom reliable transport protocol implemented in C++ on top of UDP sockets, developed as part of a Computer Networks university assignment.

The project implements several TCP-inspired mechanisms, including connection establishment, reliable data delivery, sliding windows, acknowledgments, retransmissions, and flow control.

The goal is to explore how reliable communication can be implemented over an unreliable transport protocol such as UDP.

## Features

- **Connection establishment:** Custom three-way handshake using SYN, SYN-ACK, and ACK messages.
- **Reliable data transfer:** Sequence numbers and cumulative acknowledgments.
- **Sliding window:** Multiple outstanding segments can be transmitted before receiving acknowledgments.
- **Retransmission:** Outstanding segments are retransmitted when a timeout occurs.
- **Flow control:** Receiver-advertised window limits the amount of in-flight data.
- **Multiple connections:** Separate connection states, sockets, buffers, and timers.
- **Multithreading:** POSIX threads and mutexes for packet processing and synchronization.

## Architecture

The implementation is divided into three main components:

- **Sender (`libsend.cpp`):** Handles connection initiation, segmentation, data transmission, acknowledgments, and retransmissions.
- **Receiver (`librecv.cpp`):** Handles incoming connections, in-order packet reception, acknowledgment generation, and receive buffer management.
- **Common utilities (`libcommon.cpp`):** Provides socket polling and timeout handling.

### Communication Flow

```text
        CLIENT                      SERVER
           |                           |
           | -------- SYN -----------> |
           | <------ SYN-ACK --------- |
           | -------- ACK -----------> |
           |                           |
           | ===== Connection ======= |
           |                           |
           | -------- DATA ----------> |
           | -------- DATA ----------> |
           | <------- ACK ------------ |
           |                           |
           |  [Timeout if necessary]   |
           | -------- DATA ----------> |
           | <------- ACK ------------ |
           |                           |
```

The sender and receiver communicate through UDP sockets using a custom protocol header defined in `protocol.h`.

## Protocol Design

### Connection Establishment

A three-way handshake establishes a logical connection between the client and server.

The server creates a dedicated UDP socket for each accepted connection and communicates the assigned port and connection identifier to the client.

### Sliding Window and Acknowledgments

Application data is divided into segments, each assigned a sequence number.

The sender transmits segments within its current sending window and tracks outstanding acknowledgments.

The receiver accepts segments in sequence and sends cumulative acknowledgments. Out-of-order segments are not buffered for reordering.

### Retransmission

The sender uses timers to detect possible packet or acknowledgment loss.

When a timeout occurs, the outstanding segments in the current window are retransmitted, following a Go-Back-N-style approach.

### Flow Control

The receiver advertises its remaining buffer capacity through the `recv_window` field.

The sender uses this information to avoid transmitting more data than the receiver can currently accommodate.

The initial sending window is estimated using the bandwidth-delay product and constrained by implementation-defined limits.

## Project Structure

```text
reliable-udp-transport/
├── lib/
│   ├── include/
│   │   ├── lib.h
│   │   ├── protocol.h
│   │   └── utils.h
│   ├── libcommon.cpp
│   ├── librecv.cpp
│   └── libsend.cpp
├── client.cpp
├── server.cpp
├── Makefile
├── README.md
└── .gitignore
```

## Build

### Requirements

- Linux environment (or WSL on Windows)
- C++ compiler (g++)
- GNU Make
- POSIX threads
- Linux socket and timer APIs

### Compilation

```bash
make
```

This builds the `client` and `server` executables.

To remove generated binaries and object files:

```bash
make clean
```

## Usage

The project demonstrates file transfer between a UDP client and server using the custom reliable transport protocol.

### 1. Configure the server address

The client currently uses a hardcoded server IP address in `client.cpp`:

```cpp
inet_aton("172.16.0.100", &addr);
```

For local testing, replace it with:

```cpp
inet_aton("127.0.0.1", &addr);
```

Rebuild the project after modifying the address.

### 2. Start the server

In the first terminal:

```bash
./server
```

The server listens on UDP port `8032` and waits for a client connection.

### 3. Send a file

In another terminal:

```bash
./client sample.txt
```

The client establishes a connection, sends the file size, and then transmits the file contents.

The server writes the received data to:

```text
file.out
```

You can compare the original and received files using:

```bash
cmp sample.txt file.out
```

### Multiple Clients

The server can accept multiple client connections:

```bash
./server 2
```

Start two clients in separate terminals:

```bash
./client file1.txt
```

```bash
./client file2.txt
```

The server stores the received files as `file0.out` and `file1.out`, in connection acceptance order.

### Optional Client Delay

The client accepts an optional second argument specifying a delay in seconds before starting the file transfer:

```bash
./client sample.txt 3
```

### Current Limitations

- The server address is hardcoded in the client.
- The client does not implement graceful connection termination and remains running after queuing the file data.
- The protocol is an educational implementation, not a replacement for production TCP.
- Reliability and concurrent-transfer behavior should be validated through testing before production use.
