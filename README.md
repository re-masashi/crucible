# Crucible

An Embeddable, Multicore, In-Memory Term Store

Crucible is a concurrent, in-memory, key-value database `built in Rust`, designed for direct embedding within Vial's runtime. It is inspired by Erlang's ETS, prioritizing high-throughput, low-latency data access for multi-threaded applications. The entire system resides within the Vial VM's process space, eliminating network latency and serialization overhead for operations.

The core data structure is a sharded hash table, allowing for fine-grained locking and scaling across multiple CPU cores. Keys and values are represented by a generic Term enum, capable of expressing integers, floats, strings, and nested collections (arrays/structs). This document-oriented term model allows for the storage of complex, schemaless data structures while maintaining the performance characteristics of a key-value store.

Concurrency is managed through a combination of per-shard RwLocks for isolation and techniques like epoch-based garbage collection for safe, non-blocking memory reclamation. The design favors read-heavy workloads with support for highly concurrent lookups.

The primary interface is a Rust API, with a stable C ABI provided for FFI integration into other language runtimes, including JIT and AOT compiled environments for Vial (future plans). A key feature is the ability to enforce static types at the database boundary via table level schemas or something like that, making it a type safe storage layer, unlike the dynamically typed ETS.

## Goals

- Performance: Achieve sub-microsecond latencies for read/write operations by eliminating network and serialization overhead.
- Concurrency: Scale read/write throughput linearly with available CPU cores by using sharded data structures and fine-grained locking.
- Integration: Provide a zero-dependency, embeddable library with a stable C API for seamless integration into Vial's runtime.
- Type Safety: Implement an optional schema layer to enforce Vial's static type constraints on keys and values at runtime.
- Durability (Optional): Offer a high-performance, append-only Write-Ahead Log (WAL) for optional on-disk persistence and crash recovery.
- Core Features: Implement essential primitives for concurrent systems, including atomic multi-key transactions (via OCC), PubSub event notifications for data changes, and Time-To-Live (TTL) for automatic data eviction.

## Possible Use Cases

As a sister project to the Vial compiler, Crucible serves as the standard, high-performance state management layer for the language's runtime (not integrated yet).

- Runtime Caching: An in-memory cache for frequently accessed data, such as database query results, or rendered web content.
- Shared State Management: A concurrent state store for multi-threaded applications, like web server session data or the world state in a multiplayer game engine.
- Job Queue Implementation: A backing store for a high-throughput, in-process job queue, using atomic operations to manage job acquisition and completion.
- Runtime Telemetry: A sink for high-frequency diagnostic data and performance counters, providing a fast and safe way for the runtime to record its own metrics without impacting performance.

## Building and Testing

To build the project:
```bash
cargo build
```

To run tests:
```bash
cargo test
```

To run benchmarks:
```bash
cargo run --release
```
