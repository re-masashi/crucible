use crucible::*;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn benchmark<F>(name: &str, iterations: usize, mut f: F)
where
    F: FnMut(),
{
    for _ in 0..100 {
        f();
    }

    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let elapsed = start.elapsed();

    let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();
    let ns_per_op = elapsed.as_nanos() / iterations as u128;

    println!("{}", name);
    println!("  Total time: {:?}", elapsed);
    println!("  Ops/sec: {:.2}", ops_per_sec);
    println!("  ns/op: {}", ns_per_op);
    println!();
}

fn run_benchmarks(store: &Arc<Store>, label: &str) {
    println!("{}\n", label);

    println!(" Single-threaded \n");

    benchmark("Insert (no TTL)", 100_000, || {
        let key = format!("key_{}", rand::random::<u32>());
        store.insert(&key, Term::Int(42)).unwrap();
    });

    benchmark("Insert (with TTL)", 100_000, || {
        let key = format!("ttl_key_{}", rand::random::<u32>());
        store.insert_with_ttl(&key, Term::Int(42), 3600).unwrap();
    });

    println!("Pre-populating 10,000 keys (5,000 with TTL, 5,000 without)...");
    for i in 0..5_000 {
        store
            .insert(&format!("test_key_{}", i), Term::Int(i as i64))
            .unwrap();
    }
    for i in 5_000..10_000 {
        store
            .insert_with_ttl(&format!("test_key_{}", i), Term::Int(i as i64), 3600)
            .unwrap();
    }
    println!();

    benchmark("Get (no TTL keys)", 100_000, || {
        let key = format!("test_key_{}", rand::random::<u32>() % 5_000);
        let _ = store.get(&key);
    });

    benchmark("Get (with TTL, not expired)", 100_000, || {
        let key = format!("test_key_{}", 5_000 + rand::random::<u32>() % 5_000);
        let _ = store.get(&key);
    });

    println!("Pre-populating 5,000 keys with 1ms TTL (will expire)...");
    for i in 10_000..15_000 {
        store
            .insert_with_ttl_millis(&format!("test_key_{}", i), Term::Int(i as i64), 100)
            .unwrap();
    }
    thread::sleep(Duration::from_millis(200));
    println!();

    benchmark("Get (expired keys, lazy delete)", 100_000, || {
        let key = format!("test_key_{}", 10_000 + rand::random::<u32>() % 5_000);
        let _ = store.get(&key);
    });

    benchmark("TTL check (get remaining time)", 100_000, || {
        let key = format!("test_key_{}", 5_000 + rand::random::<u32>() % 5_000);
        let _ = store.ttl(&key);
    });

    benchmark("Expire (set TTL on existing key)", 100_000, || {
        let key = format!("test_key_{}", rand::random::<u32>() % 5_000);
        let _ = store.expire(&key, 7200);
    });

    benchmark("Persist (remove TTL)", 100_000, || {
        let key = format!("test_key_{}", 5_000 + rand::random::<u32>() % 5_000);
        let _ = store.persist(&key);
    });

    benchmark("Update (preserves TTL)", 100_000, || {
        let key = format!("test_key_{}", 5_000 + rand::random::<u32>() % 5_000);
        let _ = store.update(&key, Term::Int(999));
    });

    benchmark("Contains (with expiration check)", 100_000, || {
        let key = format!("test_key_{}", rand::random::<u32>() % 10_000);
        let _ = store.contains_key(&key);
    });

    println!(" Multi-threaded (8 threads) \n");

    let iterations = 100_000;

    let start = Instant::now();
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let store = Arc::clone(store);
            thread::spawn(move || {
                for _ in 0..iterations {
                    let key = format!("test_key_{}", rand::random::<u32>() % 10_000);
                    let _ = store.get(&key);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();
    let total_ops = iterations * 8;
    println!("Concurrent reads (mixed TTL)");
    println!("  Total time: {:?}", elapsed);
    println!("  Ops/sec: {:.2}", total_ops as f64 / elapsed.as_secs_f64());
    println!("  ns/op: {}", elapsed.as_nanos() / total_ops as u128);
    println!();

    let start = Instant::now();
    let handles: Vec<_> = (0..8)
        .map(|thread_id| {
            let store = Arc::clone(store);
            thread::spawn(move || {
                for i in 0..iterations {
                    let key = format!("write_ttl_{}_{}", thread_id, i);
                    store
                        .insert_with_ttl(&key, Term::Int(i as i64), 3600)
                        .unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();
    println!("Concurrent writes (with TTL)");
    println!("  Total time: {:?}", elapsed);
    println!("  Ops/sec: {:.2}", total_ops as f64 / elapsed.as_secs_f64());
    println!("  ns/op: {}", elapsed.as_nanos() / total_ops as u128);
    println!();

    let start = Instant::now();
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let store = Arc::clone(store);
            thread::spawn(move || {
                for i in 0..iterations {
                    if i % 10 == 0 {
                        let key = format!("mixed_ttl_{}", rand::random::<u32>() % 10_000);
                        store
                            .insert_with_ttl(&key, Term::Int(i as i64), 3600)
                            .unwrap();
                    } else {
                        let key = format!("test_key_{}", rand::random::<u32>() % 10_000);
                        let _ = store.get(&key);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();
    println!("Mixed workload 90/10 read/write (with TTL)");
    println!("  Total time: {:?}", elapsed);
    println!("  Ops/sec: {:.2}", total_ops as f64 / elapsed.as_secs_f64());
    println!("  ns/op: {}", elapsed.as_nanos() / total_ops as u128);
    println!();

    println!("Final store size: {}\n", store.len());
}

fn main() {
    println!("TEST 1: WITHOUT BACKGROUND CLEANUP");

    let store1 = Arc::new(Store::new(Some("no_bg_cleanup".into()), 64));
    run_benchmarks(&store1, "CRUCIBLE TTL BENCHMARKS (No Background Cleanup)");

    println!(" Manual Cleanup Performance \n");

    println!("Inserting 50,000 keys with 100ms TTL...");
    let start = Instant::now();
    for i in 0..50_000 {
        store1
            .insert_with_ttl_millis(&format!("cleanup_key_{}", i), Term::Int(i as i64), 100)
            .unwrap();
    }
    println!("Insert time: {:?}", start.elapsed());

    thread::sleep(Duration::from_millis(200));

    println!("Checking expiration status...");
    let (expired, not_expired) = store1.check_expired_count();
    println!("Expired: {}, Not expired: {}", expired, not_expired);
    println!("Total in store before cleanup: {}", store1.len());

    let start = Instant::now();
    let removed = store1.cleanup_expired();
    let elapsed = start.elapsed();

    println!("Cleanup expired keys");
    println!("  Removed: {} keys", removed);
    println!("  Total time: {:?}", elapsed);
    println!("  Keys/sec: {:.2}", removed as f64 / elapsed.as_secs_f64());
    println!("Final store size: {}\n", store1.len());

    println!("TEST 2: WITH BACKGROUND CLEANUP (every 2 seconds)");

    let store2 = Arc::new(Store::new(Some("with_bg_cleanup".into()), 64));

    println!("Starting background cleanup thread (interval: 2 seconds)...");
    let cleanup_handle = store2.clone().start_background_cleanup(2);
    println!("Background cleanup started!\n");

    thread::sleep(Duration::from_millis(100));

    run_benchmarks(&store2, "CRUCIBLE TTL BENCHMARKS (With Background Cleanup)");

    println!(" Background Cleanup Stress Test \n");

    println!("Inserting 100,000 keys with 500ms TTL while cleanup runs...");
    let start = Instant::now();
    for i in 0..100_000 {
        store2
            .insert_with_ttl_millis(&format!("stress_key_{}", i), Term::Int(i as i64), 500)
            .unwrap();

        if i > 0 && i % 20_000 == 0 {
            println!("  Inserted {} keys, store size: {}", i, store2.len());
        }
    }
    let insert_time = start.elapsed();
    println!("Insert completed in {:?}", insert_time);
    println!("Store size immediately after inserts: {}", store2.len());

    println!("\nWaiting 3 seconds for expiration and cleanup...");
    thread::sleep(Duration::from_secs(3));

    println!("Store size after cleanup cycles: {}", store2.len());

    println!("\nStopping background cleanup...");
    store2.stop_background_cleanup();
    cleanup_handle.join().unwrap();
    println!("Background cleanup stopped.\n");

    println!("TEST 3: MEMORY BENCHMARKS");
    run_memory_benchmarks();
}

fn get_memory_usage_mb() -> f64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if let Some(kb) = parts.get(1) {
                return kb.parse::<f64>().unwrap() / 1024.0;
            }
        }
    }
    0.0
}

fn get_memory_stats() -> (usize, usize, usize, usize) {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap();
    let parts: Vec<usize> = statm
        .split_whitespace()
        .map(|s| s.parse().unwrap_or(0))
        .collect();
    let page_size = 4096;
    (
        parts[0] * page_size / 1024 / 1024, // total virtual memory (MB)
        parts[1] * page_size / 1024 / 1024, // resident set size (MB)
        parts[2] * page_size / 1024 / 1024, // shared pages (MB)
        parts[3] * page_size / 1024 / 1024, // code segment (MB)
    )
}

fn get_detailed_memory_breakdown() -> std::collections::HashMap<String, usize> {
    let mut map = std::collections::HashMap::new();

    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    for line in status.lines() {
        if line.starts_with("VmPeak:")
            || line.starts_with("VmSize:")
            || line.starts_with("VmRSS:")
            || line.starts_with("VmData:")
            || line.starts_with("VmStk:")
            || line.starts_with("VmExe:")
            || line.starts_with("VmLib:")
            || line.starts_with("VmPTE:")
        {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let key = parts[0].trim_end_matches(':').to_string();
                if let Ok(kb) = parts[1].parse::<usize>() {
                    map.insert(key, kb / 1024); // Convert to MB
                }
            }
        }
    }
    map
}

fn run_memory_benchmarks() {
    println!("\nMEMORY BENCHMARKS\n");

    let key_count = 500_000;
    let value_size = 100;

    println!(
        "Testing with {} keys, ~{} bytes value each\n",
        key_count, value_size
    );

    let mem_stats_before = get_memory_stats();
    // let detailed_before = get_detailed_memory_breakdown();

    println!("BEFORE");
    println!(
        "  Virtual: {} MB, RSS: {} MB",
        mem_stats_before.0, mem_stats_before.1
    );
    println!();

    println!("1. Creating store...");
    let store = std::sync::Arc::new(Store::new(Some("memory_bench".into()), 64));
    let mem_before = get_memory_usage_mb();
    println!("   Memory before: {:.2} MB\n", mem_before);

    println!("2. Inserting {} string keys...", key_count);
    let start = Instant::now();
    for i in 0..key_count {
        let key = format!("key_{}", i);
        let value = "x".repeat(value_size);
        store.insert_string(&key, &value).unwrap();
        if i > 0 && i % 100_000 == 0 {
            println!("   Inserted {} keys...", i);
        }
    }
    let insert_time = start.elapsed();
    // let mem_after_strings = get_memory_usage_mb();
    let mem_stats_after_strings = get_memory_stats();
    let detailed_after_strings = get_detailed_memory_breakdown();

    println!("   Time: {:?}", insert_time);
    println!("   VmRSS: {:.2} MB", mem_stats_after_strings.1);
    println!(
        "   Memory increase (VmRSS): {:.2} MB",
        mem_stats_after_strings.1 - mem_stats_before.1
    );
    println!(
        "   Memory per key: {:.2} bytes\n",
        (mem_stats_after_strings.1 - mem_stats_before.1) as f64 * 1024.0 * 1024.0
            / key_count as f64
    );

    println!("3. Inserting {} bytes keys...", key_count);
    let store2 = std::sync::Arc::new(Store::new(Some("memory_bench2".into()), 64));
    // let mem_before2 = get_memory_usage_mb();
    let mem_stats_before2 = get_memory_stats();
    let start = Instant::now();
    for i in 0..key_count {
        let key = format!("bytes_key_{}", i);
        let value = vec![0u8; value_size];
        store2.insert_bytes(&key, &value).unwrap();
        if i > 0 && i % 100_000 == 0 {
            println!("   Inserted {} keys...", i);
        }
    }
    let insert_time = start.elapsed();
    // let mem_after_bytes = get_memory_usage_mb();
    let mem_stats_after_bytes = get_memory_stats();
    let detailed_after_bytes = get_detailed_memory_breakdown();

    println!("   Time: {:?}", insert_time);
    println!("   VmRSS: {:.2} MB", mem_stats_after_bytes.1);
    println!(
        "   Memory increase (VmRSS): {:.2} MB",
        mem_stats_after_bytes.1 - mem_stats_before2.1
    );
    println!(
        "   Memory per key: {:.2} bytes\n",
        (mem_stats_after_bytes.1 - mem_stats_before2.1) as f64 * 1024.0 * 1024.0 / key_count as f64
    );

    println!("4. Arena stats after {} string keys:", key_count);
    let arena_stats = store.arena_stats();
    let total_capacity: usize = arena_stats.iter().map(|s| s.capacity).sum();
    let total_used: usize = arena_stats.iter().map(|s| s.used_bytes).sum();
    println!(
        "   Total arena capacity: {} MB",
        total_capacity / 1024 / 1024
    );
    println!("   Total arena used: {} MB", total_used / 1024 / 1024);
    println!("   Number of arenas: {}\n", arena_stats.len());

    println!("5. Arena stats after {} bytes keys:", key_count);
    let arena_stats2 = store2.arena_stats();
    let total_capacity2: usize = arena_stats2.iter().map(|s| s.capacity).sum();
    let total_used2: usize = arena_stats2.iter().map(|s| s.used_bytes).sum();
    println!(
        "   Total arena capacity: {} MB",
        total_capacity2 / 1024 / 1024
    );
    println!("   Total arena used: {} MB", total_used2 / 1024 / 1024);
    println!("   Number of arenas: {}\n", arena_stats2.len());

    println!("6. Detailed memory breakdown (VmData = heap + data):");
    println!("   String store:");
    println!(
        "     VmPeak: {} MB",
        detailed_after_strings.get("VmPeak").unwrap_or(&0)
    );
    println!(
        "     VmSize: {} MB",
        detailed_after_strings.get("VmSize").unwrap_or(&0)
    );
    println!(
        "     VmRSS:  {} MB",
        detailed_after_strings.get("VmRSS").unwrap_or(&0)
    );
    println!(
        "     VmData: {} MB",
        detailed_after_strings.get("VmData").unwrap_or(&0)
    );
    println!("   Bytes store:");
    println!(
        "     VmPeak: {} MB",
        detailed_after_bytes.get("VmPeak").unwrap_or(&0)
    );
    println!(
        "     VmSize: {} MB",
        detailed_after_bytes.get("VmSize").unwrap_or(&0)
    );
    println!(
        "     VmRSS:  {} MB",
        detailed_after_bytes.get("VmRSS").unwrap_or(&0)
    );
    println!(
        "     VmData: {} MB",
        detailed_after_bytes.get("VmData").unwrap_or(&0)
    );
    println!();

    println!("7. Memory comparison:");
    println!("   CRUCIBLE (string keys):");
    println!(
        "     Total RSS: {} MB",
        mem_stats_after_strings.1 - mem_stats_before.1
    );
    println!(
        "     Per key: {:.2} bytes",
        (mem_stats_after_strings.1 - mem_stats_before.1) as f64 * 1024.0 * 1024.0
            / key_count as f64
    );
    println!("   CRUCIBLE (bytes keys):");
    println!(
        "     Total RSS: {} MB",
        mem_stats_after_bytes.1 - mem_stats_before2.1
    );
    println!(
        "     Per key: {:.2} bytes",
        (mem_stats_after_bytes.1 - mem_stats_before2.1) as f64 * 1024.0 * 1024.0 / key_count as f64
    );
    println!();

    println!("MEMORY BENCHMARK FINISHED\n");
}
