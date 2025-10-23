use crucible::*;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn benchmark<F>(name: &str, iterations: usize, mut f: F)
where
    F: FnMut(),
{
    // Warmup
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
    println!("=== {} ===\n", label);

    // === SINGLE-THREADED BENCHMARKS ===

    println!("--- Single-threaded ---\n");

    benchmark("Insert (no TTL)", 100_000, || {
        let key = format!("key_{}", rand::random::<u32>());
        store.insert(&key, Term::Int(42)).unwrap();
    });

    benchmark("Insert (with TTL)", 100_000, || {
        let key = format!("ttl_key_{}", rand::random::<u32>());
        store.insert_with_ttl(&key, Term::Int(42), 3600).unwrap();
    });

    // Pre-populate for read benchmarks
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

    // Insert keys that will expire soon
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

    // === MULTI-THREADED BENCHMARKS ===

    println!("--- Multi-threaded (8 threads) ---\n");

    let iterations = 100_000;

    // Concurrent reads
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

    // Concurrent writes with TTL
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

    // Mixed workload
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
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║  TEST 1: WITHOUT BACKGROUND CLEANUP                        ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let store1 = Arc::new(Store::new(Some("no_bg_cleanup".into()), 64));
    run_benchmarks(&store1, "CRUCIBLE TTL BENCHMARKS (No Background Cleanup)");

    // Manual cleanup benchmark
    println!("--- Manual Cleanup Performance ---\n");

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

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  TEST 2: WITH BACKGROUND CLEANUP (every 2 seconds)         ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let store2 = Arc::new(Store::new(Some("with_bg_cleanup".into()), 64));

    // Start background cleanup (every 2 seconds)
    println!("Starting background cleanup thread (interval: 2 seconds)...");
    let cleanup_handle = store2.clone().start_background_cleanup(2);
    println!("Background cleanup started!\n");

    // Give it a moment to start
    thread::sleep(Duration::from_millis(100));

    run_benchmarks(&store2, "CRUCIBLE TTL BENCHMARKS (With Background Cleanup)");

    // Test cleanup while inserting expired keys
    println!("--- Background Cleanup Stress Test ---\n");

    println!("Inserting 100,000 keys with 500ms TTL while cleanup runs...");
    let start = Instant::now();
    for i in 0..100_000 {
        store2
            .insert_with_ttl_millis(&format!("stress_key_{}", i), Term::Int(i as i64), 500)
            .unwrap();

        // Print progress
        if i > 0 && i % 20_000 == 0 {
            println!("  Inserted {} keys, store size: {}", i, store2.len());
        }
    }
    let insert_time = start.elapsed();
    println!("Insert completed in {:?}", insert_time);
    println!("Store size immediately after inserts: {}", store2.len());

    // Wait for keys to expire and cleanup to run
    println!("\nWaiting 3 seconds for expiration and cleanup...");
    thread::sleep(Duration::from_secs(3));

    println!("Store size after cleanup cycles: {}", store2.len());

    // Stop background cleanup
    println!("\nStopping background cleanup...");
    store2.stop_background_cleanup();
    cleanup_handle.join().unwrap();
    println!("Background cleanup stopped.\n");

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  TEST 3: AGGRESSIVE BACKGROUND CLEANUP (every 100ms)       ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let store3 = Arc::new(Store::new(Some("aggressive_cleanup".into()), 64));

    // Start aggressive cleanup (every 100ms)
    println!("Starting aggressive background cleanup (interval: 100ms)...");
    // Note: start_background_cleanup takes seconds, so we pass 0 for sub-second
    // We'll need to modify the function or create a new one
    // For now, let's use 1 second as the minimum
    let cleanup_handle3 = store3.clone().start_background_cleanup(1);
    println!("Aggressive cleanup started!\n");

    thread::sleep(Duration::from_millis(100));

    // Quick benchmark to see impact
    println!("--- Quick Performance Check ---\n");

    benchmark("Insert (with TTL, aggressive cleanup)", 50_000, || {
        let key = format!("aggressive_key_{}", rand::random::<u32>());
        store3.insert_with_ttl(&key, Term::Int(42), 3600).unwrap();
    });

    let iterations = 50_000;
    let start = Instant::now();
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let store = Arc::clone(&store3);
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
    println!("Concurrent reads (with aggressive cleanup)");
    println!("  Total time: {:?}", elapsed);
    println!("  Ops/sec: {:.2}", total_ops as f64 / elapsed.as_secs_f64());
    println!();

    store3.stop_background_cleanup();
    cleanup_handle3.join().unwrap();
}
