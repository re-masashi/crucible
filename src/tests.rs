use crate::*;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_insert_and_get() {
    let store = Store::new(Some("test".into()), 16);

    store.insert("key1", Term::Int(42)).unwrap();
    store
        .insert("key2", Term::Float(std::f64::consts::PI))
        .unwrap();
    store.insert("key3", Term::Bool(true)).unwrap();

    assert_eq!(store.get("key1").unwrap(), Term::Int(42));
    assert_eq!(
        store.get("key2").unwrap(),
        Term::Float(std::f64::consts::PI)
    );
    assert_eq!(store.get("key3").unwrap(), Term::Bool(true));
}

#[test]
fn test_get_nonexistent_key() {
    let store = Store::new(Some("test".into()), 16);

    match store.get("nonexistent") {
        Err(StoreError::KeyNotFound) => {}
        _ => panic!("Expected KeyNotFound error"),
    }
}

#[test]
fn test_update() {
    let store = Store::new(Some("test".into()), 16);

    store.insert("key", Term::Int(42)).unwrap();
    assert_eq!(store.get("key").unwrap(), Term::Int(42));

    let old = store.update("key", Term::Int(100)).unwrap();
    assert_eq!(old, Term::Int(42));
    assert_eq!(store.get("key").unwrap(), Term::Int(100));
}

#[test]
fn test_update_nonexistent_key() {
    let store = Store::new(Some("test".into()), 16);

    match store.update("nonexistent", Term::Int(42)) {
        Err(StoreError::KeyNotFound) => {}
        _ => panic!("Expected KeyNotFound error"),
    }
}

#[test]
fn test_remove() {
    let store = Store::new(Some("test".into()), 16);

    store.insert("key", Term::Int(42)).unwrap();
    assert!(store.contains_key("key"));

    let removed = store.remove("key").unwrap();
    assert_eq!(removed, Term::Int(42));
    assert!(!store.contains_key("key"));
}

#[test]
fn test_remove_nonexistent_key() {
    let store = Store::new(Some("test".into()), 16);

    match store.remove("nonexistent") {
        Err(StoreError::KeyNotFound) => {}
        _ => panic!("Expected KeyNotFound error"),
    }
}

#[test]
fn test_contains_key() {
    let store = Store::new(Some("test".into()), 16);

    assert!(!store.contains_key("key"));

    store.insert("key", Term::Int(42)).unwrap();
    assert!(store.contains_key("key"));

    store.remove("key").unwrap();
    assert!(!store.contains_key("key"));
}

#[test]
fn test_len_and_is_empty() {
    let store = Store::new(Some("test".into()), 16);

    assert_eq!(store.len(), 0);
    assert!(store.is_empty());

    store.insert("key1", Term::Int(1)).unwrap();
    store.insert("key2", Term::Int(2)).unwrap();
    store.insert("key3", Term::Int(3)).unwrap();

    assert_eq!(store.len(), 3);
    assert!(!store.is_empty());

    store.clear();
    assert_eq!(store.len(), 0);
    assert!(store.is_empty());
}

#[test]
fn test_clear() {
    let store = Store::new(Some("test".into()), 16);

    for i in 0..100 {
        store.insert(&format!("key_{}", i), Term::Int(i)).unwrap();
    }

    assert_eq!(store.len(), 100);
    store.clear();
    assert_eq!(store.len(), 0);
}

// TTL TESTS
#[test]
fn test_insert_with_ttl_seconds() {
    let store = Store::new(Some("test".into()), 16);

    store.insert_with_ttl("key", Term::Int(42), 3600).unwrap();

    // Should be retrievable immediately
    assert_eq!(store.get("key").unwrap(), Term::Int(42));

    // Check TTL exists
    let ttl = store.ttl("key");
    assert!(ttl.is_some());
    assert!(ttl.unwrap() > 3500 && ttl.unwrap() <= 3600);
}

#[test]
fn test_insert_with_ttl_millis() {
    let store = Store::new(Some("test".into()), 16);

    store
        .insert_with_ttl_millis("key", Term::Int(42), 5000)
        .unwrap();

    assert_eq!(store.get("key").unwrap(), Term::Int(42));

    let ttl_ms = store.ttl_millis("key");
    assert!(ttl_ms.is_some());
    assert!(ttl_ms.unwrap() > 4500 && ttl_ms.unwrap() <= 5000);
}

#[test]
fn test_ttl_expiration() {
    let store = Store::new(Some("test".into()), 16);

    // Insert key with 100ms TTL
    store
        .insert_with_ttl_millis("key", Term::Int(42), 100)
        .unwrap();

    // Should exist immediately
    assert_eq!(store.get("key").unwrap(), Term::Int(42));

    // Wait for expiration
    thread::sleep(Duration::from_millis(150));

    // Should be expired (lazy deletion)
    match store.get("key") {
        Err(StoreError::KeyNotFound) => {}
        Ok(_) => panic!("Key should have expired"),
        Err(StoreError::ArenaError(_)) => panic!("shouldnt happen"),
        Err(StoreError::TypeError) => panic!("shouldnt happen"),
    }
}

#[test]
fn test_ttl_no_expiration() {
    let store = Store::new(Some("test".into()), 16);

    // Insert without TTL
    store.insert("key", Term::Int(42)).unwrap();

    // TTL should be 0 (no expiration)
    assert_eq!(store.ttl("key"), Some(0));
}

#[test]
fn test_expire() {
    let store = Store::new(Some("test".into()), 16);

    // Insert permanent key
    store.insert("key", Term::Int(42)).unwrap();
    assert_eq!(store.ttl("key"), Some(0));

    // Set TTL
    store.expire("key", 3600).unwrap();

    let ttl = store.ttl("key").unwrap();
    assert!(ttl > 3500 && ttl <= 3600);
}

#[test]
fn test_expire_millis() {
    let store = Store::new(Some("test".into()), 16);

    store.insert("key", Term::Int(42)).unwrap();
    store.expire_millis("key", 5000).unwrap();

    let ttl_ms = store.ttl_millis("key").unwrap();
    assert!(ttl_ms > 4500 && ttl_ms <= 5000);
}

#[test]
fn test_persist() {
    let store = Store::new(Some("test".into()), 16);

    // Insert with TTL
    store.insert_with_ttl("key", Term::Int(42), 3600).unwrap();
    assert!(store.ttl("key").unwrap() > 0);

    // Remove TTL
    store.persist("key").unwrap();
    assert_eq!(store.ttl("key"), Some(0));
}

#[test]
fn test_update_preserves_ttl() {
    let store = Store::new(Some("test".into()), 16);

    // Insert with TTL
    store.insert_with_ttl("key", Term::Int(42), 3600).unwrap();
    let ttl_before = store.ttl("key").unwrap();

    // Update value
    store.update("key", Term::Int(100)).unwrap();

    // TTL should be preserved (approximately)
    let ttl_after = store.ttl("key").unwrap();
    assert!(ttl_after >= ttl_before - 2); // Allow 2 second drift
}

#[test]
fn test_contains_key_expired() {
    let store = Store::new(Some("test".into()), 16);

    store
        .insert_with_ttl_millis("key", Term::Int(42), 100)
        .unwrap();
    assert!(store.contains_key("key"));

    thread::sleep(Duration::from_millis(150));
    assert!(!store.contains_key("key"));
}

#[test]
fn test_ttl_on_expired_key() {
    let store = Store::new(Some("test".into()), 16);

    store
        .insert_with_ttl_millis("key", Term::Int(42), 100)
        .unwrap();
    thread::sleep(Duration::from_millis(150));

    // TTL should return None for expired key
    assert_eq!(store.ttl("key"), None);
}

#[test]
fn test_cleanup_expired() {
    let store = Store::new(Some("test".into()), 16);

    // Insert keys with short TTL
    for i in 0..100 {
        store
            .insert_with_ttl_millis(&format!("expire_{}", i), Term::Int(i), 100)
            .unwrap();
    }

    // Insert permanent keys
    for i in 0..50 {
        store
            .insert(&format!("permanent_{}", i), Term::Int(i))
            .unwrap();
    }

    assert_eq!(store.len(), 150);

    // Wait for expiration
    thread::sleep(Duration::from_millis(150));

    // Cleanup
    let removed = store.cleanup_expired();
    assert_eq!(removed, 100);
    assert_eq!(store.len(), 50);
}

#[test]
fn test_cleanup_no_expired_keys() {
    let store = Store::new(Some("test".into()), 16);

    for i in 0..100 {
        store.insert(&format!("key_{}", i), Term::Int(i)).unwrap();
    }

    let removed = store.cleanup_expired();
    assert_eq!(removed, 0);
    assert_eq!(store.len(), 100);
}

#[test]
fn test_cleanup_mixed_expiration() {
    let store = Store::new(Some("test".into()), 16);

    // Insert keys that expire at different times
    for i in 0..30 {
        store
            .insert_with_ttl_millis(&format!("short_{}", i), Term::Int(i), 50)
            .unwrap();
    }
    for i in 0..30 {
        store
            .insert_with_ttl_millis(&format!("medium_{}", i), Term::Int(i), 150)
            .unwrap();
    }
    for i in 0..30 {
        store
            .insert_with_ttl(&format!("long_{}", i), Term::Int(i), 3600)
            .unwrap();
    }

    assert_eq!(store.len(), 90);

    // Wait for short TTL to expire
    thread::sleep(Duration::from_millis(100));
    let removed = store.cleanup_expired();
    assert_eq!(removed, 30);
    assert_eq!(store.len(), 60);

    // Wait for medium TTL to expire
    thread::sleep(Duration::from_millis(100));
    let removed = store.cleanup_expired();
    assert_eq!(removed, 30);
    assert_eq!(store.len(), 30);
}

#[test]
fn test_check_expired_count() {
    let store = Store::new(Some("test".into()), 16);

    for i in 0..50 {
        store
            .insert_with_ttl_millis(&format!("expire_{}", i), Term::Int(i), 100)
            .unwrap();
    }
    for i in 0..50 {
        store
            .insert(&format!("permanent_{}", i), Term::Int(i))
            .unwrap();
    }

    thread::sleep(Duration::from_millis(150));

    let (expired, not_expired) = store.check_expired_count();
    assert_eq!(expired, 50);
    assert_eq!(not_expired, 50);
}

#[test]
fn test_concurrent_inserts() {
    let store = Arc::new(Store::new(Some("test".into()), 64));
    let mut handles = vec![];

    for thread_id in 0..8 {
        let store = Arc::clone(&store);
        let handle = thread::spawn(move || {
            for i in 0..1000 {
                let key = format!("thread_{}_{}", thread_id, i);
                store.insert(&key, Term::Int(i)).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(store.len(), 8000);
}

#[test]
fn test_concurrent_reads() {
    let store = Arc::new(Store::new(Some("test".into()), 64));

    // Pre-populate
    for i in 0..1000 {
        store.insert(&format!("key_{}", i), Term::Int(i)).unwrap();
    }

    let mut handles = vec![];

    for _ in 0..8 {
        let store = Arc::clone(&store);
        let handle = thread::spawn(move || {
            for i in 0..1000 {
                let key = format!("key_{}", i % 1000);
                assert!(store.get(&key).is_ok());
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_concurrent_mixed_operations() {
    let store = Arc::new(Store::new(Some("test".into()), 64));
    let mut handles = vec![];

    // Pre-populate
    for i in 0..1000 {
        store.insert(&format!("key_{}", i), Term::Int(i)).unwrap();
    }

    for thread_id in 0..8 {
        let store = Arc::clone(&store);
        let handle = thread::spawn(move || {
            for i in 0..500 {
                let key = format!("key_{}", i % 1000);

                if thread_id % 2 == 0 {
                    // Read
                    let _ = store.get(&key);
                } else {
                    // Write
                    let _ = store.update(&key, Term::Int(thread_id as i64));
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(store.len(), 1000);
}

#[test]
fn test_concurrent_inserts_with_ttl() {
    let store = Arc::new(Store::new(Some("test".into()), 64));
    let mut handles = vec![];

    for thread_id in 0..8 {
        let store = Arc::clone(&store);
        let handle = thread::spawn(move || {
            for i in 0..1000 {
                let key = format!("thread_{}_{}", thread_id, i);
                store.insert_with_ttl(&key, Term::Int(i), 3600).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(store.len(), 8000);
}

#[test]
fn test_concurrent_cleanup() {
    let store = Arc::new(Store::new(Some("test".into()), 64));

    // Insert keys with short TTL
    for i in 0..1000 {
        store
            .insert_with_ttl_millis(&format!("key_{}", i), Term::Int(i), 100)
            .unwrap();
    }

    thread::sleep(Duration::from_millis(150));

    // Multiple threads cleanup simultaneously
    let mut handles = vec![];
    for _ in 0..4 {
        let store = Arc::clone(&store);
        let handle = thread::spawn(move || store.cleanup_expired());
        handles.push(handle);
    }

    let mut total_removed = 0;
    for handle in handles {
        total_removed += handle.join().unwrap();
    }

    // Should have removed all 1000 keys (distributed across threads)
    assert_eq!(total_removed, 1000);
    assert_eq!(store.len(), 0);
}

#[test]
fn test_background_cleanup() {
    let store = Arc::new(Store::new(Some("test".into()), 64));

    // Start background cleanup (every 1 second)
    let cleanup_handle = store.clone().start_background_cleanup(1);

    // Insert keys with 500ms TTL
    for i in 0..100 {
        store
            .insert_with_ttl_millis(&format!("key_{}", i), Term::Int(i), 500)
            .unwrap();
    }

    assert_eq!(store.len(), 100);

    // Wait for expiration and cleanup
    thread::sleep(Duration::from_millis(1500));

    // Keys should be cleaned up
    assert_eq!(store.len(), 0);

    // Stop cleanup
    store.stop_background_cleanup();
    cleanup_handle.join().unwrap();
}

#[test]
fn test_string_interning() {
    let store = Store::new(Some("test".into()), 16);

    let id1 = store.intern("hello");
    let id2 = store.intern("hello");
    let id3 = store.intern("world");

    // Same string should return same ID
    assert_eq!(id1, id2);
    assert_ne!(id1, id3);

    // Resolve IDs
    assert_eq!(store.resolve(id1), "hello");
    assert_eq!(store.resolve(id3), "world");
}

#[test]
fn test_string_interning_concurrent() {
    let store = Arc::new(Store::new(Some("test".into()), 64));
    let mut handles = vec![];

    for _ in 0..8 {
        let store = Arc::clone(&store);
        let handle = thread::spawn(move || {
            for _ in 0..100 {
                let id = store.intern("concurrent_test");
                assert_eq!(store.resolve(id), "concurrent_test");
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_empty_key() {
    let store = Store::new(Some("test".into()), 16);

    store.insert("", Term::Int(42)).unwrap();
    assert_eq!(store.get("").unwrap(), Term::Int(42));
}

#[test]
fn test_very_long_key() {
    let store = Store::new(Some("test".into()), 16);

    let long_key = "a".repeat(10000);
    store.insert(&long_key, Term::Int(42)).unwrap();
    assert_eq!(store.get(&long_key).unwrap(), Term::Int(42));
}

#[test]
fn test_special_characters_in_key() {
    let store = Store::new(Some("test".into()), 16);

    let keys = [
        "key with spaces",
        "key:with:colons",
        "key/with/slashes",
        "key🔥with🚀emoji",
        "key\nwith\nnewlines",
    ];

    for (i, key) in keys.iter().enumerate() {
        store.insert(key, Term::Int(i as i64)).unwrap();
    }

    for (i, key) in keys.iter().enumerate() {
        assert_eq!(store.get(key).unwrap(), Term::Int(i as i64));
    }
}

#[test]
fn test_large_number_of_keys() {
    let store = Store::new(Some("test".into()), 64);

    for i in 0..10_000 {
        store.insert(&format!("key_{}", i), Term::Int(i)).unwrap();
    }

    assert_eq!(store.len(), 10_000);

    for i in 0..10_000 {
        assert_eq!(store.get(&format!("key_{}", i)).unwrap(), Term::Int(i));
    }
}

#[test]
fn test_term_types() {
    let store = Store::new(Some("test".into()), 16);

    store.insert("int", Term::Int(42)).unwrap();
    store
        .insert("float", Term::Float(std::f64::consts::PI))
        .unwrap();
    store.insert("bool_true", Term::Bool(true)).unwrap();
    store.insert("bool_false", Term::Bool(false)).unwrap();
    store.insert("unit", Term::Unit).unwrap();
    store.insert("any", Term::Any).unwrap();

    assert_eq!(store.get("int").unwrap(), Term::Int(42));
    assert_eq!(
        store.get("float").unwrap(),
        Term::Float(std::f64::consts::PI)
    );
    assert_eq!(store.get("bool_true").unwrap(), Term::Bool(true));
    assert_eq!(store.get("bool_false").unwrap(), Term::Bool(false));
    assert_eq!(store.get("unit").unwrap(), Term::Unit);
    assert_eq!(store.get("any").unwrap(), Term::Any);
}

#[test]
fn test_overwrite_existing_key() {
    let store = Store::new(Some("test".into()), 16);

    store.insert("key", Term::Int(1)).unwrap();
    assert_eq!(store.get("key").unwrap(), Term::Int(1));

    store.insert("key", Term::Int(2)).unwrap();
    assert_eq!(store.get("key").unwrap(), Term::Int(2));

    store.insert("key", Term::Int(3)).unwrap();
    assert_eq!(store.get("key").unwrap(), Term::Int(3));
}

#[test]
#[allow(clippy::needless_range_loop)]
fn test_shard_distribution() {
    let store = Store::new(Some("test".into()), 16);

    // Insert 1000 keys and check they're distributed
    for i in 0..1000 {
        store.insert(&format!("key_{}", i), Term::Int(i)).unwrap();
    }

    // Count keys per shard
    let mut shard_counts = [0; 16];
    for i in 0..16 {
        shard_counts[i] = store.shards[i].map.len();
    }

    // Each shard should have some keys (very unlikely all go to one shard)
    let max_count = *shard_counts.iter().max().unwrap();
    let min_count = *shard_counts.iter().min().unwrap();

    // Distribution shouldn't be perfect, but shouldn't be terrible
    assert!(min_count > 0, "Some shard has no keys!");
    assert!(max_count < 200, "Distribution is very skewed!");
}

#[test]
fn test_default_shard_count() {
    let store = Store::new_default(Some("test".into()));

    // Should be power of 2
    assert!(store.shards.len().is_power_of_two());

    // Should be reasonable (at least 16, at most 256)
    assert!(store.shards.len() >= 16);
    assert!(store.shards.len() <= 256);
}

#[test]
#[ignore] // Run with: cargo test -- --ignored
fn stress_test_high_throughput() {
    let store = Arc::new(Store::new(Some("stress".into()), 64));
    let mut handles = vec![];

    for thread_id in 0..16 {
        let store = Arc::clone(&store);
        let handle = thread::spawn(move || {
            for i in 0..10_000 {
                let key = format!("thread_{}_{}", thread_id, i);
                store.insert_with_ttl(&key, Term::Int(i), 3600).unwrap();

                if i % 2 == 0 {
                    let _ = store.get(&key);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert!(store.len() >= 160_000);
}

#[test]
#[ignore]
fn stress_test_cleanup_under_load() {
    let store = Arc::new(Store::new(Some("stress".into()), 64));

    // Start background cleanup
    let cleanup_handle = store.clone().start_background_cleanup(1);

    let mut handles = vec![];

    // Continuously insert keys with short TTL
    for thread_id in 0..8 {
        let store = Arc::clone(&store);
        let handle = thread::spawn(move || {
            for i in 0..5_000 {
                let key = format!("thread_{}_{}", thread_id, i);
                store
                    .insert_with_ttl_millis(&key, Term::Int(i), 200)
                    .unwrap();
                thread::sleep(Duration::from_micros(100));
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Wait for cleanup
    thread::sleep(Duration::from_secs(2));

    // Most keys should be cleaned up
    assert!(store.len() < 1000, "Cleanup didn't work under load");

    store.stop_background_cleanup();
    cleanup_handle.join().unwrap();
}

#[test]
fn test_typed_insert_and_get() {
    let store = Store::new(Some("typed_test".into()), 16);

    // Insert with type inference
    store.insert_typed("int_key", 42_i64).unwrap();
    store
        .insert_typed("float_key", std::f64::consts::PI)
        .unwrap();
    store.insert_typed("bool_key", true).unwrap();
    store.insert_typed("unit_key", ()).unwrap();

    // Get with type annotation
    let int_val: i64 = store.get_typed("int_key").unwrap();
    assert_eq!(int_val, 42);

    let float_val: f64 = store.get_typed("float_key").unwrap();
    assert_eq!(float_val, std::f64::consts::PI);

    let bool_val: bool = store.get_typed("bool_key").unwrap();
    assert!(bool_val);

    let unit_val: () = store.get_typed("unit_key").unwrap();
    assert_eq!(unit_val, ());
}

#[test]
fn test_typed_type_error() {
    let store = Store::new(Some("typed_test".into()), 16);

    // Insert as i64
    store.insert_typed("key", 42_i64).unwrap();

    // Try to get as f64 - should error
    let result: Result<f64, _> = store.get_typed("key");
    match result {
        Err(StoreError::TypeError) => {}
        _ => panic!("Expected TypeError"),
    }
}

#[test]
fn test_typed_update() {
    let store = Store::new(Some("typed_test".into()), 16);

    store.insert_typed("count", 10_i64).unwrap();

    let old: i64 = store.update_typed("count", 20_i64).unwrap();
    assert_eq!(old, 10);

    let current: i64 = store.get_typed("count").unwrap();
    assert_eq!(current, 20);
}

#[test]
fn test_typed_with_ttl() {
    let store = Store::new(Some("typed_test".into()), 16);

    // Insert with TTL
    store
        .insert_typed_with_ttl("session", 123_i64, 3600)
        .unwrap();

    let value: i64 = store.get_typed("session").unwrap();
    assert_eq!(value, 123);

    // Check TTL exists
    assert!(store.ttl("session").unwrap() > 3500);
}

#[test]
fn test_typed_with_ttl_millis() {
    let store = Store::new(Some("typed_test".into()), 16);

    store
        .insert_typed_with_ttl_millis("temp", 99_i64, 5000)
        .unwrap();

    let value: i64 = store.get_typed("temp").unwrap();
    assert_eq!(value, 99);
}

#[test]
fn test_typed_remove() {
    let store = Store::new(Some("typed_test".into()), 16);

    store.insert_typed("key", 42_i64).unwrap();

    let removed: i64 = store.remove_typed("key").unwrap();
    assert_eq!(removed, 42);

    assert!(!store.contains_key("key"));
}

#[test]
fn test_typed_concurrent() {
    use std::sync::Arc;
    use std::thread;

    let store = Arc::new(Store::new(Some("typed_concurrent".into()), 64));
    let mut handles = vec![];

    for thread_id in 0..8 {
        let store = Arc::clone(&store);
        let handle = thread::spawn(move || {
            for i in 0..1000 {
                let key = format!("thread_{}_{}", thread_id, i);
                store.insert_typed(&key, i as i64).unwrap();

                let value: i64 = store.get_typed(&key).unwrap();
                assert_eq!(value, i as i64);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(store.len(), 8000);
}

#[test]
fn test_typed_zero_cost() {
    // This test verifies that typed and dynamic APIs have same performance
    let store = Store::new(Some("perf_test".into()), 16);

    // Both should compile to identical code
    store.insert_typed("typed", 42_i64).unwrap();
    store.insert("dynamic", Term::Int(42)).unwrap();

    let typed_val: i64 = store.get_typed("typed").unwrap();
    let dynamic_val = store.get("dynamic").unwrap();

    assert_eq!(typed_val, 42);
    assert_eq!(dynamic_val, Term::Int(42));
}

#[test]
fn test_string_insert_and_get() {
    let store = Store::new(Some("string_test".into()), 16);

    store.insert_string("greeting", "Hello, World!").unwrap();
    store.insert_string("name", "Alice").unwrap();
    store.insert_string("empty", "").unwrap();

    assert_eq!(store.get_string("greeting").unwrap(), "Hello, World!");
    assert_eq!(store.get_string("name").unwrap(), "Alice");
    assert_eq!(store.get_string("empty").unwrap(), "");
}

#[test]
fn test_string_with_ttl() {
    let store = Store::new(Some("string_test".into()), 16);

    store
        .insert_string_with_ttl("session_token", "abc123xyz", 3600)
        .unwrap();

    assert_eq!(store.get_string("session_token").unwrap(), "abc123xyz");
    assert!(store.ttl("session_token").unwrap() > 3500);
}

#[test]
fn test_string_unicode() {
    let store = Store::new(Some("string_test".into()), 16);

    let unicode_strings = [
        "Hello, 世界",
        "🔥🚀✨",
        "Привет мир",
        "مرحبا بالعالم",
        "こんにちは",
    ];

    for (i, s) in unicode_strings.iter().enumerate() {
        let key = format!("unicode_{}", i);
        store.insert_string(&key, s).unwrap();
    }

    for (i, expected) in unicode_strings.iter().enumerate() {
        let key = format!("unicode_{}", i);
        assert_eq!(store.get_string(&key).unwrap(), *expected);
    }
}

#[test]
fn test_string_update() {
    let store = Store::new(Some("string_test".into()), 16);

    store.insert_string("message", "old value").unwrap();

    let old = store.update_string("message", "new value").unwrap();
    assert_eq!(old, "old value");

    assert_eq!(store.get_string("message").unwrap(), "new value");
}

#[test]
fn test_string_remove() {
    let store = Store::new(Some("string_test".into()), 16);

    store.insert_string("temp", "temporary").unwrap();

    let removed = store.remove_string("temp").unwrap();
    assert_eq!(removed, "temporary");

    assert!(!store.contains_key("temp"));
}

#[test]
fn test_string_type_error() {
    let store = Store::new(Some("string_test".into()), 16);

    // Insert as integer
    store.insert_typed("number", 42_i64).unwrap();

    // Try to get as string - should error
    match store.get_string("number") {
        Err(StoreError::TypeError) => {}
        _ => panic!("Expected TypeError"),
    }
}

#[test]
fn test_string_concurrent() {
    use std::sync::Arc;
    use std::thread;

    let store = Arc::new(Store::new(Some("string_concurrent".into()), 64));
    let mut handles = vec![];

    for thread_id in 0..8 {
        let store = Arc::clone(&store);
        let handle = thread::spawn(move || {
            for i in 0..1000 {
                let key = format!("thread_{}_{}", thread_id, i);
                let value = format!("value_{}_{}", thread_id, i);

                store.insert_string(&key, &value).unwrap();

                let retrieved = store.get_string(&key).unwrap();
                assert_eq!(retrieved, value);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_string_very_long() {
    let store = Store::new(Some("string_test".into()), 16);

    // 1MB string
    let long_string = "a".repeat(1_000_000);

    store.insert_string("long", &long_string).unwrap();
    let retrieved = store.get_string("long").unwrap();

    assert_eq!(retrieved.len(), 1_000_000);
    assert_eq!(retrieved, long_string);
}

#[test]
fn test_bytes_insert_and_get() {
    let store = Store::new(Some("bytes_test".into()), 16);

    let data = vec![1, 2, 3, 4, 5];
    store.insert_bytes("data", &data).unwrap();

    let retrieved = store.get_bytes("data").unwrap();
    assert_eq!(retrieved, data);
}

#[test]
fn test_bytes_empty() {
    let store = Store::new(Some("bytes_test".into()), 16);

    store.insert_bytes("empty", &[]).unwrap();

    let retrieved = store.get_bytes("empty").unwrap();
    assert_eq!(retrieved, Vec::<u8>::new());
}

#[test]
fn test_bytes_with_ttl() {
    let store = Store::new(Some("bytes_test".into()), 16);

    let data = vec![0xFF, 0xAA, 0x55];
    store.insert_bytes_with_ttl("blob", &data, 3600).unwrap();

    let retrieved = store.get_bytes("blob").unwrap();
    assert_eq!(retrieved, data);
}

#[test]
fn test_bytes_binary_data() {
    let store = Store::new(Some("bytes_test".into()), 16);

    // Binary data (not valid UTF-8)
    let binary = vec![0x00, 0x01, 0xFF, 0xFE, 0x80, 0x7F];

    store.insert_bytes("binary", &binary).unwrap();
    let retrieved = store.get_bytes("binary").unwrap();

    assert_eq!(retrieved, binary);
}

#[test]
fn test_bytes_large() {
    let store = Store::new(Some("bytes_test".into()), 16);

    // 1MB of binary data
    let large_data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

    store.insert_bytes("large", &large_data).unwrap();
    let retrieved = store.get_bytes("large").unwrap();

    assert_eq!(retrieved.len(), 1_000_000);
    assert_eq!(retrieved, large_data);
}
