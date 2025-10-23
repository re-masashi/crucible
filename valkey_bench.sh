# Start valkey server
valkey-server &

# Wait a second for it to start
sleep 1

echo "=== valkey Benchmarks ==="
echo ""
echo "--- Single-threaded ---"
echo ""

# SET (insert)
echo "Insert (single-threaded)"
valkey-benchmark -t set -n 100000 -q -c 1 --threads 1 | grep -E "requests per second|latency"

echo ""

# GET (read)
echo "Get (single-threaded)"
valkey-benchmark -t get -n 100000 -q -c 1 --threads 1 | grep -E "requests per second|latency"

echo ""
echo "--- Multi-threaded (8 clients) ---"
echo ""

# Concurrent reads
echo "Concurrent reads (8 clients)"
valkey-benchmark -t get -n 800000 -q -c 8 --threads 8 | grep -E "requests per second|latency"

echo ""

# Concurrent writes
echo "Concurrent writes (8 clients)"
valkey-benchmark -t set -n 800000 -q -c 8 --threads 8 | grep -E "requests per second|latency"

echo ""

# Mixed (using LPUSH + LPOP as proxy)
echo "Mixed workload (approximate)"
valkey-benchmark -n 800000 -q -c 8 --threads 8 | grep -E "requests per second" | head -3

# Kill valkey
pkill valkey-server
