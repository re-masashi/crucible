-module(ets_bench).
-export([run/0]).

run() ->
    io:format("=== ETS Benchmarks ===~n~n"),
    
    % Create ETS table (similar to your 64 shards)
    Tab = ets:new(bench_table, [set, public, {read_concurrency, true}, {write_concurrency, true}]),
    
    % Pre-populate
    io:format("Pre-populating 10,000 keys...~n"),
    lists:foreach(fun(I) ->
        Key = list_to_binary(io_lib:format("test_key_~p", [I])),
        ets:insert(Tab, {Key, I})
    end, lists:seq(1, 10000)),
    
    io:format("~n--- Single-threaded ---~n~n"),
    
    % Insert benchmark
    bench_insert(Tab, 100000),
    
    % Get benchmark
    bench_get(Tab, 100000),
    
    io:format("~n--- Multi-threaded (8 threads) ---~n~n"),
    
    % Concurrent reads
    bench_concurrent_reads(Tab, 8, 100000),
    
    % Concurrent writes
    bench_concurrent_writes(Tab, 8, 100000),
    
    % Mixed workload
    bench_mixed(Tab, 8, 100000),
    
    ets:delete(Tab).

bench_insert(Tab, N) ->
    Start = erlang:monotonic_time(microsecond),
    lists:foreach(fun(_I) ->
        Key = list_to_binary(io_lib:format("key_~p", [rand:uniform(1000000)])),
        ets:insert(Tab, {Key, 42})
    end, lists:seq(1, N)),
    Duration = erlang:monotonic_time(microsecond) - Start,
    OpsPerSec = N * 1000000 / Duration,
    io:format("Insert (single-threaded)~n"),
    io:format("  Total time: ~p ms~n", [Duration / 1000]),
    io:format("  Ops/sec: ~.2f~n~n", [OpsPerSec]).

bench_get(Tab, N) ->
    Start = erlang:monotonic_time(microsecond),
    lists:foreach(fun(_I) ->
        Key = list_to_binary(io_lib:format("test_key_~p", [rand:uniform(10000)])),
        ets:lookup(Tab, Key)
    end, lists:seq(1, N)),
    Duration = erlang:monotonic_time(microsecond) - Start,
    OpsPerSec = N * 1000000 / Duration,
    io:format("Get (single-threaded)~n"),
    io:format("  Total time: ~p ms~n", [Duration / 1000]),
    io:format("  Ops/sec: ~.2f~n~n", [OpsPerSec]).

bench_concurrent_reads(Tab, NumThreads, OpsPerThread) ->
    Parent = self(),
    Start = erlang:monotonic_time(microsecond),
    
    Pids = [spawn(fun() ->
        lists:foreach(fun(_I) ->
            Key = list_to_binary(io_lib:format("test_key_~p", [rand:uniform(10000)])),
            ets:lookup(Tab, Key)
        end, lists:seq(1, OpsPerThread)),
        Parent ! {done, self()}
    end) || _ <- lists:seq(1, NumThreads)],
    
    [receive {done, Pid} -> ok end || Pid <- Pids],
    
    Duration = erlang:monotonic_time(microsecond) - Start,
    TotalOps = NumThreads * OpsPerThread,
    OpsPerSec = TotalOps * 1000000 / Duration,
    io:format("Concurrent reads (~p threads)~n", [NumThreads]),
    io:format("  Total time: ~p ms~n", [Duration / 1000]),
    io:format("  Ops/sec: ~.2f~n~n", [OpsPerSec]).

bench_concurrent_writes(Tab, NumThreads, OpsPerThread) ->
    Parent = self(),
    Start = erlang:monotonic_time(microsecond),
    
    Pids = [spawn(fun() ->
        ThreadId = erlang:unique_integer([positive]),
        lists:foreach(fun(I) ->
            Key = list_to_binary(io_lib:format("write_key_~p_~p", [ThreadId, I])),
            ets:insert(Tab, {Key, I})
        end, lists:seq(1, OpsPerThread)),
        Parent ! {done, self()}
    end) || _ <- lists:seq(1, NumThreads)],
    
    [receive {done, Pid} -> ok end || Pid <- Pids],
    
    Duration = erlang:monotonic_time(microsecond) - Start,
    TotalOps = NumThreads * OpsPerThread,
    OpsPerSec = TotalOps * 1000000 / Duration,
    io:format("Concurrent writes (~p threads)~n", [NumThreads]),
    io:format("  Total time: ~p ms~n", [Duration / 1000]),
    io:format("  Ops/sec: ~.2f~n~n", [OpsPerSec]).

bench_mixed(Tab, NumThreads, OpsPerThread) ->
    Parent = self(),
    Start = erlang:monotonic_time(microsecond),
    
    Pids = [spawn(fun() ->
        lists:foreach(fun(I) ->
            case I rem 10 of
                0 ->
                    Key = list_to_binary(io_lib:format("mixed_key_~p", [rand:uniform(10000)])),
                    ets:insert(Tab, {Key, I});
                _ ->
                    Key = list_to_binary(io_lib:format("test_key_~p", [rand:uniform(10000)])),
                    ets:lookup(Tab, Key)
            end
        end, lists:seq(1, OpsPerThread)),
        Parent ! {done, self()}
    end) || _ <- lists:seq(1, NumThreads)],
    
    [receive {done, Pid} -> ok end || Pid <- Pids],
    
    Duration = erlang:monotonic_time(microsecond) - Start,
    TotalOps = NumThreads * OpsPerThread,
    OpsPerSec = TotalOps * 1000000 / Duration,
    io:format("Mixed workload 90/10 read/write (~p threads)~n", [NumThreads]),
    io:format("  Total time: ~p ms~n", [Duration / 1000]),
    io:format("  Ops/sec: ~.2f~n~n", [OpsPerSec]).
