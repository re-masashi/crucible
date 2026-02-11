#!/usr/bin/env escript

-compile([export_all]).

-define(ITERATIONS, 100000).

benchmark(Name, Fun) ->
    % Warmup
    [Fun() || _ <- lists:seq(1, 100)],
    
    Start = erlang:monotonic_time(),
    [Fun() || _ <- lists:seq(1, ?ITERATIONS)],
    End = erlang:monotonic_time(),
    
    Elapsed = erlang:convert_time_unit(End - Start, native, microsecond),
    OpsPerSec = ?ITERATIONS / (Elapsed / 1000000),
    NsPerOp = Elapsed * 1000 / ?ITERATIONS,
    
    io:format("~s~n", [Name]),
    io:format("  Total time: ~p µs~n", [Elapsed]),
    io:format("  Ops/sec: ~w~n", [round(OpsPerSec)]),
    io:format("  ns/op: ~w~n", [round(NsPerOp)]),
    io:format("~n", []).

main(_) ->
    rand:seed(default),
    
    io:format("ETS BENCHMARK~n~n"),
    
    % Test 1: Basic Insert (no TTL)
    Tab = ets:new(test_table, [set, public, {write_concurrency, true}]),
    
    io:format("Single-threaded~n~n"),
    
    benchmark("Insert (no TTL)", fun() ->
        Key = ["key_", integer_to_list(rand:uniform(4294967295))],
        ets:insert(Tab, {Key, 42})
    end),
    
    % Pre-populate for read tests
    io:format("Pre-populating 10,000 keys...~n"),
    lists:foreach(fun(I) ->
        Key = ["test_key_", integer_to_list(I)],
        ets:insert(Tab, {Key, I, no_expiry})
    end, lists:seq(0, 4999)),
    lists:foreach(fun(I) ->
        Key = ["test_key_", integer_to_list(I)],
        Expiry = erlang:system_time(millisecond) + 3600000,
        ets:insert(Tab, {Key, I, {expiry, Expiry}})
    end, lists:seq(5000, 9999)),
    io:format("Done.~n~n"),
    
    benchmark("Get (no TTL keys)", fun() ->
        Key = ["test_key_", integer_to_list(rand:uniform(5000) - 1)],
        case ets:lookup(Tab, Key) of
            [{Key, _, no_expiry}] -> ok;
            _ -> ok
        end
    end),
    
    benchmark("Get (with TTL, not expired)", fun() ->
        Key = ["test_key_", integer_to_list(5000 + rand:uniform(5000) - 1)],
        case ets:lookup(Tab, Key) of
            [{Key, _, {expiry, T}}] -> 
                Now = erlang:system_time(millisecond),
                if T > Now -> ok; true -> ok end;
            _ -> ok
        end
    end),
    
    % Insert expired keys for lazy delete test
    io:format("Pre-populating 5,000 keys with 100ms TTL (will expire)...~n"),
    Now = erlang:system_time(millisecond),
    lists:foreach(fun(I) ->
        Key = ["test_key_", integer_to_list(I)],
        Expiry = Now + 50,
        ets:insert(Tab, {Key, I, {expiry, Expiry}})
    end, lists:seq(10000, 14999)),
    timer:sleep(100),
    io:format("Done.~n~n"),
    
    benchmark("Get (expired keys, lazy delete)", fun() ->
        KeyNum = 10000 + rand:uniform(5000) - 1,
        Key = ["test_key_", integer_to_list(KeyNum)],
        case ets:lookup(Tab, Key) of
            [{_K, _V, {expiry, T}}] -> 
                ets:delete(Tab, Key);
            _ -> ok
        end
    end),
    
    benchmark("Contains key", fun() ->
        Key = ["test_key_", integer_to_list(rand:uniform(10000) - 1)],
        ets:member(Tab, Key)
    end),
    
    io:format("Multi-threaded (8 threads)~n~n"),
    
    % Concurrent reads
    Parent = self(),
    ReadsPerThread = ?ITERATIONS div 8,
    
    spawn_workers(8, fun(_) ->
        [begin
            Key = ["test_key_", integer_to_list(rand:uniform(10000) - 1)],
            ets:lookup(Tab, Key)
        end || _ <- lists:seq(1, ReadsPerThread)],
        Parent ! done
    end),
    
    Start = erlang:monotonic_time(),
    wait_for_workers(8),
    End = erlang:monotonic_time(),
    Elapsed = erlang:convert_time_unit(End - Start, native, microsecond),
    TotalOps = ?ITERATIONS,
    io:format("Concurrent reads~n"),
    io:format("  Total time: ~p µs~n", [Elapsed]),
    io:format("  Ops/sec: ~w~n", [round(TotalOps / (Elapsed / 1000000))]),
    io:format("  ns/op: ~w~n", [round(Elapsed * 1000 / TotalOps)]),
    io:format("~n", []),
    
    % Concurrent writes
    spawn_workers(8, fun(_) ->
        [begin
            Key = ["write_ttl_", integer_to_list(I)],
            ets:insert(Tab, {Key, I, {expiry, erlang:system_time(millisecond) + 3600000}})
        end || I <- lists:seq(1, ReadsPerThread)],
        Parent ! done
    end),
    
    Start2 = erlang:monotonic_time(),
    wait_for_workers(8),
    End2 = erlang:monotonic_time(),
    Elapsed2 = erlang:convert_time_unit(End2 - Start2, native, microsecond),
    io:format("Concurrent writes~n"),
    io:format("  Total time: ~p µs~n", [Elapsed2]),
    io:format("  Ops/sec: ~w~n", [round(TotalOps / (Elapsed2 / 1000000))]),
    io:format("  ns/op: ~w~n", [round(Elapsed2 * 1000 / TotalOps)]),
    io:format("~n", []),
    
    io:format("Final table size: ~p~n", [ets:info(Tab, size)]),
    
    ets:delete(Tab).

spawn_workers(0, _Fun) -> ok;
spawn_workers(N, Fun) ->
    spawn(fun() -> Fun(N) end),
    spawn_workers(N - 1, Fun).

wait_for_workers(0) -> ok;
wait_for_workers(N) ->
    receive
        done -> wait_for_workers(N - 1)
    end.
