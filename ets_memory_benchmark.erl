#!/usr/bin/env escript

-compile([export_all]).

-define(KEY_COUNT, 500000).
-define(VALUE_SIZE, 100).

get_memory_mb() ->
    {Mem, _} = erlang:process_info(self(), memory),
    Mem / (1024 * 1024).

get_total_memory_mb() ->
    erlang:memory(total) / (1024 * 1024).

main(_) ->
    rand:seed(default),
    
    io:format("ETS MEMORY BENCHMARK~n~n"),
    io:format("Testing with ~p keys, ~p bytes value each~n~n", [?KEY_COUNT, ?VALUE_SIZE]),
    
    io:format("1. Creating ETS table...~n"),
    Tab = ets:new(test_table, [set, public, {write_concurrency, true}]),
    MemBefore = get_total_memory_mb(),
    io:format("   Memory before: ~.2f MB~n~n", [MemBefore]),
    
    io:format("2. Inserting ~p string keys...~n", [?KEY_COUNT]),
    {Time1, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            Key = ["key_", integer_to_list(I)],
            Value = string:chars($x, ?VALUE_SIZE),
            ets:insert(Tab, {Key, Value})
        end, lists:seq(0, ?KEY_COUNT - 1))
    end),
    MemAfterStrings = get_total_memory_mb(),
    io:format("   Time: ~.2f seconds~n", [Time1 / 1000000]),
    io:format("   Memory after: ~.2f MB~n", [MemAfterStrings]),
    io:format("   Memory increase: ~.2f MB~n", [MemAfterStrings - MemBefore]),
    io:format("   Memory per key: ~.2f bytes~n~n", [(MemAfterStrings - MemBefore) * 1024 * 1024 / ?KEY_COUNT]),
    
    io:format("3. Inserting ~p binary keys...~n", [?KEY_COUNT]),
    Tab2 = ets:new(test_table2, [set, public, {write_concurrency, true}]),
    MemBefore2 = get_total_memory_mb(),
    {Time2, _} = timer:tc(fun() ->
        lists:foreach(fun(I) ->
            Key = ["bytes_key_", integer_to_list(I)],
            Value = binary:copy(<<0>>, ?VALUE_SIZE),
            ets:insert(Tab2, {Key, Value})
        end, lists:seq(0, ?KEY_COUNT - 1))
    end),
    MemAfterBytes = get_total_memory_mb(),
    io:format("   Time: ~.2f seconds~n", [Time2 / 1000000]),
    io:format("   Memory after: ~.2f MB~n", [MemAfterBytes]),
    io:format("   Memory increase: ~.2f MB~n", [MemAfterBytes - MemBefore2]),
    io:format("   Memory per key: ~.2f bytes~n~n", [(MemAfterBytes - MemBefore2) * 1024 * 1024 / ?KEY_COUNT]),
    
    io:format("4. ETS table info (string keys):~n"),
    io:format("   Table size: ~p~n", [ets:info(Tab, size)]),
    io:format("   Memory: ~p bytes~n", [ets:info(Tab, memory)]),
    io:format("   Memory per entry: ~.2f bytes~n~n", [ets:info(Tab, memory) / ets:info(Tab, size)]),
    
    io:format("5. ETS table info (binary keys):~n"),
    io:format("   Table size: ~p~n", [ets:info(Tab2, size)]),
    io:format("   Memory: ~p bytes~n", [ets:info(Tab2, memory)]),
    io:format("   Memory per entry: ~.2f bytes~n~n", [ets:info(Tab2, memory) / ets:info(Tab2, size)]),
    
    io:format("6. Memory breakdown comparison:~n"),
    io:format("   ETS (string keys):~n"),
    io:format("     Total: ~.2f MB~n", [MemAfterStrings - MemBefore]),
    io:format("     Per key: ~.2f bytes~n", [(MemAfterStrings - MemBefore) * 1024 * 1024 / ?KEY_COUNT]),
    io:format("   ETS (binary keys):~n"),
    io:format("     Total: ~.2f MB~n", [MemAfterBytes - MemBefore2]),
    io:format("     Per key: ~.2f bytes~n", [(MemAfterBytes - MemBefore2) * 1024 * 1024 / ?KEY_COUNT]),
    io:format("~n"),
    
    io:format("7. After deleting all keys:~n"),
    ets:delete(Tab),
    ets:delete(Tab2),
    garbage_collect(),
    timer:sleep(100),
    MemAfterDelete = get_total_memory_mb(),
    io:format("   Memory after GC: ~.2f MB~n", [MemAfterDelete]),
    io:format("   Memory freed: ~.2f MB~n~n", [MemAfterBytes - MemAfterDelete]),
    
    io:format("ETS MEMORY BENCHMARK COMPLETED~n").
