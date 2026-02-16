# Crucible

An in memory key-value store intended to be used as a cache, much like [ETS](https://www.erlang.org/doc/apps/stdlib/ets.html). Language agnostic and (hopefully) can be embedded in VMs.
It is not indended to be ETS

## Peformance

<img width="1020" height="592" alt="image" src="https://github.com/user-attachments/assets/f3a70daa-f579-406d-a20d-d93834485049" />

(Benchmarked on an HP ProBook 430 G2 with i5 5200U and 8 GB DDR3 RAM)

## TODO

- [ ] LRU eviction
- [ ] Deletion
- [ ] Making it use less memory
- [ ] PoC usage in a backend (for presence, caching, etc)

