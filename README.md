# MARPUL — Execution Engine


MARPUL is an intraday algorithmic trading system for NSE (India) equities,
built around a real-time market-data pipeline, a rule-based entry/exit
signal engine, and an order-execution layer that talks to a broker's REST
API. It runs as three cooperating processes on a small ARM VPS, pinned to
dedicated cores, communicating over a lock-free ring buffer (within a
process) and ZeroMQ (between processes).

This repository is a focused extract of the **low-latency execution path**
and the **order-execution / position-lifecycle module**, pulled out of the
full private codebase for a portfolio writeup. It isn't the whole system —
the signal-generation logic, tuned thresholds, and broker credentials stay
private — but the two pieces here are real, complete, and independently
buildable.

## What's in here and what it maps to

| File | What it is | Buildable here? |
|---|---|---|
| `include/marpul/ring_buffer.hpp` | Lock-free MPSC ring buffer with per-slot sequencing and cache-line-padded atomics, used on the market-data ingestion path between the feed handler and the calculation thread. | Yes |
| `include/marpul/cpu_affinity.hpp` | Core pinning, `SCHED_FIFO` real-time priority, `mlockall`, and stack pre-touch helpers used to keep the hot threads off the default scheduler. | Yes |
| `bench/ring_buffer_bench.cpp` | Producer/consumer benchmark: publishes timestamped messages through the ring buffer and reports p50/p95/p99/max latency, with and without core pinning. | Yes |
| `src/execution_engine.cpp` | Order-execution and position-lifecycle module: turns an entry signal into a sized, placed, and monitored order against the broker, and manages exits (targets, trailing stop, a resting catastrophe backstop). | Reference only — see below |

If you're matching this repo against a resume line about lock-free ring
buffers, cache-line padding, and CPU pinning: that's `ring_buffer.hpp` +
`cpu_affinity.hpp`, and it's genuinely runnable — see the benchmark below.
`execution_engine.cpp` is a different part of the system (order placement
and position management) and doesn't use the ring buffer itself; it's
included because it's the piece most people mean by "execution engine" by
name.

## Why `execution_engine.cpp` isn't wired into the CMake build

It depends on several sibling modules from the private codebase — shared
type definitions, tuned strategy constants, the Kelly position sizer, the
risk engine's kill switch, and a paper-trading shim — plus `libcurl` and
`hiredis`. Pulling all of that in would mean either publishing the full
private strategy logic (the actual trading edge, and not something to put
on a public repo) or stubbing it out until it no longer reflects the real
system. It's included as-is so the design is visible and readable: the
signal-to-fill flow, the broker's form-encoded REST quirks, the
fill-or-kill polling with real fill-price capture, and the virtual
stop-loss cancel/confirm state machine.

## Build & run

Ring buffer + benchmark (no external dependencies beyond a C++17 compiler
and pthreads):

```bash
# Direct compile
g++ -O3 -std=c++17 -pthread bench/ring_buffer_bench.cpp -o ring_buffer_bench
./ring_buffer_bench            # defaults: 5000 messages, cores 0 and 1
./ring_buffer_bench 20000 0 2  # 20k messages, producer on core 0, consumer on core 2

# Or via CMake
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
./build/ring_buffer_bench
```

Sample output (numbers are from a single-core CI sandbox and will look far
more favorable on a real multi-core box — see the note below):

```
hardware_concurrency = 1, using cores 0 (producer) / 0 (consumer)
default scheduler:           p50=  11.30us  p95=  12.37us  p99=  63.72us  max=6789.06us  n=300
pinned to separate cores:    p50=  11.32us  p95=  13.82us  p99=  15.92us  max=  30.77us  n=300
```

> The benchmark deliberately does **not** enable `SCHED_FIFO`: a real-time,
> busy-spinning consumer that outranks everything else on a machine with
> only one or two cores available to it (a laptop, a shared CI runner) will
> starve the producer thread and hang the process. Production runs this on
> a 4-core box with each hot thread pinned to its own physical core, where
> that risk doesn't apply — `cpu_affinity.hpp` still exposes
> `set_realtime_priority()` for that environment. Run the benchmark on
> your actual target hardware (ideally 2+ dedicated cores) before quoting
> a specific latency-improvement number from it.

`execution_engine.cpp`: read as reference — see the file header for the
signal-to-fill flow and the broker integration notes. It won't compile
standalone (see above).

## Design notes worth reading the source for

- **Ring buffer**: each slot's sequence counter lives on its own cache
  line, separate from the payload bytes, specifically so the consumer
  marking a slot free doesn't fight a producer's in-flight write for the
  same cache line.
- **Backpressure policy**: `publish()` spins rather than drops or
  overwrites when the ring is full. For market data, stalling the producer
  is preferable to silently corrupting or reordering ticks.
- **Fill-price capture**: `execution_engine.cpp` never assumes an order
  filled exactly at its limit price — it parses the broker's actual
  reported fill price, since limit orders commonly get price improvement
  in either direction and a systematic bias here compounds across a
  position-sizing model's rolling trade history.
- **Virtual stop-loss**: the stop isn't a resting broker order from entry;
  it converts from an in-process watch into a live limit order only once
  price nears the trigger zone, with a separate confirm/cancel state
  machine to avoid ever taking both a filled limit exit *and* an emergency
  market exit on the same position.

## Stack

C++17 · lock-free concurrency (Vyukov-style MPSC ring buffer) · POSIX
real-time scheduling (`SCHED_FIFO`, `mlockall`, CPU affinity) · libcurl ·
Redis (hiredis) · ZeroMQ (private modules) · deployed on Oracle Cloud
Infrastructure, Ampere A1 (ARM), ap-mumbai-1.


**NOTE - THIS IS JUST A PART OF A PROPRIETARY SOFTWARE, ACTUAL FILES MAY DIFFER.**
