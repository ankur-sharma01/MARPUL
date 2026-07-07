// ring_buffer_bench.cpp
//
// Producer/consumer latency benchmark for ring_buffer.hpp.
//
// Runs the same publish/consume workload twice: once with the producer and
// consumer threads left on the default scheduler, and once with them pinned
// to separate cores (via cpu_affinity.hpp). Reports p50 / p95 / p99 / max
// publish-to-consume latency for both runs.
//
// This exists to produce a real, reproducible number for "core pinning
// improves tail latency" instead of quoting one from memory. Numbers will
// vary by machine — run it on the target box before citing a result.
//
// Deliberately does NOT enable SCHED_FIFO here: a real-time, busy-spinning
// consumer thread that outranks everything else on a machine with only one
// or two cores available to it (a laptop, a shared CI runner, a small VM)
// will starve the producer and hang the process. In production this runs
// on a 4-OCPU box with each hot thread on its own dedicated physical core,
// where that risk doesn't apply — see cpu_affinity.hpp for the RT-priority
// helper used there. For a portable benchmark, core pinning alone already
// isolates the two threads from scheduler migration and cross-core cache
// effects, which is what's being measured here.
//
// Build:
//   g++ -O3 -std=c++17 -pthread bench/ring_buffer_bench.cpp -o ring_buffer_bench
// Run:
//   ./ring_buffer_bench [messages] [producer_core] [consumer_core]

#include "../include/marpul/ring_buffer.hpp"
#include "../include/marpul/cpu_affinity.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

struct Msg {
    int64_t send_ns;
};

static uint64_t now_ns() noexcept {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            Clock::now().time_since_epoch())
            .count());
}

struct RunResult {
    std::vector<double> latencies_us;
    bool                timed_out{false};
};

static RunResult run_once(
    size_t n_messages, bool pin_threads, int producer_core, int consumer_core)
{
    using Ring = marpul::RingBuffer<sizeof(Msg), 65536>;
    auto ring = std::make_unique<Ring>();

    std::atomic<bool> stop{false};
    std::vector<double> latencies;
    latencies.reserve(n_messages);

    std::thread consumer([&] {
        if (pin_threads) marpul::pin_to_core(consumer_core);
        size_t received = 0;
        // Safety valve: bail out instead of spinning forever if the producer
        // stalls for any reason (e.g. starved by an unrelated process).
        auto last_progress = Clock::now();
        while (received < n_messages) {
            bool got = ring->consume_one([&](const uint8_t* data, uint32_t size) {
                Msg m{};
                std::memcpy(&m, data, size);
                uint64_t t_now = now_ns();
                latencies.push_back(
                    static_cast<double>(t_now - static_cast<uint64_t>(m.send_ns)) / 1000.0);
                ++received;
            });
            if (got) {
                last_progress = Clock::now();
            } else {
                if (Clock::now() - last_progress > std::chrono::seconds(5)) {
                    stop.store(true, std::memory_order_relaxed);
                    return;
                }
                MARPUL_CPU_RELAX();
            }
        }
    });

    std::thread producer([&] {
        if (pin_threads) marpul::pin_to_core(producer_core);
        for (size_t i = 0; i < n_messages && !stop.load(std::memory_order_relaxed); ++i) {
            Msg m{static_cast<int64_t>(now_ns())};
            ring->publish(reinterpret_cast<const uint8_t*>(&m), sizeof(m));
            // Pace publishes to roughly simulate a real feed (~2 kHz) rather
            // than measuring an unrealistic tight-loop burst.
            std::this_thread::sleep_for(std::chrono::microseconds(500));
        }
    });

    producer.join();
    consumer.join();

    return RunResult{std::move(latencies), stop.load(std::memory_order_relaxed)};
}

static void report(const char* label, const RunResult& result) {
    if (result.timed_out || result.latencies_us.empty()) {
        std::printf("%-28s stalled — no result (see note above)\n", label);
        return;
    }
    std::vector<double> lat = result.latencies_us;
    std::sort(lat.begin(), lat.end());
    auto pct = [&](double p) {
        size_t idx = static_cast<size_t>(p * static_cast<double>(lat.size() - 1));
        return lat[idx];
    };
    std::printf("%-28s p50=%7.2fus  p95=%7.2fus  p99=%7.2fus  max=%7.2fus  n=%zu\n",
                label, pct(0.50), pct(0.95), pct(0.99), lat.back(), lat.size());
}

int main(int argc, char** argv) {
    size_t n_messages    = (argc > 1) ? static_cast<size_t>(std::atoll(argv[1])) : 5000;
    int    producer_core = (argc > 2) ? std::atoi(argv[2]) : 0;
    int    consumer_core = (argc > 3) ? std::atoi(argv[3]) : 1;

    unsigned hw = std::thread::hardware_concurrency();
    if (hw < 2) {
        std::printf(
            "NOTE: hardware_concurrency() reports %u core(s). Core pinning needs at "
            "least 2 to show separation between producer and consumer; both threads "
            "will share core 0 on this machine and the two runs will look similar.\n",
            hw);
        producer_core = 0;
        consumer_core = 0;
    } else {
        producer_core = producer_core % static_cast<int>(hw);
        consumer_core = consumer_core % static_cast<int>(hw);
    }
    std::printf("hardware_concurrency = %u, using cores %d (producer) / %d (consumer)\n",
                hw, producer_core, consumer_core);

    auto baseline = run_once(n_messages, /*pin_threads=*/false, producer_core, consumer_core);
    report("default scheduler:", baseline);

    auto pinned = run_once(n_messages, /*pin_threads=*/true, producer_core, consumer_core);
    report("pinned to separate cores:", pinned);

    return 0;
}
