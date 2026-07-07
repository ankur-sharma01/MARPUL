#pragma once
//
// ring_buffer.hpp
//
// Lock-free multi-producer / single-consumer (MPSC) ring buffer used on the
// market-data ingestion path: one or more producer threads (exchange feed
// handlers) publish fixed-size records; a single dedicated consumer thread
// drains them into the calculation engine.
//
// Design notes
// ------------
// * Vyukov-style MPSC queue: each slot carries its own sequence counter,
//   so producers only ever contend with each other on a single atomic
//   fetch_add, never with the consumer.
// * The sequence counter for each slot lives on its own cache line,
//   separate from the payload. Without this, the consumer's write to the
//   sequence counter (marking a slot free) shares a cache line with the
//   payload bytes a producer is about to write, and the two threads end up
//   fighting over that line on every single slot recycle (false sharing).
//   Measured on an ARM Neoverse-N1 box this alone was worth low-single-digit
//   microseconds of tail latency per publish under load.
// * Slot lifecycle: seq == index (free) -> seq == index + 1 (published,
//   ready to consume) -> seq == index + CAPACITY (free again for the next
//   lap). This is what lets multiple producers land on the same slot index
//   across different laps without a lock.
// * publish() spins rather than fails when the ring is full. For a feed
//   handler that must not drop or reorder ticks, stalling the producer is
//   the correct failure mode — the alternative is silently corrupting or
//   dropping market data. approx_fill() / stalls() exist so the consumer
//   side can alert if it's falling behind.
//
// Not safe for multiple concurrent consumers — that would need a proper
// MPMC scheme (e.g. distinct claim/publish sequences per consumer group).
//
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <cassert>
#include <cstdlib>
#include <new>

#if defined(__aarch64__)
    #define MARPUL_CPU_RELAX() __asm__ volatile("yield" ::: "memory")
#elif defined(__x86_64__)
    #define MARPUL_CPU_RELAX() __asm__ volatile("pause" ::: "memory")
#else
    #include <thread>
    #define MARPUL_CPU_RELAX() std::this_thread::yield()
#endif

namespace marpul {

inline constexpr size_t kCacheLineBytes = 64;

// One atomic per cache line — prevents adjacent atomics from false-sharing.
template <typename T>
struct alignas(kCacheLineBytes) PaddedAtomic {
    std::atomic<T> value;
    char           _pad[kCacheLineBytes - sizeof(std::atomic<T>)];

    PaddedAtomic() noexcept : value(T{}) {}
    explicit PaddedAtomic(T init) noexcept : value(init) {}
    PaddedAtomic(const PaddedAtomic&)            = delete;
    PaddedAtomic& operator=(const PaddedAtomic&) = delete;
};

// RingBuffer<SlotBytes, Capacity>
//   SlotBytes  - payload size per slot, in bytes.
//   Capacity   - number of slots. Must be a power of two.
template <size_t SlotBytes, size_t Capacity>
class RingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");
    static_assert(Capacity >= 8, "Capacity must be at least 8 slots");

    static constexpr int64_t kMask = static_cast<int64_t>(Capacity) - 1;

    struct Slot {
        uint8_t  data[SlotBytes];
        uint32_t size{0};

        alignas(kCacheLineBytes) std::atomic<int64_t> seq;
        char _seq_pad[kCacheLineBytes - sizeof(std::atomic<int64_t>)];

        Slot() noexcept : seq(0) { std::memset(data, 0, sizeof(data)); }
        Slot(const Slot&)            = delete;
        Slot& operator=(const Slot&) = delete;
    };

    Slot* const slots_;

    PaddedAtomic<int64_t> enqueue_pos_{0}; // claimed by producers via fetch_add
    PaddedAtomic<int64_t> dequeue_pos_{0}; // advanced by the single consumer

    std::atomic<uint64_t> published_{0};
    std::atomic<uint64_t> consumed_{0};
    std::atomic<uint64_t> stalls_{0}; // times a producer spun past kWarnSpins

    static constexpr int kWarnSpins = 100'000;

public:
    RingBuffer()
        : slots_(static_cast<Slot*>(
              ::aligned_alloc(kCacheLineBytes, Capacity * sizeof(Slot))))
    {
        if (!slots_) throw std::bad_alloc();
        for (size_t i = 0; i < Capacity; ++i) {
            new (&slots_[i]) Slot();
            slots_[i].seq.store(static_cast<int64_t>(i), std::memory_order_relaxed);
        }
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }

    ~RingBuffer() noexcept {
        for (size_t i = 0; i < Capacity; ++i) slots_[i].~Slot();
        ::free(slots_);
    }

    RingBuffer(const RingBuffer&)            = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;

    // Thread-safe from any number of producer threads.
    // Spins until the target slot has been freed by the consumer.
    void publish(const uint8_t* data, uint32_t size) noexcept {
        assert(size > 0 && size <= SlotBytes);

        const int64_t pos = enqueue_pos_.value.fetch_add(1, std::memory_order_relaxed);
        Slot& slot = slots_[pos & kMask];

        int spins = 0;
        for (;;) {
            const int64_t seq = slot.seq.load(std::memory_order_acquire);
            if (seq - pos == 0) break; // slot is free for this lap
            if (++spins == kWarnSpins) {
                stalls_.fetch_add(1, std::memory_order_relaxed);
            }
            MARPUL_CPU_RELAX();
        }

        std::memcpy(slot.data, data, size);
        slot.size = size;

        // release: everything written above must be visible before the
        // consumer observes the new sequence number.
        slot.seq.store(pos + 1, std::memory_order_release);
        published_.fetch_add(1, std::memory_order_relaxed);
    }

    // Single-consumer only. Returns false if no slot is ready yet.
    // Handler signature: void(const uint8_t* data, uint32_t size)
    template <typename Handler>
    bool consume_one(Handler&& handler) noexcept {
        const int64_t pos  = dequeue_pos_.value.load(std::memory_order_relaxed);
        Slot&         slot = slots_[pos & kMask];

        const int64_t seq = slot.seq.load(std::memory_order_acquire);
        if (seq - (pos + 1) != 0) return false;

        handler(static_cast<const uint8_t*>(slot.data), slot.size);
        consumed_.fetch_add(1, std::memory_order_relaxed);

        // release: free the slot for the next lap only after the handler
        // has finished reading it.
        slot.seq.store(pos + static_cast<int64_t>(Capacity), std::memory_order_release);
        dequeue_pos_.value.store(pos + 1, std::memory_order_relaxed);
        return true;
    }

    template <typename Handler>
    void consume_loop(const std::atomic<bool>& stop, Handler&& handler) noexcept {
        while (!stop.load(std::memory_order_relaxed)) {
            if (!consume_one(handler)) MARPUL_CPU_RELAX();
        }
        while (consume_one(handler)) {} // drain remainder on shutdown
    }

    size_t approx_fill() const noexcept {
        const int64_t w = enqueue_pos_.value.load(std::memory_order_relaxed);
        const int64_t r = dequeue_pos_.value.load(std::memory_order_relaxed);
        if (w <= r) return 0;
        const int64_t d = w - r;
        return static_cast<size_t>(d < static_cast<int64_t>(Capacity) ? d : Capacity);
    }

    uint64_t published() const noexcept { return published_.load(std::memory_order_relaxed); }
    uint64_t consumed()  const noexcept { return consumed_.load(std::memory_order_relaxed); }
    uint64_t stalls()    const noexcept { return stalls_.load(std::memory_order_relaxed); }
    static constexpr size_t capacity()  noexcept { return Capacity; }
};

} // namespace marpul
