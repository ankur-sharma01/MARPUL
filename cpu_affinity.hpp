#pragma once
//
// cpu_affinity.hpp
//
// Thread placement and scheduling helpers for latency-sensitive threads:
// core pinning, SCHED_FIFO real-time priority, locked memory, and stack
// pre-touch. Every function here is called once at thread start-up, never
// in the hot path, so there's no reason to keep them out of a header.
//
// One-time host setup this assumes has already been done:
//
//   1. Real-time priority (in /etc/security/limits.conf):
//        <user> soft rtprio 99
//        <user> hard rtprio 99
//
//   2. Transparent Huge Pages disabled system-wide:
//        echo never > /sys/kernel/mm/transparent_hugepage/enabled
//        echo never > /sys/kernel/mm/transparent_hugepage/defrag
//
//   3. CPU governor pinned to performance on the cores in use:
//        echo performance > /sys/devices/system/cpu/cpuN/cpufreq/scaling_governor
//
//   4. Swappiness lowered:
//        echo 1 > /proc/sys/vm/swappiness
//
// Notes:
//   - set_realtime_priority() commonly fails with EPERM on a VPS without
//     CAP_SYS_NICE. The thread still runs at normal priority in that case;
//     callers should treat the return value as a warning, not a hard stop.
//   - lock_memory() must run on the main thread before any worker threads
//     are spawned — MCL_FUTURE only covers allocations made after the call.
//   - pin_to_core() is per-thread: call it from inside each hot thread's
//     entry function, not once from main().
//
#include <sched.h>
#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include <cstdio>
#include <cerrno>
#include <cstdint>

namespace marpul {

// Pin the calling thread to a specific logical CPU core.
inline bool pin_to_core(int core_id) noexcept {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    int rc = ::pthread_setaffinity_np(::pthread_self(), sizeof(cpuset), &cpuset);
    if (rc != 0) {
        std::fprintf(stderr, "pin_to_core(%d) failed: %s\n", core_id, ::strerror(rc));
        return false;
    }
    return true;
}

// Set SCHED_FIFO real-time scheduling for the calling thread.
// priority is 1 (lowest RT) to 99 (highest RT).
inline bool set_realtime_priority(int priority) noexcept {
    if (priority < 1 || priority > 99) {
        std::fprintf(stderr, "set_realtime_priority: %d out of range [1,99]\n", priority);
        return false;
    }

    struct sched_param param{};
    param.sched_priority = priority;

    int rc = ::pthread_setschedparam(::pthread_self(), SCHED_FIFO, &param);
    if (rc != 0) {
        std::fprintf(stderr,
            "set_realtime_priority(%d) failed: %s "
            "(needs CAP_SYS_NICE or an /etc/security/limits.conf entry)\n",
            priority, ::strerror(rc));
        return false;
    }
    return true;
}

// Lock all current and future process memory pages in RAM, so the kernel
// never swaps a page mid-session. Call once, from the main thread, before
// spawning any worker threads.
inline bool lock_memory() noexcept {
    if (::mlockall(MCL_CURRENT | MCL_FUTURE) != 0) {
        std::fprintf(stderr, "mlockall() failed: %s\n", ::strerror(errno));
        return false;
    }
    return true;
}

// Touch every page of `bytes` of stack up front, so the first deep call
// during market hours doesn't take a page fault.
inline void pretouch_stack(size_t bytes = 512 * 1024) noexcept {
    static constexpr size_t kPage = 4096;
    volatile char* p = static_cast<volatile char*>(__builtin_alloca(bytes));
    for (size_t i = 0; i < bytes; i += kPage) p[i] = 0;
}

// Advise the kernel not to back a specific region with Transparent Huge
// Pages. Belt-and-suspenders on top of the system-wide setting above.
inline void disable_thp_hint(void* addr, size_t size) noexcept {
#if defined(MADV_NOHUGEPAGE)
    ::madvise(addr, size, MADV_NOHUGEPAGE);
#else
    (void)addr; (void)size;
#endif
}

// Convenience: pin + RT-priority + stack pre-touch in one call, for a
// thread that's about to enter its hot loop.
inline bool setup_hot_thread(int core_id, int priority = 90) noexcept {
    bool ok = pin_to_core(core_id);
    ok      = set_realtime_priority(priority) && ok;
    pretouch_stack();
    return ok;
}

inline int current_cpu() noexcept { return ::sched_getcpu(); }

inline void verify_cpu(int expected_core, const char* thread_name) noexcept {
    int actual = current_cpu();
    if (actual != expected_core) {
        std::fprintf(stderr, "WARNING: %s is running on CPU %d, expected CPU %d\n",
                     thread_name, actual, expected_core);
    }
}

} // namespace marpul
