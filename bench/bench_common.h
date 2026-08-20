#ifndef AWP_BENCH_COMMON_H
#define AWP_BENCH_COMMON_H

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#if defined(__APPLE__)
#include <mach/mach_time.h>
#include <pthread/qos.h>
#endif

#if defined(__ARM_NEON) || defined(__aarch64__)
#include <arm_neon.h>
#endif

static inline void bench_cpu_relax(void)
{
#if defined(__x86_64__) || defined(__i386__)
    __asm__ __volatile__("pause" ::: "memory");
#elif defined(__aarch64__) || defined(__arm__)
    __asm__ __volatile__("yield" ::: "memory");
#endif
}

/**
 * Read raw CPU hardware timestamp register.
 */
static inline uint64_t bench_rdtsc(void)
{
#if defined(__aarch64__) || defined(__ARM_ARCH_ISA_A64)
    uint64_t val;
    __asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(val));
    return val;
#elif defined(__x86_64__) || defined(__i386__)
    uint32_t lo, hi;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)hi << 32) | lo;
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
#endif
}

/**
 * Nanosecond clock using Mach absolute time or CLOCK_MONOTONIC.
 */
static inline uint64_t bench_now_ns(void)
{
#if defined(__APPLE__)
    static mach_timebase_info_data_t tb;
    if (tb.denom == 0)
        mach_timebase_info(&tb);
    return mach_absolute_time() * tb.numer / tb.denom;
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
#endif
}

/**
 * Pin current thread to Performance Cores (macOS Apple Silicon).
 */
static inline void bench_pin_pcores(void)
{
#if defined(__APPLE__)
    pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
#endif
}

/**
 * Compute 64-byte payload checksum (ARM NEON or scalar fallback).
 */
static inline uint32_t bench_fast_sum_64(const uint8_t *ptr)
{
#if defined(__ARM_NEON) || defined(__aarch64__)
    uint8x16_t v0 = vld1q_u8(ptr);
    uint8x16_t v1 = vld1q_u8(ptr + 16);
    uint8x16_t v2 = vld1q_u8(ptr + 32);
    uint8x16_t v3 = vld1q_u8(ptr + 48);

    uint16x8_t s0 = vpaddlq_u8(v0);
    uint16x8_t s1 = vpadalq_u8(s0, v1);
    uint16x8_t s2 = vpadalq_u8(s1, v2);
    uint16x8_t s3 = vpadalq_u8(s2, v3);

    return (uint32_t)vaddlvq_u16(s3);
#else
    uint32_t sum = 0;
    for (size_t i = 0; i < 64; i++)
        sum += ptr[i];
    return sum;
#endif
}

static inline int bench_cmp_u64(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a;
    uint64_t y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

typedef struct {
    double min_ns;
    double p50_ns;
    double p90_ns;
    double p99_ns;
    double p999_ns;
    double p9999_ns;
    double max_ns;
    double mean_ns;
} bench_percentiles_t;

static inline bench_percentiles_t bench_calc_percentiles(uint64_t *samples, size_t count)
{
    bench_percentiles_t p;
    memset(&p, 0, sizeof(p));
    if (count == 0 || !samples)
        return p;

    qsort(samples, count, sizeof(uint64_t), bench_cmp_u64);

    uint64_t sum = 0;
    for (size_t i = 0; i < count; i++)
        sum += samples[i];
    p.mean_ns = (double)sum / (double)count;

    p.min_ns   = (double)samples[0];
    p.p50_ns   = (double)samples[count * 50 / 100];
    p.p90_ns   = (double)samples[count * 90 / 100];
    p.p99_ns   = (double)samples[count * 99 / 100];
    p.p999_ns  = (double)samples[count > 1000 ? count * 999 / 1000 : count - 1];
    p.p9999_ns = (double)samples[count > 10000 ? count * 9999 / 10000 : count - 1];
    p.max_ns   = (double)samples[count - 1];
    return p;
}

#endif /* AWP_BENCH_COMMON_H */
