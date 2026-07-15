/**
 * Open-loop scheduled arrival harness with mock publisher accept.
 *
 * NOT a real-publisher SLA claim: process() only records local mock accept.
 * Plug a real SDK accept into mock_publish() for track-(d) qualification.
 *
 * Usage: ./build/bench_openloop [rate_msg_s] [count] [workers]
 * Example: ./build/bench_openloop 1000 2000 8
 */
#include "awp/awp.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <time.h>

typedef struct {
    uint64_t id;
    uint64_t sched_ns;
    uint64_t submit_start_ns;
    uint64_t accept_ns;
    int submit_rc;
    int accept_rc;
} sample_t;

static sample_t *g_samples;
static atomic_uint_fast64_t g_accepts;
static atomic_int g_accept_err;

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* Mock local-buffer accept: copy is "done" when callback returns. */
static int mock_publish(const awp_frame_t *f, void *user)
{
    sample_t *s;
    uint64_t id;
    (void)user;
    if (f->payload_len < sizeof(uint64_t)) {
        atomic_fetch_add(&g_accept_err, 1);
        return -1;
    }
    memcpy(&id, f->payload, sizeof(id));
    if (id >= (uint64_t)atomic_load(&g_accepts) + 1000000ull) {
        /* soft bound only for diagnostics */
    }
    if (g_samples && id < 1000000ull) {
        s = &g_samples[id];
        s->accept_ns = now_ns();
        s->accept_rc = 0;
    }
    atomic_fetch_add(&g_accepts, 1);
    return 0;
}

static int cmp_u64(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a;
    uint64_t y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

static uint64_t pct(uint64_t *v, size_t n, double p)
{
    size_t i;
    if (n == 0)
        return 0;
    i = (size_t)(p * (double)(n - 1));
    if (i >= n)
        i = n - 1;
    return v[i];
}

int main(int argc, char **argv)
{
    double rate = 1000.0;
    int count = 2000;
    int workers = 8;
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    uint64_t t0, interval_ns;
    int i, offered = 0, submit_ok = 0;
    uint64_t *lat_submit_accept, *lat_sched_accept, *lateness;
    size_t n_ok = 0;
    int late_invalid = 0;

    if (argc > 1)
        rate = atof(argv[1]);
    if (argc > 2)
        count = atoi(argv[2]);
    if (argc > 3)
        workers = atoi(argv[3]);
    if (rate <= 0.0 || count <= 0 || workers <= 0) {
        fprintf(stderr, "usage: %s [rate] [count] [workers]\n", argv[0]);
        return 2;
    }

    printf("bench_openloop: MOCK publisher only — not track-(d) SLA evidence\n");
    printf("rate=%.0f/s count=%d workers=%d\n", rate, count, workers);

    g_samples = calloc((size_t)count, sizeof(sample_t));
    lat_submit_accept = calloc((size_t)count, sizeof(uint64_t));
    lat_sched_accept = calloc((size_t)count, sizeof(uint64_t));
    lateness = calloc((size_t)count, sizeof(uint64_t));
    if (!g_samples || !lat_submit_accept || !lat_sched_accept || !lateness) {
        fprintf(stderr, "oom\n");
        return 1;
    }

    atomic_store(&g_accepts, 0);
    atomic_store(&g_accept_err, 0);

    awp_config_init(&cfg);
    cfg.n_workers = (uint32_t)workers;
    cfg.queue_capacity = 256;
    cfg.frame_pool_size = (uint32_t)(count < 4096 ? 4096 : count);
    cfg.enable_supervisor = 0;
    cfg.process = mock_publish;

    if (awp_pool_create(&cfg, &pool) != 0) {
        fprintf(stderr, "create failed\n");
        return 1;
    }

    interval_ns = (uint64_t)(1e9 / rate);
    t0 = now_ns();

    for (i = 0; i < count; i++) {
        uint64_t sched = t0 + (uint64_t)i * interval_ns;
        uint64_t id = (uint64_t)i;
        char sym[16];
        uint64_t now;

        g_samples[i].id = id;
        g_samples[i].sched_ns = sched;

        now = now_ns();
        if (now < sched) {
            struct timespec ts;
            uint64_t rem = sched - now;
            ts.tv_sec = (time_t)(rem / 1000000000ull);
            ts.tv_nsec = (long)(rem % 1000000000ull);
            nanosleep(&ts, NULL);
        }
        g_samples[i].submit_start_ns = now_ns();
        if (g_samples[i].submit_start_ns > sched)
            lateness[i] = g_samples[i].submit_start_ns - sched;
        else
            lateness[i] = 0;

        snprintf(sym, sizeof(sym), "S%u", (unsigned)(i % 64));
        g_samples[i].submit_rc = awp_submit(pool, "ol", sym, &id, sizeof(id), 0);
        offered++;
        if (g_samples[i].submit_rc == 0)
            submit_ok++;
    }

    /* Drain */
    {
        int w = 0;
        while (atomic_load(&g_accepts) < (uint64_t)submit_ok && w < 30000) {
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000L };
            nanosleep(&ts, NULL);
            w++;
        }
    }
    awp_pool_shutdown(pool);

    for (i = 0; i < count; i++) {
        if (g_samples[i].submit_rc != 0 || g_samples[i].accept_rc != 0)
            continue;
        if (g_samples[i].accept_ns == 0)
            continue;
        lat_submit_accept[n_ok] =
            g_samples[i].accept_ns - g_samples[i].submit_start_ns;
        lat_sched_accept[n_ok] =
            g_samples[i].accept_ns - g_samples[i].sched_ns;
        if (lateness[i] > interval_ns)
            late_invalid++;
        n_ok++;
    }

    qsort(lat_submit_accept, n_ok, sizeof(uint64_t), cmp_u64);
    qsort(lat_sched_accept, n_ok, sizeof(uint64_t), cmp_u64);
    qsort(lateness, (size_t)count, sizeof(uint64_t), cmp_u64);

    printf("accounting: offered=%d submit_ok=%d accepts=%llu accept_err=%d samples_ok=%zu\n",
           offered, submit_ok, (unsigned long long)atomic_load(&g_accepts),
           atomic_load(&g_accept_err), n_ok);
    {
        int exact_ok = (offered == submit_ok &&
                        (uint64_t)submit_ok == atomic_load(&g_accepts) &&
                        atomic_load(&g_accept_err) == 0);
        printf("exact: offered==submit_ok==accepts? %s\n",
               exact_ok ? "YES" : "NO");
        printf("generator lateness p50=%.3fms p99=%.3fms late_runs>%lldns=%d\n",
               pct(lateness, (size_t)count, 0.50) / 1e6,
               pct(lateness, (size_t)count, 0.99) / 1e6,
               (long long)interval_ns, late_invalid);
        if (n_ok > 0) {
            printf("submit→accept p50=%.3fms p95=%.3fms p99=%.3fms\n",
                   pct(lat_submit_accept, n_ok, 0.50) / 1e6,
                   pct(lat_submit_accept, n_ok, 0.95) / 1e6,
                   pct(lat_submit_accept, n_ok, 0.99) / 1e6);
            printf("sched→accept  p50=%.3fms p95=%.3fms p99=%.3fms\n",
                   pct(lat_sched_accept, n_ok, 0.50) / 1e6,
                   pct(lat_sched_accept, n_ok, 0.95) / 1e6,
                   pct(lat_sched_accept, n_ok, 0.99) / 1e6);
        }

        {
            awp_pool_metrics_t m;
            if (awp_pool_get_metrics(pool, &m) == 0)
                printf("drops=%llu process_errors=%llu shutdown_aborts=%llu\n",
                       (unsigned long long)m.dropped,
                       (unsigned long long)m.process_errors,
                       (unsigned long long)m.shutdown_aborts);
        }

        awp_pool_destroy(pool);
        free(g_samples);
        free(lat_submit_accept);
        free(lat_sched_accept);
        free(lateness);
        return exact_ok ? 0 : 1;
    }
}
