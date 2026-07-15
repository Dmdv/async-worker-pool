/**
 * Example: SPSC topology — one venue reader thread, atomic SPSC rings.
 *
 * Use when a single producer owns all submits (or you pin one reader per
 * process and do not share the pool across threads).
 */
#include "awp/awp.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <unistd.h>

static atomic_uint_fast64_t g_n;

static int on_msg(const awp_frame_t *f, void *user)
{
    (void)user;
    printf("[SPSC] shard=%u feed=%s symbol=%s seq=%llu\n",
           f->shard, f->feed, f->symbol, (unsigned long long)f->seq);
    atomic_fetch_add(&g_n, 1);
    return 0;
}

int main(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    int i;

    awp_config_init(&cfg);
    cfg.n_workers = 4;
    cfg.queue_capacity = 64;
    cfg.frame_pool_size = 256;
    cfg.ring_mode = AWP_RING_SPSC;
    cfg.enable_supervisor = 0;
    cfg.process = on_msg;

    if (awp_pool_create(&cfg, &pool) != 0)
        return 1;

    /* Single thread submits — matches SPSC contract. */
    for (i = 0; i < 12; i++) {
        const char *syms[] = { "BTCUSDT", "ETHUSDT", "SOLUSDT" };
        char payload[32];
        snprintf(payload, sizeof(payload), "%d", i);
        awp_submit(pool, "trades", syms[i % 3], payload, strlen(payload), 0);
    }

    while (atomic_load(&g_n) < 12)
        usleep(500);
    printf("SPSC example done: %llu messages\n",
           (unsigned long long)atomic_load(&g_n));

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    return 0;
}
