/**
 * Example: MPSC topology — multiple venue reader threads, one consumer
 * thread per worker (default production shape).
 */
#include "awp/awp.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include <unistd.h>

static atomic_uint_fast64_t g_n;

static int on_msg(const awp_frame_t *f, void *user)
{
    (void)user;
    printf("[MPSC] shard=%u feed=%s symbol=%s seq=%llu\n",
           f->shard, f->feed, f->symbol, (unsigned long long)f->seq);
    atomic_fetch_add(&g_n, 1);
    return 0;
}

typedef struct {
    awp_pool_t *pool;
    int id;
} venue_t;

static void *venue_main(void *arg)
{
    venue_t *v = arg;
    int i;
    const char *syms[] = { "BTCUSDT", "ETHUSDT", "SOLUSDT" };
    for (i = 0; i < 6; i++) {
        char payload[32];
        snprintf(payload, sizeof(payload), "v%d-%d", v->id, i);
        awp_submit(v->pool, "trades", syms[(v->id + i) % 3], payload,
                   strlen(payload), 0);
        usleep(500);
    }
    return NULL;
}

int main(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    venue_t v[3];
    pthread_t th[3];
    int i;
    const uint64_t expect = 18;

    awp_config_init(&cfg);
    cfg.n_workers = 8;
    cfg.queue_capacity = 64;
    cfg.frame_pool_size = 256;
    cfg.ring_mode = AWP_RING_MPSC;
    cfg.process = on_msg;

    if (awp_pool_create(&cfg, &pool) != 0)
        return 1;

    for (i = 0; i < 3; i++) {
        v[i].pool = pool;
        v[i].id = i;
        pthread_create(&th[i], NULL, venue_main, &v[i]);
    }
    for (i = 0; i < 3; i++)
        pthread_join(th[i], NULL);

    while (atomic_load(&g_n) < expect)
        usleep(500);
    printf("MPSC example done: %llu messages (3 readers)\n",
           (unsigned long long)atomic_load(&g_n));

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    return 0;
}
