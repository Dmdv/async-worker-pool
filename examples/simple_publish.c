/**
 * Example: venue reader threads → AWP pool → mock NATS publish callback.
 *
 * Demonstrates runtime gate AWP_ENABLED and the public API surface.
 */
#include "awp/awp.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <unistd.h>
#include <pthread.h>

static atomic_uint_fast64_t g_published;

static int mock_nats_publish(const awp_frame_t *frame, void *user)
{
    (void)user;
    /* Stand-in for natsConnection_PublishAsync(conn, subject, data, len) */
    printf("publish feed=%s symbol=%s shard=%u seq=%llu len=%zu\n",
           frame->feed, frame->symbol, frame->shard,
           (unsigned long long)frame->seq, frame->payload_len);
    atomic_fetch_add(&g_published, 1);
    return 0;
}

static void *venue_reader(void *arg)
{
    awp_pool_t *pool = arg;
    const char *symbols[] = { "BTCUSDT", "ETHUSDT", "SOLUSDT" };
    int i;
    for (i = 0; i < 10; i++) {
        const char *sym = symbols[i % 3];
        char payload[64];
        snprintf(payload, sizeof(payload), "{\"px\":%d}", 1000 + i);
        if (awp_submit(pool, "trades", sym, payload, strlen(payload), 0) != 0)
            fprintf(stderr, "submit failed\n");
        usleep(1000);
    }
    return NULL;
}

int main(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    pthread_t t1, t2;
    awp_pool_metrics_t m;

    if (!awp_runtime_enabled()) {
        printf("AWP_ENABLED is not set — enabling for demo (set env for production gate)\n");
        setenv("AWP_ENABLED", "1", 1);
    }
    printf("awp_runtime_enabled=%d version=%d.%d.%d\n",
           awp_runtime_enabled(),
           AWP_VERSION_MAJOR, AWP_VERSION_MINOR, AWP_VERSION_PATCH);

    awp_config_init(&cfg);
    cfg.n_workers = 8;
    cfg.queue_capacity = 64;
    cfg.frame_pool_size = 256;
    cfg.process = mock_nats_publish;
    cfg.enable_supervisor = 1;

    if (awp_pool_create(&cfg, &pool) != 0) {
        fprintf(stderr, "awp_pool_create failed\n");
        return 1;
    }

    pthread_create(&t1, NULL, venue_reader, pool);
    pthread_create(&t2, NULL, venue_reader, pool);
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);

    while (atomic_load(&g_published) < 20)
        usleep(1000);

    awp_pool_get_metrics(pool, &m);
    printf("submitted=%llu published=%llu drops=%llu\n",
           (unsigned long long)m.submitted,
           (unsigned long long)atomic_load(&g_published),
           (unsigned long long)m.dropped);

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    return 0;
}
