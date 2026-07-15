#ifndef AWP_TEST_COMMON_H
#define AWP_TEST_COMMON_H

#include "awp/awp.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <assert.h>

static int g_fails = 0;
static int g_passes = 0;

#define TEST_CHECK(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL: %s (%s:%d)\n", msg, __FILE__, __LINE__); \
        g_fails++; \
    } else { \
        g_passes++; \
    } \
} while (0)

#define TEST_EQ_U64(a, b, msg) TEST_CHECK((uint64_t)(a) == (uint64_t)(b), msg)
#define TEST_EQ_I(a, b, msg)   TEST_CHECK((int)(a) == (int)(b), msg)

static inline void test_sleep_ms(unsigned ms)
{
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (long)(ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}

/* Echo process: count + optional hang / error injection via payload. */
typedef struct {
    atomic_uint_fast64_t count;
    atomic_uint_fast64_t errors_seen;
    atomic_int hang;          /* if 1, process sleeps long */
    int fail_seq;             /* if >=0, fail when frame->seq == fail_seq */
    pthread_mutex_t order_mu;
    char last_key[128];
    uint64_t last_seq_for_key;
    int reorder_violations;
} test_ctx_t;

static inline void test_ctx_init(test_ctx_t *c)
{
    memset(c, 0, sizeof(*c));
    atomic_store(&c->count, 0);
    atomic_store(&c->errors_seen, 0);
    atomic_store(&c->hang, 0);
    c->fail_seq = -1;
    pthread_mutex_init(&c->order_mu, NULL);
}

static inline void test_ctx_destroy(test_ctx_t *c)
{
    pthread_mutex_destroy(&c->order_mu);
}

static inline int test_process(const awp_frame_t *frame, void *user)
{
    test_ctx_t *c = (test_ctx_t *)user;
    char key[128];

    /* Cooperative hang: poll so tests can clear hang without 60s waits. */
    while (atomic_load(&c->hang))
        test_sleep_ms(5);

    if (c->fail_seq >= 0 && (int)frame->seq == c->fail_seq) {
        atomic_fetch_add(&c->errors_seen, 1);
        return -1;
    }

    /* Payload convention: first byte 'E' means soft error. */
    if (frame->payload_len > 0 && frame->payload[0] == 'E') {
        atomic_fetch_add(&c->errors_seen, 1);
        return 42;
    }

    snprintf(key, sizeof(key), "%s|%s", frame->feed, frame->symbol);
    pthread_mutex_lock(&c->order_mu);
    if (strcmp(c->last_key, key) == 0) {
        if (frame->seq < c->last_seq_for_key)
            c->reorder_violations++;
    }
    strncpy(c->last_key, key, sizeof(c->last_key) - 1);
    c->last_seq_for_key = frame->seq;
    pthread_mutex_unlock(&c->order_mu);

    atomic_fetch_add(&c->count, 1);
    return 0;
}

static inline void wait_processed(test_ctx_t *c, uint64_t n, unsigned timeout_ms)
{
    unsigned waited = 0;
    while (atomic_load(&c->count) < n && waited < timeout_ms) {
        test_sleep_ms(5);
        waited += 5;
    }
}

#endif
