/**
 * E2E matrix: multi-reader (or single-reader) dispatch for every ring_mode.
 *
 * Topology respects mode:
 *   SPSC/SPMC → 1 producer thread
 *   MPSC/MPMC → N producer threads
 * Pool always has one consumer thread per worker (SC subset of SPMC/MPMC).
 */
#include "test_common.h"
#include "mode_util.h"

#define N_KEYS   500
#define MSGS_PER 200

typedef struct {
    awp_pool_t *pool;
    int reader_id;
    int msgs;
    atomic_int *errors;
} reader_args_t;

static void *reader_main(void *arg)
{
    reader_args_t *a = arg;
    int i;
    for (i = 0; i < a->msgs; i++) {
        char symbol[32];
        char payload[64];
        int key = (a->reader_id * 997 + i * 13) % N_KEYS;
        snprintf(symbol, sizeof(symbol), "SYM%04d", key);
        snprintf(payload, sizeof(payload), "r%d-i%d", a->reader_id, i);
        if (awp_submit(a->pool, "trades", symbol, payload, strlen(payload), 0) != 0)
            atomic_fetch_add(a->errors, 1);
    }
    if (awp_submit(a->pool, "markPrice", "", "bc", 2, AWP_FRAME_BROADCAST) != 0)
        atomic_fetch_add(a->errors, 1);
    return NULL;
}

#define MAX_READERS 8

typedef struct {
    atomic_uint_fast64_t count;
    pthread_mutex_t mu;
    int last_i[MAX_READERS][N_KEYS];
    unsigned char seen[MAX_READERS][N_KEYS];
    int reorder;
} e2e_ctx_t;

static int e2e_process(const awp_frame_t *frame, void *user)
{
    e2e_ctx_t *c = (e2e_ctx_t *)user;
    int key = -1;
    int rid = -1, li = -1;

    if (strcmp(frame->feed, "trades") == 0 &&
        sscanf(frame->symbol, "SYM%d", &key) == 1 &&
        key >= 0 && key < N_KEYS &&
        frame->payload_len > 0 &&
        sscanf((const char *)frame->payload, "r%d-i%d", &rid, &li) == 2 &&
        rid >= 0 && rid < MAX_READERS) {
        pthread_mutex_lock(&c->mu);
        if (c->seen[rid][key] && li < c->last_i[rid][key])
            c->reorder++;
        c->last_i[rid][key] = li;
        c->seen[rid][key] = 1;
        pthread_mutex_unlock(&c->mu);
    }
    atomic_fetch_add(&c->count, 1);
    return 0;
}

static int run_e2e_mode(awp_ring_mode_t mode)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    e2e_ctx_t ctx;
    int n_readers = awp_mode_suggest_producers(mode);
    pthread_t readers[8];
    reader_args_t args[8];
    atomic_int errors;
    int i;
    const char *bc_feeds[] = { "markPrice", NULL };
    uint64_t expect;
    awp_pool_metrics_t m;
    uint64_t t0, t1;
    struct timespec ts;
    int before_fails = g_fails;

    if (n_readers > 8)
        n_readers = 8;

    printf("e2e [%s] readers=%d\n", awp_mode_name(mode), n_readers);
    memset(&ctx, 0, sizeof(ctx));
    atomic_store(&ctx.count, 0);
    pthread_mutex_init(&ctx.mu, NULL);
    atomic_init(&errors, 0);

    awp_config_init(&cfg);
    cfg.n_workers = 16;
    cfg.n_broadcast_workers = 1;
    cfg.broadcast_feeds = bc_feeds;
    cfg.queue_capacity = 128;
    cfg.frame_pool_size = 4096;
    cfg.ring_mode = mode;
    cfg.enable_supervisor = 1;
    cfg.process = e2e_process;
    cfg.user = &ctx;

    if (awp_pool_create(&cfg, &pool) != 0) {
        fprintf(stderr, "  create failed\n");
        g_fails++;
        return 1;
    }

    clock_gettime(CLOCK_MONOTONIC, &ts);
    t0 = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;

    for (i = 0; i < n_readers; i++) {
        args[i].pool = pool;
        args[i].reader_id = i;
        args[i].msgs = MSGS_PER;
        args[i].errors = &errors;
        pthread_create(&readers[i], NULL, reader_main, &args[i]);
    }
    for (i = 0; i < n_readers; i++)
        pthread_join(readers[i], NULL);

    expect = (uint64_t)n_readers * MSGS_PER + (uint64_t)n_readers;
    {
        unsigned waited = 0;
        while (atomic_load(&ctx.count) < expect && waited < 20000) {
            test_sleep_ms(10);
            waited += 10;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &ts);
    t1 = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
    awp_pool_get_metrics(pool, &m);

    printf("  processed=%llu expect=%llu errors=%d drops=%llu reorder=%d wall_ms=%llu\n",
           (unsigned long long)atomic_load(&ctx.count),
           (unsigned long long)expect,
           atomic_load(&errors),
           (unsigned long long)m.dropped,
           ctx.reorder,
           (unsigned long long)((t1 - t0) / 1000000ull));

    TEST_EQ_I(atomic_load(&errors), 0, "no submit errors");
    TEST_EQ_U64(atomic_load(&ctx.count), expect, "all messages");
    TEST_EQ_U64(m.dropped, 0, "zero drops");
    TEST_EQ_I(ctx.reorder, 0, "per-(reader,key) FIFO");

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    pthread_mutex_destroy(&ctx.mu);
    return g_fails > before_fails ? 1 : 0;
}

int main(int argc, char **argv)
{
    awp_ring_mode_t modes[] = {
        AWP_RING_SPSC, AWP_RING_MPSC, AWP_RING_SPMC, AWP_RING_MPMC
    };
    awp_ring_mode_t only;
    int all = 1;
    int i;

    if (argc > 1) {
        int pr = awp_mode_parse(argv[1], &only);
        if (pr < 0) {
            awp_mode_print_usage(argv[0]);
            return 2;
        }
        all = (pr == 1);
    }

    printf("e2e matrix over ring modes\n");
    for (i = 0; i < 4; i++) {
        if (!all && modes[i] != only)
            continue;
        (void)run_e2e_mode(modes[i]);
    }

    printf("\ne2e_modes: %d passed, %d failed\n", g_passes, g_fails);
    return g_fails ? 1 : 0;
}
