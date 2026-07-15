#include "internal.h"

/* FNV-1a 64-bit */
uint64_t awp_hash_key(const char *feed, const char *symbol)
{
    const uint64_t offset = 14695981039346656037ull;
    const uint64_t prime  = 1099511628211ull;
    uint64_t h = offset;
    const unsigned char *p;

    if (!feed)
        feed = "";
    if (!symbol)
        symbol = "";

    for (p = (const unsigned char *)feed; *p; p++) {
        h ^= (uint64_t)(*p);
        h *= prime;
    }
    h ^= (uint64_t)0x1F;
    h *= prime;
    for (p = (const unsigned char *)symbol; *p; p++) {
        h ^= (uint64_t)(*p);
        h *= prime;
    }
    return h;
}

static int feed_is_broadcast(const awp_pool_t *pool, const char *feed)
{
    uint32_t i;
    if (!pool || !feed || pool->n_broadcast_feeds == 0)
        return 0;
    for (i = 0; i < pool->n_broadcast_feeds; i++) {
        if (pool->broadcast_feeds[i] &&
            strcmp(pool->broadcast_feeds[i], feed) == 0)
            return 1;
    }
    return 0;
}

uint32_t awp_compute_shard(const awp_pool_t *pool,
                           const char *feed,
                           const char *symbol,
                           uint32_t flags)
{
    uint64_t h;
    int is_bc;

    if (!pool || pool->cfg.n_workers == 0)
        return 0;

    is_bc = (flags & AWP_FRAME_BROADCAST) != 0 || feed_is_broadcast(pool, feed);

    if (is_bc && pool->cfg.n_broadcast_workers > 0) {
        /* Round-robin broadcast among dedicated workers via hash of feed only. */
        h = awp_hash_key(feed, "");
        return (uint32_t)(h % (uint64_t)pool->cfg.n_broadcast_workers);
    }

    if (pool->n_shard_workers == 0)
        return 0;

    h = awp_hash_key(feed, symbol);
    return pool->shard_base +
           (uint32_t)(h % (uint64_t)pool->n_shard_workers);
}

uint32_t awp_shard_of(const awp_pool_t *pool,
                      const char *feed,
                      const char *symbol,
                      uint32_t flags)
{
    uint32_t s;
    if (!pool)
        return 0;
    awp_api_enter((awp_pool_t *)pool);
    s = awp_compute_shard(pool, feed, symbol, flags);
    awp_api_leave((awp_pool_t *)pool);
    return s;
}
