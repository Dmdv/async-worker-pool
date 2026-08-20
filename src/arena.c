/**
 * @file arena.c
 * @brief High-Performance Cache-Aligned Arena Allocator for HFT & Low-Latency Systems.
 */
#include "internal.h"
#include <sys/mman.h>

struct awp_arena_chunk {
    struct awp_arena_chunk *next;
    size_t capacity;
    size_t offset;
};

void awp_arena_init(awp_arena_t *a, size_t default_chunk_size)
{
    if (!a)
        return;
    if (default_chunk_size == 0)
        default_chunk_size = 2 * 1024 * 1024; // Default 2MB HugePage size
    a->head = NULL;
    a->default_chunk_size = default_chunk_size;
    a->total_allocated = 0;
}

static awp_arena_chunk_t *arena_new_chunk(size_t min_capacity)
{
    size_t cap = min_capacity;
    if (cap < 64 * 1024)
        cap = 64 * 1024; // Minimum 64KB chunk
    cap = awp_round_up_pow2((uint32_t)cap);

    size_t total_bytes = sizeof(awp_arena_chunk_t) + cap + AWP_CACHELINE;
    void *mem = NULL;

    if (posix_memalign(&mem, AWP_CACHELINE, total_bytes) != 0 || !mem) {
        mem = malloc(total_bytes);
        if (!mem)
            return NULL;
    }

    awp_arena_chunk_t *chunk = (awp_arena_chunk_t *)mem;
    chunk->next = NULL;
    chunk->capacity = cap;
    chunk->offset = 0;
    return chunk;
}

void *awp_arena_alloc(awp_arena_t *a, size_t size, size_t alignment)
{
    if (!a || size == 0)
        return NULL;
    if (alignment == 0)
        alignment = sizeof(void *);

    // Align chunk header offset
    if (!a->head || (a->head->offset + size + alignment > a->head->capacity)) {
        size_t needed = size > a->default_chunk_size ? size : a->default_chunk_size;
        awp_arena_chunk_t *nc = arena_new_chunk(needed);
        if (!nc)
            return NULL;
        nc->next = a->head;
        a->head = nc;
    }

    uint8_t *base = (uint8_t *)(a->head + 1);
    uintptr_t cur = (uintptr_t)(base + a->head->offset);
    uintptr_t aligned = (cur + alignment - 1) & ~(alignment - 1);
    size_t padding = (size_t)(aligned - cur);

    if (a->head->offset + padding + size > a->head->capacity) {
        // Need dedicated chunk
        awp_arena_chunk_t *nc = arena_new_chunk(size);
        if (!nc)
            return NULL;
        nc->next = a->head;
        a->head = nc;

        base = (uint8_t *)(nc + 1);
        cur = (uintptr_t)base;
        aligned = (cur + alignment - 1) & ~(alignment - 1);
        padding = (size_t)(aligned - cur);
    }

    a->head->offset += padding + size;
    a->total_allocated += padding + size;
    return (void *)aligned;
}

void *awp_arena_calloc(awp_arena_t *a, size_t count, size_t size, size_t alignment)
{
    size_t total = count * size;
    void *ptr = awp_arena_alloc(a, total, alignment);
    if (ptr)
        memset(ptr, 0, total);
    return ptr;
}

void awp_arena_reset(awp_arena_t *a)
{
    if (!a)
        return;
    awp_arena_chunk_t *curr = a->head;
    while (curr) {
        curr->offset = 0;
        curr = curr->next;
    }
    a->total_allocated = 0;
}

void awp_arena_destroy(awp_arena_t *a)
{
    if (!a)
        return;
    awp_arena_chunk_t *curr = a->head;
    while (curr) {
        awp_arena_chunk_t *next = curr->next;
        free(curr);
        curr = next;
    }
    a->head = NULL;
    a->total_allocated = 0;
}
