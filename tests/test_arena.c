/**
 * Unit tests for awp_arena_t
 */
#include "test_common.h"
#include "awp/awp.h"

static void test_arena_basic_alloc(void)
{
    awp_arena_t arena;
    awp_arena_init(&arena, 64 * 1024);

    void *p1 = awp_arena_alloc(&arena, 128, 64);
    TEST_CHECK(p1 != NULL, "p1 allocated");
    TEST_CHECK(((uintptr_t)p1 % 64) == 0, "p1 64-byte aligned");

    void *p2 = awp_arena_alloc(&arena, 256, 64);
    TEST_CHECK(p2 != NULL, "p2 allocated");
    TEST_CHECK(((uintptr_t)p2 % 64) == 0, "p2 64-byte aligned");
    TEST_CHECK((uint8_t *)p2 >= (uint8_t *)p1 + 128, "p2 after p1");

    void *p3 = awp_arena_calloc(&arena, 10, sizeof(uint64_t), 8);
    TEST_CHECK(p3 != NULL, "p3 calloc allocated");
    uint64_t *u = (uint64_t *)p3;
    for (int i = 0; i < 10; i++)
        TEST_EQ_U64(u[i], 0, "zeroed memory");

    // Reset without destroying underlying chunks
    awp_arena_reset(&arena);
    TEST_EQ_U64(arena.total_allocated, 0, "reset total_allocated");

    void *p4 = awp_arena_alloc(&arena, 128, 64);
    TEST_CHECK(p4 != NULL, "p4 allocated after reset");
    TEST_CHECK(p4 == p1, "p4 reuses first chunk buffer");

    awp_arena_destroy(&arena);
    TEST_CHECK(arena.head == NULL, "arena destroyed");
}

int main(void)
{
    printf("=== arena unit tests ===\n");
    test_arena_basic_alloc();
    printf("arena: %d passed, %d failed\n", g_passes, g_fails);
    return g_fails > 0 ? 1 : 0;
}
