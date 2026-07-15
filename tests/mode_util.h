#ifndef AWP_MODE_UTIL_H
#define AWP_MODE_UTIL_H

#include "awp/awp.h"

#include <stdio.h>
#include <string.h>

static inline const char *awp_mode_name(awp_ring_mode_t m)
{
    switch (m) {
    case AWP_RING_SPSC: return "SPSC";
    case AWP_RING_MPSC: return "MPSC";
    case AWP_RING_SPMC: return "SPMC";
    case AWP_RING_MPMC: return "MPMC";
    default: return "UNKNOWN";
    }
}

static inline int awp_mode_parse(const char *s, awp_ring_mode_t *out)
{
    if (!s || !out)
        return -1;
    if (strcmp(s, "spsc") == 0 || strcmp(s, "SPSC") == 0) {
        *out = AWP_RING_SPSC;
        return 0;
    }
    if (strcmp(s, "mpsc") == 0 || strcmp(s, "MPSC") == 0) {
        *out = AWP_RING_MPSC;
        return 0;
    }
    if (strcmp(s, "spmc") == 0 || strcmp(s, "SPMC") == 0) {
        *out = AWP_RING_SPMC;
        return 0;
    }
    if (strcmp(s, "mpmc") == 0 || strcmp(s, "MPMC") == 0) {
        *out = AWP_RING_MPMC;
        return 0;
    }
    if (strcmp(s, "all") == 0 || strcmp(s, "ALL") == 0)
        return 1; /* special: all modes */
    return -1;
}

static inline void awp_mode_print_usage(const char *prog)
{
    fprintf(stderr,
            "Usage: %s [spsc|mpsc|spmc|mpmc|all] [extra...]\n"
            "  default: all\n",
            prog);
}

/** Suggested producer count for pool submit stress under a mode. */
static inline int awp_mode_suggest_producers(awp_ring_mode_t m)
{
    switch (m) {
    case AWP_RING_SPSC:
    case AWP_RING_SPMC:
        return 1;
    case AWP_RING_MPSC:
    case AWP_RING_MPMC:
        return 4;
    default:
        return 1;
    }
}

#endif
