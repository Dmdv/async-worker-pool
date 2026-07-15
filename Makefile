# Async Worker Pool — portable Makefile (macOS + Linux)

CC      ?= cc
CFLAGS  ?= -O2 -g -Wall -Wextra -Wpedantic -std=c11
CFLAGS  += -Iinclude -pthread
LDFLAGS += -pthread

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
  CFLAGS += -D_GNU_SOURCE
endif

SRC := src/ring.c src/frame_pool.c src/shard.c src/worker.c src/supervisor.c src/pool.c
OBJ := $(SRC:.c=.o)
LIB := libawp.a

# --- tests ---
TEST_UNIT       := build/test_unit
TEST_UNIT_MODES := build/test_unit_modes
TEST_SUP        := build/test_supervisor
TEST_E2E        := build/test_e2e
TEST_E2E_MODES  := build/test_e2e_modes
TEST_RING       := build/test_ring_modes

# --- benches ---
BENCH_DISPATCH  := build/bench_dispatch
BENCH_ALL_MODES := build/bench_all_modes
BENCH_RING      := build/bench_ring

# --- examples ---
EX_SIMPLE := build/simple_publish
EX_SPSC   := build/example_spsc
EX_MPSC   := build/example_mpsc
EX_SPMC   := build/example_spmc
EX_MPMC   := build/example_mpmc

.PHONY: all lib tests bench examples e2e check clean dirs

all: dirs lib tests bench examples

dirs:
	@mkdir -p build src

lib: $(LIB)

$(LIB): $(OBJ)
	ar rcs $@ $^

src/%.o: src/%.c include/awp/awp.h src/internal.h
	$(CC) $(CFLAGS) -c $< -o $@

# tests
$(TEST_UNIT): tests/test_unit.c $(LIB)
	$(CC) $(CFLAGS) -Itests $< -L. -lawp $(LDFLAGS) -o $@

$(TEST_UNIT_MODES): tests/test_unit_modes.c $(LIB)
	$(CC) $(CFLAGS) -Itests $< -L. -lawp $(LDFLAGS) -o $@

$(TEST_SUP): tests/test_supervisor.c $(LIB)
	$(CC) $(CFLAGS) -Itests -Isrc $< -L. -lawp $(LDFLAGS) -o $@

$(TEST_E2E): tests/test_e2e.c $(LIB)
	$(CC) $(CFLAGS) -Itests $< -L. -lawp $(LDFLAGS) -o $@

$(TEST_E2E_MODES): tests/test_e2e_modes.c $(LIB)
	$(CC) $(CFLAGS) -Itests $< -L. -lawp $(LDFLAGS) -o $@

$(TEST_RING): tests/test_ring_modes.c $(LIB)
	$(CC) $(CFLAGS) -Itests -Isrc $< -L. -lawp $(LDFLAGS) -o $@

# benches
$(BENCH_DISPATCH): bench/bench_dispatch.c $(LIB)
	$(CC) $(CFLAGS) $< -L. -lawp $(LDFLAGS) -o $@

$(BENCH_ALL_MODES): bench/bench_all_modes.c $(LIB)
	$(CC) $(CFLAGS) $< -L. -lawp $(LDFLAGS) -o $@

$(BENCH_RING): bench/bench_ring.c $(LIB)
	$(CC) $(CFLAGS) -Isrc $< -L. -lawp $(LDFLAGS) -o $@

# examples
$(EX_SIMPLE): examples/simple_publish.c $(LIB)
	$(CC) $(CFLAGS) $< -L. -lawp $(LDFLAGS) -o $@

$(EX_SPSC): examples/example_spsc.c $(LIB)
	$(CC) $(CFLAGS) $< -L. -lawp $(LDFLAGS) -o $@

$(EX_MPSC): examples/example_mpsc.c $(LIB)
	$(CC) $(CFLAGS) $< -L. -lawp $(LDFLAGS) -o $@

$(EX_SPMC): examples/example_spmc.c $(LIB)
	$(CC) $(CFLAGS) -Isrc $< -L. -lawp $(LDFLAGS) -o $@

$(EX_MPMC): examples/example_mpmc.c $(LIB)
	$(CC) $(CFLAGS) -Isrc $< -L. -lawp $(LDFLAGS) -o $@

tests: dirs $(TEST_UNIT) $(TEST_UNIT_MODES) $(TEST_SUP) $(TEST_E2E) $(TEST_E2E_MODES) $(TEST_RING)
bench: dirs $(BENCH_DISPATCH) $(BENCH_ALL_MODES) $(BENCH_RING)
examples: dirs $(EX_SIMPLE) $(EX_SPSC) $(EX_MPSC) $(EX_SPMC) $(EX_MPMC)
e2e: $(TEST_E2E) $(TEST_E2E_MODES)

check: all
	@echo "=== unit (legacy) ===" && $(TEST_UNIT)
	@echo "=== unit modes (all) ===" && $(TEST_UNIT_MODES) all
	@echo "=== ring modes ===" && $(TEST_RING)
	@echo "=== supervisor ===" && $(TEST_SUP)
	@echo "=== e2e (default MPSC) ===" && $(TEST_E2E)
	@echo "=== e2e modes (all) ===" && $(TEST_E2E_MODES) all
	@echo "=== bench dispatch ===" && $(BENCH_DISPATCH) 3000 1000
	@echo "=== bench all modes ===" && $(BENCH_ALL_MODES) 4000 1000 all
	@echo "=== bench ring ===" && $(BENCH_RING) 50000 all
	@echo "=== examples ===" && \
		AWP_ENABLED=1 $(EX_SIMPLE) >/tmp/awp_ex_simple.out && \
		$(EX_SPSC) >/tmp/awp_ex_spsc.out && \
		$(EX_MPSC) >/tmp/awp_ex_mpsc.out && \
		$(EX_SPMC) >/tmp/awp_ex_spmc.out && \
		$(EX_MPMC) >/tmp/awp_ex_mpmc.out && \
		echo "examples ok" && tail -1 /tmp/awp_ex_*.out
	@echo "ALL CHECKS PASSED"

clean:
	rm -f $(OBJ) $(LIB)
	rm -rf build
