# Async Worker Pool — portable Makefile (macOS + Linux)

CC      ?= cc
CFLAGS  ?= -O2 -g -Wall -Wextra -Wpedantic -std=c11
CFLAGS  += -Iinclude -pthread
LDFLAGS += -pthread

# strcasecmp
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
  CFLAGS += -D_GNU_SOURCE
endif

SRC := src/ring.c src/frame_pool.c src/shard.c src/worker.c src/supervisor.c src/pool.c
OBJ := $(SRC:.c=.o)

LIB  := libawp.a
TEST_UNIT := build/test_unit
TEST_SUP  := build/test_supervisor
TEST_E2E  := build/test_e2e
TEST_RING := build/test_ring_modes
BENCH     := build/bench_dispatch
EXAMPLE   := build/simple_publish

.PHONY: all lib tests bench examples e2e check clean dirs

all: dirs lib tests bench examples

dirs:
	@mkdir -p build src

lib: $(LIB)

$(LIB): $(OBJ)
	ar rcs $@ $^

src/%.o: src/%.c include/awp/awp.h src/internal.h
	$(CC) $(CFLAGS) -c $< -o $@

$(TEST_UNIT): tests/test_unit.c $(LIB)
	$(CC) $(CFLAGS) -Itests $< -L. -lawp $(LDFLAGS) -o $@

$(TEST_SUP): tests/test_supervisor.c $(LIB)
	$(CC) $(CFLAGS) -Itests -Isrc $< -L. -lawp $(LDFLAGS) -o $@

$(TEST_E2E): tests/test_e2e.c $(LIB)
	$(CC) $(CFLAGS) -Itests $< -L. -lawp $(LDFLAGS) -o $@

$(TEST_RING): tests/test_ring_modes.c $(LIB)
	$(CC) $(CFLAGS) -Itests -Isrc $< -L. -lawp $(LDFLAGS) -o $@

$(BENCH): bench/bench_dispatch.c $(LIB)
	$(CC) $(CFLAGS) $< -L. -lawp $(LDFLAGS) -o $@

$(EXAMPLE): examples/simple_publish.c $(LIB)
	$(CC) $(CFLAGS) $< -L. -lawp $(LDFLAGS) -o $@

tests: dirs $(TEST_UNIT) $(TEST_SUP) $(TEST_E2E) $(TEST_RING)
bench: dirs $(BENCH)
examples: dirs $(EXAMPLE)
e2e: $(TEST_E2E)

check: all
	@echo "=== unit ===" && $(TEST_UNIT)
	@echo "=== ring modes ===" && $(TEST_RING)
	@echo "=== supervisor ===" && $(TEST_SUP)
	@echo "=== e2e ===" && $(TEST_E2E)
	@echo "=== bench ===" && $(BENCH) 3000 1000
	@echo "=== example ===" && AWP_ENABLED=1 $(EXAMPLE) >/tmp/awp_example.out && tail -5 /tmp/awp_example.out
	@echo "ALL CHECKS PASSED"

clean:
	rm -f $(OBJ) $(LIB)
	rm -rf build
