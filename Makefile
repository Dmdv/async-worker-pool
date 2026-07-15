# Async Worker Pool — portable Makefile (macOS + Linux)

CC      ?= cc
AR      ?= ar
RANLIB  ?= ranlib
# User-overridable flags. Mandatory -I/-pthread always appended (override).
CFLAGS  ?= -O2 -g -Wall -Wextra -Wpedantic -std=c11
LDFLAGS ?=
EXTRA_CFLAGS  ?=
EXTRA_LDFLAGS ?=
override CFLAGS  += -Iinclude -pthread $(EXTRA_CFLAGS)
override LDFLAGS += -pthread $(EXTRA_LDFLAGS)

PREFIX  ?= /usr/local
DESTDIR ?=

VERSION_MAJOR := 0
VERSION_MINOR := 1
VERSION_PATCH := 0
VERSION       := $(VERSION_MAJOR).$(VERSION_MINOR).$(VERSION_PATCH)

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
  override CFLAGS += -D_GNU_SOURCE
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
TEST_E2E_LIFE   := build/test_e2e_lifecycle
TEST_RING       := build/test_ring_modes
TEST_TEARDOWN   := build/test_teardown_contract
TEST_RESTART_FAIL := build/test_restart_create_fail

# Hooks object: worker rebuilt with -DAWP_TEST_HOOKS for injection tests
OBJ_HOOKS := $(patsubst src/%.c,build/hooks/%.o,$(SRC))
LIB_HOOKS := build/libawp_hooks.a

# --- benches ---
BENCH_DISPATCH  := build/bench_dispatch
BENCH_ALL_MODES := build/bench_all_modes
BENCH_RING      := build/bench_ring
BENCH_OPENLOOP  := build/bench_openloop

# --- examples ---
EX_SIMPLE := build/simple_publish
EX_SPSC   := build/example_spsc
EX_MPSC   := build/example_mpsc
EX_SPMC   := build/example_spmc
EX_MPMC   := build/example_mpmc

.PHONY: all lib tests bench examples e2e check check-all check-bench \
	check-sanitize check-func install uninstall clean dirs pkgconfig

all: dirs lib tests bench examples

dirs:
	@mkdir -p build src build/hooks

lib: $(LIB)

$(LIB): $(OBJ)
	$(AR) rcs $@ $^
	$(RANLIB) $@

src/%.o: src/%.c include/awp/awp.h src/internal.h
	$(CC) $(CFLAGS) -c $< -o $@

# Hook-enabled static lib (test only)
build/hooks/%.o: src/%.c include/awp/awp.h src/internal.h
	@mkdir -p build/hooks
	$(CC) $(CFLAGS) -DAWP_TEST_HOOKS -c $< -o $@

$(LIB_HOOKS): $(OBJ_HOOKS)
	$(AR) rcs $@ $^
	$(RANLIB) $@

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

$(TEST_E2E_LIFE): tests/test_e2e_lifecycle.c $(LIB)
	$(CC) $(CFLAGS) -Itests -Isrc $< -L. -lawp $(LDFLAGS) -o $@

$(TEST_RING): tests/test_ring_modes.c $(LIB)
	$(CC) $(CFLAGS) -Itests -Isrc $< -L. -lawp $(LDFLAGS) -o $@

$(TEST_TEARDOWN): tests/test_teardown_contract.c $(LIB)
	$(CC) $(CFLAGS) -Itests -Isrc $< -L. -lawp $(LDFLAGS) -o $@

$(TEST_RESTART_FAIL): tests/test_restart_create_fail.c $(LIB_HOOKS)
	$(CC) $(CFLAGS) -DAWP_TEST_HOOKS -Itests -Isrc $< -Lbuild -lawp_hooks $(LDFLAGS) -o $@

# benches
$(BENCH_DISPATCH): bench/bench_dispatch.c $(LIB)
	$(CC) $(CFLAGS) $< -L. -lawp $(LDFLAGS) -o $@

$(BENCH_ALL_MODES): bench/bench_all_modes.c $(LIB)
	$(CC) $(CFLAGS) $< -L. -lawp $(LDFLAGS) -o $@

$(BENCH_RING): bench/bench_ring.c $(LIB)
	$(CC) $(CFLAGS) -Isrc $< -L. -lawp $(LDFLAGS) -o $@

$(BENCH_OPENLOOP): bench/bench_openloop.c $(LIB)
	$(CC) $(CFLAGS) $< -L. -lawp $(LDFLAGS) -o $@

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

tests: dirs $(TEST_UNIT) $(TEST_UNIT_MODES) $(TEST_SUP) $(TEST_E2E) \
	$(TEST_E2E_MODES) $(TEST_E2E_LIFE) $(TEST_RING) $(TEST_TEARDOWN) $(TEST_RESTART_FAIL)
bench: dirs $(BENCH_DISPATCH) $(BENCH_ALL_MODES) $(BENCH_RING) $(BENCH_OPENLOOP)
examples: dirs $(EX_SIMPLE) $(EX_SPSC) $(EX_MPSC) $(EX_SPMC) $(EX_MPMC)
e2e: $(TEST_E2E) $(TEST_E2E_MODES)

# Functional correctness only — no latency thresholds (CI default).
check: check-func
check-func: tests
	@echo "=== unit (legacy) ===" && $(TEST_UNIT)
	@echo "=== unit modes (all) ===" && $(TEST_UNIT_MODES) all
	@echo "=== ring modes (exact ID) ===" && $(TEST_RING)
	@echo "=== supervisor ===" && $(TEST_SUP)
	@echo "=== e2e (default MPSC) ===" && $(TEST_E2E)
	@echo "=== e2e modes (all) ===" && $(TEST_E2E_MODES) all
	@echo "=== e2e lifecycle ===" && $(TEST_E2E_LIFE)
	@echo "=== teardown contract ===" && $(TEST_TEARDOWN)
	@echo "=== restart create fail ===" && $(TEST_RESTART_FAIL)
	@echo "FUNCTIONAL CHECKS PASSED"

check-bench: bench examples
	@echo "=== bench dispatch ===" && $(BENCH_DISPATCH) 3000 1000
	@echo "=== bench all modes ===" && $(BENCH_ALL_MODES) 4000 1000 all
	@echo "=== bench ring ===" && $(BENCH_RING) 50000 all
	@echo "=== openloop mock (1000/s, 500 msgs) ===" && $(BENCH_OPENLOOP) 1000 500 4
	@echo "=== examples ===" && \
		AWP_ENABLED=1 $(EX_SIMPLE) >/tmp/awp_ex_simple.out && \
		$(EX_SPSC) >/tmp/awp_ex_spsc.out && \
		$(EX_MPSC) >/tmp/awp_ex_mpsc.out && \
		$(EX_SPMC) >/tmp/awp_ex_spmc.out && \
		$(EX_MPMC) >/tmp/awp_ex_mpmc.out && \
		echo "examples ok"
	@echo "BENCH/EXAMPLES PASSED"

check-all: check-func check-bench
	@echo "ALL CHECKS PASSED"

# ASan+UBSan functional suite.
# Clean-path tests: LSan on. Quarantine tests: LSan off (intentional leak).
SAN_CFLAGS  := -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer
SAN_LDFLAGS := -fsanitize=address,undefined

check-sanitize:
	$(MAKE) clean
	$(MAKE) EXTRA_CFLAGS="$(SAN_CFLAGS)" EXTRA_LDFLAGS="$(SAN_LDFLAGS)" \
		tests
	@echo "=== LSan ON (clean functional subset) ==="
	ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 \
		$(TEST_UNIT)
	ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 \
		$(TEST_UNIT_MODES) all
	ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 \
		$(TEST_RING)
	ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 \
		$(TEST_E2E)
	ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 \
		$(TEST_E2E_MODES) all
	@echo "=== LSan OFF (quarantine / intentional-leak paths) ==="
	ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 \
		$(TEST_SUP)
	ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 \
		$(TEST_E2E_LIFE)
	ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 \
		$(TEST_TEARDOWN)
	ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 \
		$(TEST_RESTART_FAIL)
	@echo "SANITIZE CHECKS PASSED"

pkgconfig: force-awp-pc
	@true

# Always regenerate so PREFIX changes are never stale.
force-awp-pc: awp.pc.in
	sed -e 's|@PREFIX@|$(PREFIX)|g' -e 's|@VERSION@|$(VERSION)|g' awp.pc.in > awp.pc

awp.pc: force-awp-pc

install: lib force-awp-pc
	install -d $(DESTDIR)$(PREFIX)/include/awp
	install -d $(DESTDIR)$(PREFIX)/lib/pkgconfig
	install -m 644 include/awp/awp.h $(DESTDIR)$(PREFIX)/include/awp/awp.h
	install -m 644 $(LIB) $(DESTDIR)$(PREFIX)/lib/libawp.a
	install -m 644 awp.pc $(DESTDIR)$(PREFIX)/lib/pkgconfig/awp.pc
	install -d $(DESTDIR)$(PREFIX)/share/doc/awp
	install -m 644 LICENSE CHANGELOG.md README.md $(DESTDIR)$(PREFIX)/share/doc/awp/

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/include/awp/awp.h
	rmdir $(DESTDIR)$(PREFIX)/include/awp 2>/dev/null || true
	rm -f $(DESTDIR)$(PREFIX)/lib/libawp.a
	rm -f $(DESTDIR)$(PREFIX)/lib/pkgconfig/awp.pc
	rm -rf $(DESTDIR)$(PREFIX)/share/doc/awp

clean:
	rm -f $(OBJ) $(LIB) awp.pc
	rm -rf build
