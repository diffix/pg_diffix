MODULE_big = pg_diffix
OBJS = \
	$(WIN32RES) \
	$(patsubst %.c,%.o,$(wildcard src/*.c))

EXTENSION = pg_diffix
DATA = pg_diffix--0.0.1.sql
REGRESS = tests

PG_CFLAGS = -std=c17

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

.PHONY: format
format:
	sh pgindent.sh
