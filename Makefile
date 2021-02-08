MODULE_big = pg_diffix
OBJS = \
	$(WIN32RES) \
	pg_diffix.o \
	src/aggregates.o \
	src/config.o \
	src/hooks.o \
	src/random.o \
	src/validation.o

EXTENSION = pg_diffix
DATA = pg_diffix--0.0.1.sql

PG_CFLAGS = -std=c17

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

.PHONY: format
format:
	sh pgindent.sh
