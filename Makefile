MODULE_big = pg_diffix
OBJS = \
	$(WIN32RES) \
	$(patsubst %.c,%.o,$(wildcard src/*.c)) \
	$(patsubst %.c,%.o,$(wildcard src/*/*.c))

EXTENSION = pg_diffix
DATA = pg_diffix--0.0.1.sql

TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test

PG_CFLAGS = -std=c11 -Wno-declaration-after-statement -Werror-implicit-function-declaration

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
