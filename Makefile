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

PG_CFLAGS := -std=c11 -Wno-declaration-after-statement -Werror-implicit-function-declaration
ifneq ($(TARGET),release)
	PG_CFLAGS := $(PG_CFLAGS) -DDEBUG -DUSE_ASSERT_CHECKING
endif

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

override with_llvm=no
include $(PGXS)

image:
	docker build --target pg_diffix -t pg_diffix .

demo-image:
	docker build --target pg_diffix_demo -t pg_diffix_demo .
