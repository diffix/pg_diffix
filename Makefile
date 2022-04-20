EXTENSION = $(shell jq -r '.name' META.json)
EXTVERSION = $(shell jq -r '.version' META.json)

MODULE_big = $(EXTENSION)
OBJS = \
	$(WIN32RES) \
	$(patsubst %.c,%.o,$(wildcard src/*.c)) \
	$(patsubst %.c,%.o,$(wildcard src/*/*.c))
DATA = $(wildcard *--*.sql)
DOCS = $(wildcard docs/*.md)
TESTS = $(sort $(wildcard test/sql/*.sql))
REGRESS = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test

PG_CFLAGS := -std=c11 -Wno-declaration-after-statement -Werror-implicit-function-declaration
ifneq ($(TARGET),release)
	PG_CFLAGS := $(PG_CFLAGS) -DDEBUG -DUSE_ASSERT_CHECKING -O0
endif

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

override with_llvm=no
include $(PGXS)

image:
	docker build --target $(EXTENSION) -t $(EXTENSION) .

demo-image:
	docker build --target $(EXTENSION)_demo -t $(EXTENSION)_demo .

package:
	git archive --format zip --prefix=$(EXTENSION)-$(EXTVERSION)/ -o $(EXTENSION)-$(EXTVERSION).zip --worktree-attributes HEAD
