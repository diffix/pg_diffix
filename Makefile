MODULE_big = pg_opendiffix
OBJS = \
	$(WIN32RES) \
	pg_opendiffix/validation.o \
	pg_opendiffix/config.o \
	pg_opendiffix/hooks.o \
	pg_opendiffix.o

EXTENSION = pg_opendiffix
DATA = pg_opendiffix--0.0.1.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

.PHONY: format
format:
	sh pgindent.sh
