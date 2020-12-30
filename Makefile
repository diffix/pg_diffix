EXTENSION = pg_opendiffix
MODULE_big = pg_opendiffix
DATA = pg_opendiffix--0.0.1.sql
OBJS = pg_opendiffix.o

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

include $(PGXS)

.PHONY: format
format:
	sh pgindent.sh
