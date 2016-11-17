MODULES = pg_zlog
EXTENSION = pg_zlog
DATA = pg_zlog--0.1.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
