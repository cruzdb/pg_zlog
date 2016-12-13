MODULE_big = pg_zlog
OBJS = pg_zlog.o
EXTENSION = pg_zlog
DATA = pg_zlog--0.1.sql

# set from cli with make SHLIB_LINK="..."
#SHLIB_LINK = -L$HOME/install/lib -lrados -lzlog
#PG_CPPFLAGS = -I$HOME/install/include

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
