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

testdb:
	rm -rf testdb_data
	initdb -D testdb_data -A trust
	sed -i "/shared_preload_libraries/s/.*/shared_preload_libraries \
		= 'pg_zlog'/" testdb_data/postgresql.conf

testdbrun:
	postgres -D testdb_data

testdbinit:
	echo 'CREATE EXTENSION pg_zlog;' | psql postgres
	echo 'CREATE TABLE coordinates (x int, y int);' | psql postgres
	echo "SELECT pgzlog_create_log('mylog1', 'rbd', null);" | psql postgres
	echo "SELECT pgzlog_replicate_table('mylog1', 'coordinates');" | psql postgres
