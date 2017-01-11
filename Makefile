MODULE_big = pg_zlog
OBJS = pg_zlog.o connection.o ruleutils_94.o ruleutils_95.o ruleutils_96.o
EXTENSION = pg_zlog
DATA = pg_zlog--0.1.sql

SHLIB_LINK = -lrados -lzlog

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

TEST_DB_NAME ?= testdb_data1
TEST_DB_PORT ?= 5432

testdb:
	rm -rf $(TEST_DB_NAME)
	initdb -D $(TEST_DB_NAME) -A trust
	sed -i "/shared_preload_libraries/s/.*/shared_preload_libraries \
		= 'pg_zlog'/" $(TEST_DB_NAME)/postgresql.conf

testdbrun:
	postgres -D $(TEST_DB_NAME) -p $(TEST_DB_PORT)

testdbinit:
	echo 'CREATE EXTENSION pg_zlog;' | psql postgres -p $(TEST_DB_PORT)
	echo 'CREATE TABLE coordinates (x int, y int);' | psql postgres -p $(TEST_DB_PORT)
	echo "SELECT pgzlog_add_cluster('ceph', null);" | psql postgres -p $(TEST_DB_PORT)
	echo "SELECT pgzlog_add_pool('ceph', 'rbd');" | psql postgres -p $(TEST_DB_PORT)
	echo "SELECT pgzlog_add_log('rbd', 'log0', null, null);" | psql postgres -p $(TEST_DB_PORT)
	echo "SELECT pgzlog_replicate_table('log0', 'coordinates');" | psql postgres -p $(TEST_DB_PORT)
