#ifndef PG_ZLOG_H
#define PG_ZLOG_H

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"

#include <rados/librados.h>
#include <zlog/capi.h>

typedef struct ZLogConn {
  // connection objects
	rados_t cluster;
	rados_ioctx_t ioctx;
	zlog_log_t log;

  // connection states
  bool cluster_ok;
  bool ioctx_ok;
  bool log_ok;
} ZLogConn;

ZLogConn *GetConnection(const char *log_name);

extern void deparse_query(Query *query, StringInfo buffer);

#define PG_ZLOG_EXTENSION_NAME "pg_zlog"
#define PG_ZLOG_REPLICATED_TABLE_NAME "replicated_tables"
#define ATTR_NUM_REPLICATED_TABLES_RELATION_ID 1
#define ATTR_NUM_REPLICATED_TABLES_LOG 2
#define MAX_ZLOG_LOG_NAME_LENGTH 128
#define PG_ZLOG_METADATA_SCHEMA_NAME "pgzlog_metadata"
#define PG_ZLOG_CLUSTER_TABLE_NAME   "cluster"
#define PG_ZLOG_POOL_TABLE_NAME      "pool"
#define PG_ZLOG_LOG_TABLE_NAME       "log"

#define ZLOG_DEFAULT_HOST "localhost"
#define ZLOG_DEFAULT_PORT "5678"

#endif
