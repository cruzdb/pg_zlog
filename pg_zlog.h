#ifndef PG_ZLOG_H
#define PG_ZLOG_H

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"

#include <rados/librados.h>
#include <zlog/capi.h>

typedef struct ZLogConn {
	rados_t rados;
	bool connected;

	rados_ioctx_t ioctx;
	bool ioctx_open;

	zlog_log_t log;
	bool log_open;
} ZLogConn;

ZLogConn *GetConnection(const char *name, const char *conf);
void PutConnection(ZLogConn *conn);

extern void deparse_query(Query *query, StringInfo buffer);

#endif
