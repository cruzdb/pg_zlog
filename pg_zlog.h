#ifndef PG_ZLOG_H
#define PG_ZLOG_H

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"

extern void deparse_query(Query *query, StringInfo buffer);

#endif
