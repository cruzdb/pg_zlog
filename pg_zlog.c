#include "postgres.h"
#include "fmgr.h"
#include "access/stratnum.h"
#include "access/skey.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "storage/lockdefs.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/tqual.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#include <rados/librados.h>
#include <zlog/capi.h>

#define PG_ZLOG_EXTENSION_NAME "pg_zlog"
#define PG_ZLOG_METADATA_SCHEMA_NAME "pgzlog_metadata"
#define PG_ZLOG_REPLICATED_TABLE_NAME "replicated_tables"
#define ATTR_NUM_REPLICATED_TABLES_RELATION_ID 1
#define ATTR_NUM_REPLICATED_TABLES_LOG 2
#define MAX_ZLOG_LOG_NAME_LENGTH 128

PG_MODULE_MAGIC;

/* whether writes go through zlog */
static bool ZLogEnabled = true;

/* restore these hooks on unload */
static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;

typedef struct ZLogConn {
	rados_t rados;
	bool connected;

	rados_ioctx_t ioctx;
	bool ioctx_open;

	zlog_log_t log;
	bool log_open;
} ZLogConn;

/*
 * TODO:
 *   - what is the purpose of checking things like the extension being
 *   present? the only thing I can think of is that there may be a race
 *   condition with loading or unloading the extension.
 */
static int IsPgZLogActive(void)
{
	Oid oid;
	Oid namespace_oid;

	if (!ZLogEnabled)
	{
		return false;
	}

	if (!IsTransactionState())
	{
		return false;
	}

	oid = get_extension_oid(PG_ZLOG_EXTENSION_NAME, true);
	if (oid == InvalidOid)
	{
		return false;
	}

	namespace_oid = get_namespace_oid(PG_ZLOG_METADATA_SCHEMA_NAME, true);
	if (namespace_oid == InvalidOid)
	{
		return false;
	}

	oid = get_relname_relid(PG_ZLOG_REPLICATED_TABLE_NAME, namespace_oid);
	if (oid == InvalidOid)
	{
		return false;
	}

	return true;
}

/*
 *
 */
static bool IsZLogTable(Oid tableOid)
{
	bool isZLogTable;
	RangeVar *heapRangeVar;
	Relation heapRelation;
	HeapScanDesc scanDesc;
	const int scanKeyCount = 1;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple heapTuple;

	if (tableOid == InvalidOid)
		return false;

	heapRangeVar = makeRangeVar(PG_ZLOG_METADATA_SCHEMA_NAME,
			PG_ZLOG_REPLICATED_TABLE_NAME, -1);

	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], ATTR_NUM_REPLICATED_TABLES_RELATION_ID,
			InvalidStrategy, F_OIDEQ, ObjectIdGetDatum(tableOid));

	scanDesc = heap_beginscan(heapRelation, SnapshotSelf, scanKeyCount, scanKey);

	heapTuple = heap_getnext(scanDesc, ForwardScanDirection);

	isZLogTable = HeapTupleIsValid(heapTuple);

	heap_endscan(scanDesc);
	relation_close(heapRelation, AccessShareLock);

	return isZLogTable;
}

static Oid ExtractTableOid(Node *node)
{
	Oid tableOid = InvalidOid;

	NodeTag nodeType = nodeTag(node);
	if (nodeType == T_RangeTblEntry)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) node;
		tableOid = rangeTableEntry->relid;
	}
	else if (nodeType == T_RangeVar)
	{
		RangeVar *rangeVar = (RangeVar *) node;
		tableOid = RangeVarGetRelid(rangeVar, NoLock, true);
	}

	return tableOid;
}

/*
 * Does the range contain a ZLog replicated table
 */
static bool HasZLogTable(List *rangeTableList)
{
	ListCell *rangeTableCell = NULL;

	foreach(rangeTableCell, rangeTableList)
	{
		Oid rangeTableOid = ExtractTableOid((Node *) lfirst(rangeTableCell));
		if (IsZLogTable(rangeTableOid))
		{
			return true;
		}
	}

	return false;
}

/*
 *
 */
static char *ZLogTableLog(Oid zlogTableOid)
{
	char *logName;
	RangeVar *heapRangeVar;
	Relation heapRelation;
	HeapScanDesc scanDesc;
	const int scanKeyCount = 1;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple heapTuple;

	heapRangeVar = makeRangeVar(PG_ZLOG_METADATA_SCHEMA_NAME,
			PG_ZLOG_REPLICATED_TABLE_NAME, -1);
	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);

	ScanKeyInit(&scanKey[0], ATTR_NUM_REPLICATED_TABLES_RELATION_ID, InvalidStrategy,
				F_OIDEQ, ObjectIdGetDatum(zlogTableOid));

	scanDesc = heap_beginscan(heapRelation, SnapshotSelf, scanKeyCount, scanKey);

	heapTuple = heap_getnext(scanDesc, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(heapRelation);
		bool isNull = false;

		Datum logNameDatum = heap_getattr(heapTuple,
			ATTR_NUM_REPLICATED_TABLES_LOG, tupleDescriptor, &isNull);
		logName = TextDatumGetCString(logNameDatum);
	}
	else
	{
		logName = NULL;
	}

	heap_endscan(scanDesc);
	relation_close(heapRelation, AccessShareLock);

	return logName;
}

/*
 *
 */
static char *GetLogName(List *rangeTableList)
{
	ListCell *rangeTableCell;
	char *queryLogName = NULL;

	foreach(rangeTableCell, rangeTableList)
	{
		char *tableLogName;

		Oid rangeTableOid = ExtractTableOid((Node*)lfirst(rangeTableCell));
		if (rangeTableOid == InvalidOid)
		{
			continue;
		}

		tableLogName = ZLogTableLog(rangeTableOid);
		if (tableLogName == NULL)
		{
			char *relationName = get_rel_name(rangeTableOid);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("relation \"%s\" is not managed by pg_zlog", relationName)));
		}
		else if (queryLogName == NULL)
		{
			queryLogName = tableLogName;
		}
		else
		{
			int compare = strncmp(tableLogName, queryLogName, MAX_ZLOG_LOG_NAME_LENGTH);
			if (compare)
			{
				ereport(ERROR, (errmsg("cannot run queries spanning more than a single log.")));
			}
		}
	}

	return queryLogName;

}

/*
 *
 */
static void
PutConnection(ZLogConn *conn)
{
	if (conn == NULL)
	{
		return;
	}

	if (conn->log_open)
	{
		zlog_destroy(conn->log);
	}

	if (conn->ioctx_open)
	{
		rados_ioctx_destroy(conn->ioctx);
	}

	if (conn->connected)
	{
		rados_shutdown(conn->rados);
	}

	pfree(conn);
}

/*
 *
 */
static ZLogConn *
GetConnection(const char *name, const char *conf)
{
	ZLogConn *volatile conn = NULL;

	PG_TRY();
	{
		int ret;

		conn = (ZLogConn *) palloc0(sizeof(*conn));

		ret = rados_create(&conn->rados, NULL);
		if (ret)
		{
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
				errmsg("pg_zlog could not create rados context: %d", ret)));
		}

		ret = rados_conf_read_file(conn->rados, conf);
		if (ret)
		{
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
				errmsg("pg_zlog could not configure rados context: %d", ret)));
		}

		ret = rados_connect(conn->rados);
		if (ret)
		{
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
				errmsg("pg_zlog could not connect to cluster: %d", ret)));
		}

		conn->connected = true;

		ret = rados_ioctx_create(conn->rados, "rbd", &conn->ioctx);
		if (ret)
		{
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
				errmsg("pg_zlog could not open ioctx: %d", ret)));
		}

		conn->ioctx_open = true;

		ret = zlog_open_or_create(conn->ioctx, name, "localhost", "5678", &conn->log);
		if (ret)
		{
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
				errmsg("pg_zlog could not open log: %d", ret)));
		}
	}
	PG_CATCH();
	{
		PutConnection(conn);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return conn;
}

static void
ZLogSetApplied(const char *logName, int64 pos)
{
	Oid argTypes[] = {
		TEXTOID,
		INT8OID
	};
	Datum argValues[] = {
		CStringGetTextDatum(logName),
		Int64GetDatum(pos)
	};

	SPI_connect();

	SPI_execute_with_args(
		"UPDATE pgzlog_metadata.log SET last_applied_pos = $2 WHERE name = $1",
		2, argTypes, argValues, NULL, false, 1);

	SPI_finish();
}

static int64
ZLogLastAppliedPos(const char *logName)
{
	int64 pos;
	Datum posDatum;
	bool isNull;
	Oid argTypes[] = {
		TEXTOID
	};
	Datum argValues[] = {
		CStringGetTextDatum(logName)
	};

	SPI_connect();

	SPI_execute_with_args(
		"SELECT last_applied_pos FROM pgzlog_metadata.log WHERE name = $1",
		1, argTypes, argValues, NULL, false, 1);

	posDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
			1, &isNull);
	pos = DatumGetInt64(posDatum);

	SPI_finish();

	return pos;
}

/*
 * TODO:
 * - handle errors like not-written and filled
 * - handle large query strings. is there psql limit?
 */
static void
ApplyLogEntries(ZLogConn *conn, const char *logName,
		const int64 start, const int64 end)
{
	int64 cur;

	if (start > end)
	{
		ereport(ERROR, (errmsg("pg_zlog unexpected from/to "
			INT64_FORMAT "/" INT64_FORMAT, start, end)));
	}
	else if (start == end)
	{
		return;
	}

	SPI_connect();

	for (cur = start; cur < end; cur++)
	{
		char readQuery[4096];
		Datum argValues[3];
		Oid argTypes[] = {
			TEXTOID,
			TEXTOID,
			INT8OID
		};

		int ret = zlog_read(conn->log, cur, readQuery, sizeof(readQuery));
		if (ret < 0)
		{
			ereport(ERROR, (errmsg("pg_zlog cannot read log: %d", ret)));
		}

		argValues[0] = CStringGetTextDatum(logName);
		argValues[1] = CStringGetTextDatum(readQuery);
		argValues[2] = Int64GetDatum(cur);

		ret = SPI_execute_with_args("SELECT pgzlog_apply_update($1,$2,$3)",
				3, argTypes, argValues, NULL, false, 1);
		if (ret < 0)
		{
			ereport(ERROR, (errmsg("pg_zlog cannot apply query: %d", ret)));
		}
	}

	SPI_finish();
	CommandCounterIncrement();
}

/*
 * TODO:
 *   - support optimistic consistency mode (read-your-own-writes)
 */
static void PrepareConsistentRead(const char *logName)
{
	uint64_t tail;
	int64 last_applied;
	int ret;
	ZLogConn *conn;

	conn = GetConnection(logName, "/home/nwatkins/ceph/build/ceph.conf");

	ret = zlog_checktail(conn->log, &tail);
	if (ret)
	{
		ereport(ERROR, (errmsg("pg_zlog could not check tail: %d", ret)));
	}

	last_applied = ZLogLastAppliedPos(logName);

	ApplyLogEntries(conn, logName, last_applied+1, (int64)tail);

	PutConnection(conn);
}

static void PrepareConsistentWrite(const char *logName, const char *queryString)
{
	int ret;
	uint64_t appended_pos;
	int64 last_applied;
	ZLogConn *conn;

	conn = GetConnection(logName, "/home/nwatkins/ceph/build/ceph.conf");

	ret = zlog_append(conn->log, (void*)queryString,
			strlen(queryString)+1, &appended_pos);
	if (ret)
	{
		ereport(ERROR, (errmsg("pg_zlog could not append to log: %d", ret)));
	}

	last_applied = ZLogLastAppliedPos(logName);

	ApplyLogEntries(conn, logName, last_applied+1, (int64)appended_pos);

	ZLogSetApplied(logName, (int64)appended_pos);

	PutConnection(conn);
}

/*
 * Intercept utility statements.
 */
static void PgZLogProcessUtility(Node *parsetree, const char *queryString,
		ProcessUtilityContext context, ParamListInfo params,
		DestReceiver *dest, char *completionTag)
{
	if (IsPgZLogActive())
	{
		NodeTag statementType = nodeTag(parsetree);
		if (statementType == T_TruncateStmt)
		{
			TruncateStmt *truncateStatement = (TruncateStmt *) parsetree;
			List *relations = truncateStatement->relations;
			if (HasZLogTable(relations))
			{
				char *logName = GetLogName(relations);
				PrepareConsistentWrite(logName, queryString);
			}
		}
		else if (statementType == T_IndexStmt)
		{
			IndexStmt *indexStatement = (IndexStmt *) parsetree;
			Oid tableOid = ExtractTableOid((Node *) indexStatement->relation);
			if (IsZLogTable(tableOid))
			{
				char *logName = ZLogTableLog(tableOid);
				PrepareConsistentWrite(logName, queryString);
			}
		}
		else if (statementType == T_AlterTableStmt)
		{
			AlterTableStmt *alterStatement = (AlterTableStmt *) parsetree;
			Oid tableOid = ExtractTableOid((Node *) alterStatement->relation);
			if (IsZLogTable(tableOid))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("ALTER TABLE commands on zlog tables are unsupported")));
			}
		}
		else if (statementType == T_CopyStmt)
		{
			CopyStmt *copyStatement = (CopyStmt *) parsetree;
			RangeVar *relation = copyStatement->relation;
			Node *rawQuery = copyObject(copyStatement->query);

			if (relation)
			{
				Oid tableOid = ExtractTableOid((Node *) relation);
				if (IsZLogTable(tableOid))
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("COPY commands on zlog tables are unsupported")));
				}
			}
			else if (rawQuery)
			{
				Query *parsedQuery = NULL;
				List *queryList = pg_analyze_and_rewrite(rawQuery,
						queryString, NULL, 0);

				if (list_length(queryList) != 1)
				{
					ereport(ERROR, (errmsg("unexpected rewrite result")));
				}

				parsedQuery = (Query *) linitial(queryList);

				if (HasZLogTable(parsedQuery->rtable))
				{
					char *logName = GetLogName(parsedQuery->rtable);
					PrepareConsistentRead(logName);
				}
			}
		}
	}

	if (PreviousProcessUtilityHook)
	{
		PreviousProcessUtilityHook(parsetree, queryString, context,
				params, dest, completionTag);
	}
	else
	{
		standard_ProcessUtility(parsetree, queryString, context,
				params, dest, completionTag);
	}
}

void _PG_init(void);
void _PG_init(void)
{
	DefineCustomBoolVariable("pg_zlog.enabled",
		"If enabled, pg_zlog handles queries on zlog tables",
		NULL, &ZLogEnabled, true, PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = PgZLogProcessUtility;
}

void _PG_fini(void);
void _PG_fini(void)
{
	ProcessUtility_hook = PreviousProcessUtilityHook;
}
