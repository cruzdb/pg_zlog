#include "postgres.h"
#include "fmgr.h"
#include "access/stratnum.h"
#include "access/skey.h"
#include "access/heapam.h"
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

PG_MODULE_MAGIC;

/* whether writes go through zlog */
static bool ZLogEnabled = true;

/* restore these hooks on unload */
static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;

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

struct ZLogConn {
	bool r, i, l;
	rados_t rados;
	rados_ioctx_t ioctx;
	zlog_log_t log;
};

static void ZLogConnInit(struct ZLogConn *conn)
{
	conn->r = false;
	conn->i = false;
	conn->l = false;
}

static void ZLogConnect(struct ZLogConn *conn)
{
	int ret;

	ret = rados_create(&conn->rados, NULL);
	if (ret)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not create rados cluster object: ret=%d", ret)));
	}

	conn->r = true;

	ret = rados_conf_read_file(conn->rados, "/home/nwatkins/ceph/build/ceph.conf");
	if (ret)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not rados read conf: ret=%d", ret)));
	}

	ret = rados_connect(conn->rados);
	if (ret)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not connect to rados: ret=%d", ret)));
	}

	ret = rados_ioctx_create(conn->rados, "rbd", &conn->ioctx);
	if (ret)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not open ioctx: ret=%d", ret)));
	}

	conn->i = true;

	ret = zlog_open_or_create(conn->ioctx, "mylog", "localhost", "5678", &conn->log);
	if (ret)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not create log instance ret=%d", ret)));
	}

	conn->l = true;

}

static void ZLogConnDisconnect(struct ZLogConn *conn)
{
	if (conn->l)
		zlog_destroy(conn->log);
	if (conn->i)
		rados_ioctx_destroy(conn->ioctx);
	if (conn->r)
		rados_shutdown(conn->rados);
}

/*
 * TODO:
 *   - support optimistic consistency mode (read-your-own-writes)
 */
static void PrepareConsistentRead(void)
{
	uint64_t pos;
	uint64_t cur;
	int ret;
	char readQuery[4096];
	struct ZLogConn conn;
	Oid argTypes[1];
	Datum argValues[1];

	ZLogConnInit(&conn);
	ZLogConnect(&conn);

	ret = zlog_checktail(conn.log, &pos);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("PrepareConsistentREAD:CheckTail: %d", ret)));
		return;
	}

	SPI_connect();

	/*
	 * Execute preceeding queries.
	 *
	 * TODO:
	 *   - cur should start from db state
	 *   - handle errors like not-written and filled
	 *   - handle large query strings. is there psql limit?
	 */
	for (cur = 0; cur < pos; cur++) {
		ret = zlog_read(conn.log, cur, readQuery, sizeof(readQuery));
		if (ret < 0) {
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("PrepareConsistentRead:Read: %d", ret)));
			return;
		}

		ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("MOG PrepareConsistentRead:Read %llu/%s",
				(unsigned long long)cur, readQuery)));


		argTypes[0] = TEXTOID;
		argValues[0] = CStringGetTextDatum(readQuery);
		ret = SPI_execute_with_args("SELECT pgzlog_apply_update($1)",
				1, argTypes, argValues, NULL, false, 1);
		if (ret < 0) {
			ereport(WARNING, (errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("PrepareConsistentRead:SPI_execute: %d", ret)));
		} else {
			ereport(WARNING, (errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("PrepareConsistentRead:SPI_execute: %d", ret)));
		}
	}

	SPI_finish();
	CommandCounterIncrement();

	//CommandCounterIncrement();
	ZLogConnDisconnect(&conn);
}

static void PrepareConsistentWrite(const char *queryString)
{
	uint64_t pos;
	uint64_t cur;
	int ret;
	char readQuery[4096];
	struct ZLogConn conn;
	Oid argTypes[1];
	Datum argValues[1];

	ZLogConnInit(&conn);
	ZLogConnect(&conn);

	ret = zlog_append(conn.log, (void*)queryString,
			strlen(queryString)+1, &pos);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("PrepareConsistentWrite:Append: %d", ret)));
		return;
	}

		SPI_connect();

	/*
	 * Execute preceeding queries.
	 *
	 * TODO:
	 *   - cur should start from db state
	 *   - handle errors like not-written and filled
	 *   - handle large query strings. is there psql limit?
	 */
	for (cur = 0; cur < pos; cur++) {
		ret = zlog_read(conn.log, cur, readQuery, sizeof(readQuery));
		if (ret < 0) {
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("PrepareConsistentWrite:Read: %d", ret)));
			return;
		}

		ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("MOG PrepareConsistentWrite:Read %llu/%s",
				(unsigned long long)cur, readQuery)));


		argTypes[0] = TEXTOID;
		argValues[0] = CStringGetTextDatum(readQuery);
		ret = SPI_execute_with_args("SELECT pgzlog_apply_update($1)",
				1, argTypes, argValues, NULL, false, 1);
		if (ret < 0) {
			ereport(WARNING, (errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("PrepareConsistentWrite:SPI_execute: %d", ret)));
		} else {
			ereport(WARNING, (errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("PrepareConsistentWrite:SPI_execute: %d", ret)));
		}
	}
		SPI_finish();
	CommandCounterIncrement();

	ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		errmsg("PrepareConsistentWrite: %s", queryString),
		errhint("pos: %llu", (unsigned long long)pos)));

	//CommandCounterIncrement();
	ZLogConnDisconnect(&conn);
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
				PrepareConsistentWrite(queryString);
			}
		}
		else if (statementType == T_IndexStmt)
		{
			IndexStmt *indexStatement = (IndexStmt *) parsetree;
			Oid tableOid = ExtractTableOid((Node *) indexStatement->relation);
			if (IsZLogTable(tableOid))
			{
				PrepareConsistentWrite(queryString);
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
					PrepareConsistentRead();
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
