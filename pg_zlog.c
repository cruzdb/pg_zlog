#include "postgres.h"
#include "fmgr.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "parser/parse_func.h"
#include "storage/lockdefs.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/tqual.h"

#include "pg_zlog.h"

#include <rados/librados.h>
#include <zlog/capi.h>

#define PG_ZLOG_EXTENSION_NAME "pg_zlog"
#define PG_ZLOG_METADATA_SCHEMA_NAME "pgzlog_metadata"
#define PG_ZLOG_REPLICATED_TABLE_NAME "replicated_tables"
#define ATTR_NUM_REPLICATED_TABLES_RELATION_ID 1
#define ATTR_NUM_REPLICATED_TABLES_LOG 2
#define MAX_ZLOG_LOG_NAME_LENGTH 128

/* declarations for dynamic loading */
PG_MODULE_MAGIC;

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(zlog_execute);

/* whether writes go through zlog */
static bool ZLogEnabled = true;

/* whether to allow mutable functions */
static bool AllowMutableFunctions = false;

/* restore these hooks on unload */
static planner_hook_type PreviousPlannerHook = NULL;
static ExecutorStart_hook_type PreviousExecutorStartHook = NULL;
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

	SPI_connect();

	/*
	 * Grab lock and disable Paxos. This could also be pushed down into
	 * pgzlog_apply_update, it just needs to be done at least once.
	 */
	{
		int ret;
		Datum argValues[] = {CStringGetTextDatum(logName)};
		Oid argTypes[] = {TEXTOID};

		ret = SPI_execute_with_args("SELECT pgzlog_start_update($1)",
				1, argTypes, argValues, NULL, false, 0);
		if (ret < 0)
		{
			ereport(ERROR, (errmsg("pg_zlog cannot prepare update: %d", ret)));
		}

		CommandCounterIncrement();
	}

	if (start > end)
	{
		SPI_finish();
		ereport(ERROR, (errmsg("pg_zlog unexpected from/to "
			INT64_FORMAT "/" INT64_FORMAT, start, end)));
	}
	else if (start == end)
	{
		SPI_finish();
		return;
	}

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
	CommandCounterIncrement();

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
	CommandCounterIncrement();

	ZLogSetApplied(logName, (int64)appended_pos);
	CommandCounterIncrement();

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

/*
 * ErrorIfQueryNotSupported checks if the query contains unsupported features,
 * and errors out if it does.
 */
static void
ErrorIfQueryNotSupported(Query *queryTree)
{
	if (!AllowMutableFunctions && contain_mutable_functions((Node *) queryTree))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("queries containing mutable functions "
				"may load to inconsistencies"),
			errhint("To allow mutable functions, set "
				"pg_zlog.allow_mutable_functions to on")));
	}
}

/*
 * FidModificationQuery walks a query tree to find a modification query.
 */
static bool
FindModificationQueryWalker(Node *node, bool *isModificationQuery)
{
	bool walkIsComplete;

	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;
		CmdType commandType = query->commandType;

		if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
			commandType == CMD_DELETE)
		{
			*isModificationQuery = true;
			walkIsComplete = true;
		}
		else
		{
			walkIsComplete = query_tree_walker(query,
				FindModificationQueryWalker, isModificationQuery, 0);
		}
	}
	else
	{
		walkIsComplete = expression_tree_walker(node,
				FindModificationQueryWalker, isModificationQuery);
	}

	return walkIsComplete;
}

/*
 * ExtractRangeTableEntryWalker walks over a query tree, and finds all range
 * table entries. For recursing into the query tree, this function uses the
 * query tree walker since the expression tree walker doesn't recurse into
 * sub-queries.
 */
static bool
ExtractRangeTableEntryWalker(Node *node, List **rangeTableList)
{
	bool walkIsComplete = false;

	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rangeTable = (RangeTblEntry *) node;

		(*rangeTableList) = lappend(*rangeTableList, rangeTable);
	}
	else if (IsA(node, Query))
	{
		walkIsComplete = query_tree_walker((Query *) node,
			ExtractRangeTableEntryWalker, rangeTableList, QTW_EXAMINE_RTES);
	}
	else
	{
		walkIsComplete = expression_tree_walker(node,
			ExtractRangeTableEntryWalker, rangeTableList);
	}

	return walkIsComplete;
}

/*
 * ZLogExecuteFuncId returns the OID of the zlog_execute function.
 */
static Oid
ZLogExecuteFuncId(void)
{
	static Oid cachedOid = 0;
	List *nameList = NIL;
	Oid paramOids[1] = { INTERNALOID };

	if (cachedOid == InvalidOid)
	{
		nameList = list_make2(makeString("public"),
			makeString("zlog_execute"));
		cachedOid = LookupFuncName(nameList, 1, paramOids, false);
	}

	return cachedOid;
}

/*
 * CreateZLogExecutePlan creates a plan to call zlog_execute(queryString),
 * which is intercepted by the executor hook, which logs the query and executes
 * it using the regular planner.
 */
static Plan *
CreateZLogExecutePlan(char *queryString)
{
	FunctionScan *execFunctionScan;
	RangeTblFunction *execFunction;
	FuncExpr *execFuncExpr;
	Const *queryData;

	/* store the query string as a cstring */
	queryData = makeNode(Const);
	queryData->consttype = CSTRINGOID;
	queryData->constlen = -2;
	queryData->constvalue = CStringGetDatum(queryString);
	queryData->constbyval = false;
	queryData->constisnull = queryString == NULL;
	queryData->location = -1;

	execFuncExpr = makeNode(FuncExpr);
	execFuncExpr->funcid = ZLogExecuteFuncId();
	execFuncExpr->funcretset = true;
	execFuncExpr->funcresulttype = VOIDOID;
	execFuncExpr->location = -1;
	execFuncExpr->args = list_make1(queryData);

	execFunction = makeNode(RangeTblFunction);
	execFunction->funcexpr = (Node *) execFuncExpr;

	execFunctionScan = makeNode(FunctionScan);
	execFunctionScan->functions = lappend(execFunctionScan->functions, execFunction);

	return (Plan *) execFunctionScan;
}

static PlannedStmt *
NextPlannerHook(Query *query, int cursorOptions, ParamListInfo boundParams)
{
	if (PreviousPlannerHook != NULL)
	{
		return PreviousPlannerHook(query, cursorOptions, boundParams);
	}
	else
	{
		return standard_planner(query, cursorOptions, boundParams);
	}
}

static PlannedStmt *
PgZLogPlanner(Query *query, int cursorOptions, ParamListInfo boundParams)
{
	bool isModificationQuery;
	List *rangeTableList = NIL;

	if (!IsPgZLogActive())
	{
		return NextPlannerHook(query, cursorOptions, boundParams);
	}

	FindModificationQueryWalker((Node *) query, &isModificationQuery);

	if (!isModificationQuery)
	{
		return NextPlannerHook(query, cursorOptions, boundParams);
	}

	ExtractRangeTableEntryWalker((Node *) query, &rangeTableList);

	if (HasZLogTable(rangeTableList))
	{
		PlannedStmt *plannedStatement;
		Plan *paxosExecutePlan;

		/* Build the DML query to log */
		Query *paxosQuery = copyObject(query);
		StringInfo queryString = makeStringInfo();

		/* call standard planner first to have Query transformations performed */
		plannedStatement = standard_planner(paxosQuery, cursorOptions, boundParams);

		ErrorIfQueryNotSupported(paxosQuery);

		/* get the transformed query string */
		deparse_query(paxosQuery, queryString);

		/* define the plan as a call to paxos_execute(queryString) */
		paxosExecutePlan = CreateZLogExecutePlan(queryString->data);

		/* PortalStart will use the targetlist from the plan */
		paxosExecutePlan->targetlist = plannedStatement->planTree->targetlist;

		plannedStatement->planTree = paxosExecutePlan;

		return plannedStatement;
	}
	else
	{
		return NextPlannerHook(query, cursorOptions, boundParams);
	}
}

/*
 * GetZLogQueryString returns NULL if the plan is not a zlog execute plan,
 * or the original query string.
 */
static char *
GetZLogQueryString(PlannedStmt *plan)
{
	FunctionScan *execFunctionScan;
	RangeTblFunction *execFunction;
	FuncExpr *execFuncExpr;
	Const *queryData;
	char *queryString;

	if (!IsA(plan->planTree, FunctionScan))
	{
		return NULL;
	}

	execFunctionScan = (FunctionScan *) plan->planTree;

	if (list_length(execFunctionScan->functions) != 1)
	{
		return NULL;
	}

	execFunction = linitial(execFunctionScan->functions);

	if (!IsA(execFunction->funcexpr, FuncExpr))
	{
		return NULL;
	}

	execFuncExpr = (FuncExpr *) execFunction->funcexpr;

	if (execFuncExpr->funcid != ZLogExecuteFuncId())
	{
		return NULL;
	}

	if (list_length(execFuncExpr->args) != 1)
	{
		ereport(ERROR, (errmsg("unexpected number of function "
				"arguments to zlog_execute")));
	}

	queryData = (Const *) linitial(execFuncExpr->args);
	Assert(IsA(queryData, Const));
	Assert(queryData->consttype == CSTRINGOID);

	queryString = DatumGetCString(queryData->constvalue);

	return queryString;
}

static void
NextExecutorStartHook(QueryDesc *queryDesc, int eflags)
{
	/* call into the standard executor start, or hook if set */
	if (PreviousExecutorStartHook != NULL)
	{
		PreviousExecutorStartHook(queryDesc, eflags);
	}
	else
	{
		standard_ExecutorStart(queryDesc, eflags);
	}
}

static void
PgZLogExecutorStart(QueryDesc *queryDesc, int eflags)
{
	PlannedStmt *plannedStmt = queryDesc->plannedstmt;
	List *rangeTableList = plannedStmt->rtable;
	char *paxosQueryString;

	if (!IsPgZLogActive() || (eflags & EXEC_FLAG_EXPLAIN_ONLY) != 0)
	{
		NextExecutorStartHook(queryDesc, eflags);
		return;
	}

	paxosQueryString = GetZLogQueryString(plannedStmt);
	if (paxosQueryString != NULL)
	{
		/* paxos write query */
		char *logName;
		const bool isTopLevel = true;
		List *parseTreeList;
		Node *parseTreeNode;
		List *queryTreeList;
		Query *query;

		/* disallow transactions during paxos commands */
		PreventTransactionChain(isTopLevel, "zlog commands");

		logName = GetLogName(rangeTableList);
		PrepareConsistentWrite(logName, paxosQueryString);

		queryDesc->snapshot->curcid = GetCurrentCommandId(false);

		elog(DEBUG1, "Executing: %s %d", paxosQueryString, plannedStmt->hasReturning);

		/* replan the query */
		parseTreeList = pg_parse_query(paxosQueryString);

		if (list_length(parseTreeList) != 1)
		{
			ereport(ERROR, (errmsg("can only execute single-statement queries on "
								   "replicated tables")));
		}

		parseTreeNode = (Node *) linitial(parseTreeList);
		queryTreeList = pg_analyze_and_rewrite(parseTreeNode, paxosQueryString, NULL, 0);
		query = (Query *) linitial(queryTreeList);
		queryDesc->plannedstmt = pg_plan_query(query, 0, queryDesc->params);
	}
	else if (HasZLogTable(rangeTableList))
	{
		/* paxos read query */
		char *logName;

		logName = GetLogName(rangeTableList);
		PrepareConsistentRead(logName);

		queryDesc->snapshot->curcid = GetCurrentCommandId(false);
	}

	NextExecutorStartHook(queryDesc, eflags);
}

/*
 * zlog_execute is a placeholder function to store a query string in
 * in plain postgres node trees.
 */
Datum
zlog_execute(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

void _PG_init(void);
void _PG_init(void)
{
	DefineCustomBoolVariable("pg_zlog.enabled",
		"If enabled, pg_zlog handles queries on zlog tables",
		NULL, &ZLogEnabled, true, PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_zlog.allow_mutable_functions",
		"If enabled, mutable functions in queries are allowed",
		NULL, &AllowMutableFunctions, false, PGC_USERSET,
		0, NULL, NULL, NULL);

	PreviousPlannerHook = planner_hook;
	planner_hook = PgZLogPlanner;

	PreviousExecutorStartHook = ExecutorStart_hook;
	ExecutorStart_hook = PgZLogExecutorStart;

	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = PgZLogProcessUtility;
}

void _PG_fini(void);
void _PG_fini(void)
{
	ProcessUtility_hook = PreviousProcessUtilityHook;
	ExecutorStart_hook = PreviousExecutorStartHook;
	planner_hook = PreviousPlannerHook;
}
