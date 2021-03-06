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
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/tqual.h"

#include "pg_zlog.h"

#include <zlog/capi.h>

/* declarations for dynamic loading */
PG_MODULE_MAGIC;

/* whether writes go through zlog */
static bool ZLogEnabled = true;

/* whether to allow mutable functions */
static bool AllowMutableFunctions = false;

/* restore these hooks on unload */
static planner_hook_type PreviousPlannerHook = NULL;
static ExecutorStart_hook_type PreviousExecutorStartHook = NULL;
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

/*
 *
 */
PG_FUNCTION_INFO_V1(pgzlog_read_log);
Datum
pgzlog_read_log(PG_FUNCTION_ARGS)
{
	char readQuery[4096];
	const char *log_name;
	ZLogConn *conn;
	int64 pos;
	int ret;

	log_name = text_to_cstring(PG_GETARG_TEXT_P(0));
	pos = PG_GETARG_INT64(1);
	conn = GetConnection(log_name);

	MemSet(readQuery, 0, sizeof(readQuery));

	ret = zlog_read(conn->log, pos, readQuery, sizeof(readQuery)-1);
	if (ret < 0)
	{
		ereport(ERROR, (errmsg("pg_zlog cannot read log: %d", ret)));
	}

	PG_RETURN_TEXT_P(cstring_to_text(readQuery));
}

static void
ApplyLogEntries(const char *log_name, int64 tail)
{
	Oid argTypes[] = {TEXTOID, INT8OID};
	Datum argValues[] = {
		CStringGetTextDatum(log_name),
		Int64GetDatum(tail)
	};

	SPI_connect();

	SPI_execute_with_args("SELECT pgzlog_apply_log($1,$2)",
		2, argTypes, argValues, NULL, false, 1);

	SPI_finish();
}

/*
 * TODO:
 *   - support optimistic consistency mode (read-your-own-writes)
 */
static void PrepareConsistentRead(const char *logName)
{
	uint64_t tail;
	int ret;
	ZLogConn *conn;

	conn = GetConnection(logName);

	ret = zlog_checktail(conn->log, &tail);
	if (ret)
	{
		ereport(ERROR, (errmsg("pg_zlog could not check tail: %d", ret)));
	}

	ApplyLogEntries(logName, (int64)tail);
	CommandCounterIncrement();
}

static void PrepareConsistentWrite(const char *logName, const char *queryString)
{
	int ret;
	uint64_t appended_pos;
	ZLogConn *conn;

	conn = GetConnection(logName);

	ret = zlog_append(conn->log, (void*)queryString,
			strlen(queryString)+1, &appended_pos);
	if (ret)
	{
		ereport(ERROR, (errmsg("pg_zlog could not append to log: %d", ret)));
	}

	ApplyLogEntries(logName, (int64)appended_pos);
	CommandCounterIncrement();

	ZLogSetApplied(logName, (int64)appended_pos);
	CommandCounterIncrement();
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
PG_FUNCTION_INFO_V1(zlog_execute);
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
