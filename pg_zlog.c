#include "postgres.h"
#include "fmgr.h"
#include "access/stratnum.h"
#include "access/skey.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/extension.h"
#include "nodes/makefuncs.h"
#include "storage/lockdefs.h"
#include "tcop/utility.h"
#include "utils/relcache.h"
#include "utils/tqual.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"

#define PG_ZLOG_EXTENSION_NAME "pg_zlog"
#define PG_ZLOG_METADATA_SCHEMA_NAME "pgzlog_metadata"
#define PG_ZLOG_REPLICATED_TABLE_NAME "replicated_tables"
#define ATTR_NUM_REPLICATED_TABLES_RELATION_ID 1

PG_MODULE_MAGIC;

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

	if (!IsTransactionState())
		return 0;

	oid = get_extension_oid(PG_ZLOG_EXTENSION_NAME, true);
	if (oid == InvalidOid)
		return false;

	namespace_oid = get_namespace_oid(PG_ZLOG_METADATA_SCHEMA_NAME, true);
	if (namespace_oid == InvalidOid)
		return false;

	oid = get_relname_relid(PG_ZLOG_REPLICATED_TABLE_NAME, namespace_oid);
	if (oid == InvalidOid)
		return false;

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
				//char *groupId = DeterminePaxosGroup(relations);
				//PrepareConsistentWrite(groupId, queryString);
				ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("zlog truncate"),
					errhint("%s", queryString)));
			}
		}
		else if (statementType == T_IndexStmt)
		{
			IndexStmt *indexStatement = (IndexStmt *) parsetree;
			Oid tableOid = ExtractTableOid((Node *) indexStatement->relation);
			if (IsZLogTable(tableOid))
			{
				//char *groupId = PaxosTableGroup(tableOid);
				//PrepareConsistentWrite(groupId, queryString);
				ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("zlog index stmt"),
					errhint("%s", queryString)));
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
									errmsg("COPY commands on zlog tables "
										   "are unsupported")));
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
					//char *groupId = DeterminePaxosGroup(parsedQuery->rtable);
					//PrepareConsistentRead(groupId);
					ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("zlog rawquery copy stmt"),
								errhint("%s", queryString)));
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
	ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		errmsg("hello"), errhint("world")));

	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = PgZLogProcessUtility;
}

void _PG_fini(void);
void _PG_fini(void)
{
}
