#include "postgres.h"
#include "fmgr.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "access/xact.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tqual.h"

#include "pg_zlog.h"
#include <rados/librados.h>
#include <zlog/capi.h>

typedef struct ConnCacheEntry
{
	char log_name[NAMEDATALEN];
	ZLogConn conn;
} ConnCacheEntry;

static HTAB *ConnectionHash = NULL;

static void
ZLogConnInit(ZLogConn *conn)
{
	conn->cluster_ok = false;
	conn->ioctx_ok = false;
	conn->log_ok = false;
}

static void
ZLogConnShutdown(ZLogConn *conn)
{
	if (conn->log_ok)
	{
		zlog_destroy(conn->log);
		conn->log_ok = false;
	}

	if (conn->ioctx_ok)
	{
		rados_ioctx_destroy(conn->ioctx);
		conn->ioctx_ok = false;
	}

	if (conn->cluster_ok)
	{
		rados_shutdown(conn->cluster);
		conn->cluster_ok = false;
	}
}

static bool
ZLogConnOk(ZLogConn *conn)
{
	return conn->cluster_ok && conn->ioctx_ok && conn->log_ok;
}

/*
 * OpenClusterConnection creates a new connection to a Ceph cluster given the
 * configuration path.
 */
static void
OpenClusterConnection(ZLogConn *conn, const char *conf_path)
{
	int ret;

	ret = rados_create(&conn->cluster, NULL);
	if (ret < 0)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
			errmsg("could not create rados connection: %d", ret)));
	}

	conn->cluster_ok = true;

	ret = rados_conf_read_file(conn->cluster, conf_path);
	if (ret < 0)
	{
		// this is needed for error handling from ceph_cluster_fsid
		// and ceph_pool_id. all other error handling is handled in a
		// end-of-xact hook through the conn caching logic.
		ZLogConnShutdown(conn);
		ereport(ERROR, (
			errcode(ERRCODE_CONNECTION_FAILURE),
			errmsg("rados_conf_read_file failed with retval %d", ret),
			errdetail("conf path %s", conf_path)));
	}

	ret = rados_connect(conn->cluster);
	if (ret < 0)
	{
		// this is needed for error handling from ceph_cluster_fsid
		// and ceph_pool_id. all other error handling is handled in a
		// end-of-xact hook through the conn caching logic.
		ZLogConnShutdown(conn);
		ereport(ERROR, (
			errcode(ERRCODE_CONNECTION_FAILURE),
			errmsg("rados_connect failed with retval %d", ret),
			errdetail("conf path %s", conf_path)));
	}
}

/*
 * OpenPoolConnection creates a connection to a RADOS pool.
 */
static void
OpenPoolConnection(ZLogConn *conn, const char *pool_name)
{
	int ret = rados_ioctx_create(conn->cluster, pool_name, &conn->ioctx);
	if (ret)
	{
		// this is needed for error handling from ceph_cluster_fsid
		// and ceph_pool_id. all other error handling is handled in a
		// end-of-xact hook through the conn caching logic.
		ZLogConnShutdown(conn);
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
			errmsg("pg_zlog could not open ioctx \"%s\": %d",
				pool_name, ret)));
	}

	conn->ioctx_ok = true;
}


/*
 * OpenLogConnection establishes a connection to zlog.
 */
static void
OpenLogConnection(ZLogConn *conn, const char *log_name,
		const char *host, const char *port)
{
	int ret = zlog_open_or_create(conn->ioctx,
			log_name, host, port, &conn->log);
	if (ret)
	{
		ereport(ERROR, (
			errcode(ERRCODE_CONNECTION_FAILURE),
			errmsg("pg_zlog could not open log: %d", ret)));
	}

	conn->log_ok = true;
}

/*
 * GetClusterConnection opens a connection to the Ceph cluster identified by
 * cluster_name in pgzlog_metadata.cluster.
 */
static void
GetClusterConnection(ZLogConn *conn, const char *cluster_name)
{
	RangeVar *heapRangeVar;
	Relation heapRelation;
	ScanKeyData scanKey[1];
	HeapScanDesc scanDesc;
	HeapTuple heapTuple;

	// open the cluster relation
	heapRangeVar = makeRangeVar(PG_ZLOG_METADATA_SCHEMA_NAME,
			PG_ZLOG_CLUSTER_TABLE_NAME, -1);
	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);

	// scan for the name of the cluster
	ScanKeyInit(&scanKey[0], 1, InvalidStrategy, F_TEXTEQ,
			CStringGetTextDatum(cluster_name));
	scanDesc = heap_beginscan(heapRelation, SnapshotSelf, 1, scanKey);

	// check result
	heapTuple = heap_getnext(scanDesc, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		int ret;
		bool isNull;
		char fsid_buf[38];
		char *cluster_fsid;
		char *conf_path = NULL;
		TupleDesc tupleDesc;
		Datum fsid;
		Datum conf;

		tupleDesc = RelationGetDescr(heapRelation);

		fsid = heap_getattr(heapTuple, 2, tupleDesc, &isNull);
		cluster_fsid = TextDatumGetCString(fsid);

		conf = heap_getattr(heapTuple, 3, tupleDesc, &isNull);
		if (!isNull)
			conf_path = TextDatumGetCString(conf);

		OpenClusterConnection(conn, conf_path);

		// verify stored fsid matches the cluster fsid
		ret = rados_cluster_fsid(conn->cluster, fsid_buf, 37);
		if (ret < 0)
		{
			ereport(ERROR, (
				errcode(ERRCODE_CONNECTION_FAILURE),
				errmsg("rados_cluster_fsid failed with retval %d", ret),
				errdetail("conf path %s", conf_path)));
		}

		if (strncmp(cluster_fsid, fsid_buf, 37) != 0)
		{
			ereport(ERROR, (
				errcode(ERRCODE_ASSERT_FAILURE),
				errmsg("cluster fsid does not match"),
				errdetail("expected \"%s\" got \"%s\"", cluster_fsid, fsid_buf)));
		}
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
				errmsg("cluster not found")));
	}

	heap_endscan(scanDesc);
	relation_close(heapRelation, AccessShareLock);
}


static void
GetPoolConnection(ZLogConn *conn, const char *pool_name)
{
	RangeVar *heapRangeVar;
	Relation heapRelation;
	ScanKeyData scanKey[1];
	HeapScanDesc scanDesc;
	HeapTuple heapTuple;

	// open the cluster relation
	heapRangeVar = makeRangeVar(PG_ZLOG_METADATA_SCHEMA_NAME,
			PG_ZLOG_POOL_TABLE_NAME, -1);
	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);

	// scan for the name of the cluster
	ScanKeyInit(&scanKey[0], 1, InvalidStrategy, F_TEXTEQ,
			CStringGetTextDatum(pool_name));
	scanDesc = heap_beginscan(heapRelation, SnapshotSelf, 1, scanKey);

	// check result
	heapTuple = heap_getnext(scanDesc, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		bool isNull;
		TupleDesc tupleDesc;
		char *cluster_name;
		Datum cluster_datum;

		tupleDesc = RelationGetDescr(heapRelation);

		cluster_datum = heap_getattr(heapTuple, 3, tupleDesc, &isNull);
		cluster_name = TextDatumGetCString(cluster_datum);
		GetClusterConnection(conn, cluster_name);

		OpenPoolConnection(conn, pool_name);
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
				errmsg("pool not found")));
	}

	heap_endscan(scanDesc);
	relation_close(heapRelation, AccessShareLock);
}

/*
 * GetLogConnection establishes a connection to a log, and also takes care of
 * connecting to the target cluster and storage pool.
 */
static void
GetLogConnection(ZLogConn *conn, const char *log_name)
{
	RangeVar *heapRangeVar;
	Relation heapRelation;
	ScanKeyData scanKey[1];
	HeapScanDesc scanDesc;
	HeapTuple heapTuple;

	// open the cluster relation
	heapRangeVar = makeRangeVar(PG_ZLOG_METADATA_SCHEMA_NAME,
			PG_ZLOG_LOG_TABLE_NAME, -1);
	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);

	// scan for the name of the cluster
	ScanKeyInit(&scanKey[0], 1, InvalidStrategy, F_TEXTEQ,
			CStringGetTextDatum(log_name));
	scanDesc = heap_beginscan(heapRelation, SnapshotSelf, 1, scanKey);

	// check result
	heapTuple = heap_getnext(scanDesc, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		bool isNull;
		TupleDesc tupleDesc;
		char *pool_name;
		char *host_str = ZLOG_DEFAULT_HOST;
		char *port_str = ZLOG_DEFAULT_PORT;
		Datum pool;
		Datum host;
		Datum port;

		tupleDesc = RelationGetDescr(heapRelation);

		pool = heap_getattr(heapTuple, 2, tupleDesc, &isNull);
		pool_name = TextDatumGetCString(pool);
		GetPoolConnection(conn, pool_name);

		host = heap_getattr(heapTuple, 3, tupleDesc, &isNull);
		if (!isNull)
			host_str = TextDatumGetCString(host);

		port = heap_getattr(heapTuple, 4, tupleDesc, &isNull);
		if (!isNull)
			port_str = TextDatumGetCString(port);

		OpenLogConnection(conn, log_name, host_str, port_str);
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
				errmsg("log not found")));
	}

	heap_endscan(scanDesc);
	relation_close(heapRelation, AccessShareLock);
}

/*
 * Remove any connections that are not in a good state.
 */
static void
pgzlog_xact_callback(XactEvent event, void *arg)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		if (!ZLogConnOk(&entry->conn))
			ZLogConnShutdown(&entry->conn);
	}
}

ZLogConn *
GetConnection(const char *log_name)
{
	bool found;
	ConnCacheEntry *entry;

	if (ConnectionHash == NULL)
	{
		HASHCTL ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = NAMEDATALEN;
		ctl.entrysize = sizeof(ConnCacheEntry);
		ctl.hcxt = CacheMemoryContext;

		ConnectionHash = hash_create("pg_zlog connections", 8, &ctl,
			HASH_ELEM | HASH_CONTEXT);

		RegisterXactCallback(pgzlog_xact_callback, NULL);
	}

	entry = hash_search(ConnectionHash, &log_name, HASH_ENTER, &found);
	if (!found || !ZLogConnOk(&entry->conn))
	{
		ZLogConnInit(&entry->conn);
		GetLogConnection(&entry->conn, log_name);
	}

	return &entry->conn;
}

/*
 * ceph_pool_id returns the identification number of a rados pool.
 *
 * TODO:
 *   this code is largely identical to GetClusterConnection, but is repeated
 *   here to avoid a try/catch block wrapping GetClusterConnection. That may
 *   be OK to do, but there were some open questions about volatile-ness with
 *   stack variables in the recovery code. If we end up expanding a lot of
 *   this connection code we should re-vist this to avoid more duplication.
 */
PG_FUNCTION_INFO_V1(ceph_pool_id);
Datum
ceph_pool_id(PG_FUNCTION_ARGS)
{
	ZLogConn conn;
	RangeVar *heapRangeVar;
	Relation heapRelation;
	ScanKeyData scanKey[1];
	HeapScanDesc scanDesc;
	HeapTuple heapTuple;
	char *cluster_name;
	char *pool_name;
	int64_t pool_id;

	ZLogConnInit(&conn);

	cluster_name = text_to_cstring(PG_GETARG_TEXT_P(0));
	pool_name = text_to_cstring(PG_GETARG_TEXT_P(1));

	// open the cluster relation
	heapRangeVar = makeRangeVar(PG_ZLOG_METADATA_SCHEMA_NAME,
			PG_ZLOG_CLUSTER_TABLE_NAME, -1);
	heapRelation = relation_openrv(heapRangeVar, AccessShareLock);

	// scan for the name of the cluster
	ScanKeyInit(&scanKey[0], 1, InvalidStrategy, F_TEXTEQ,
			CStringGetTextDatum(cluster_name));
	scanDesc = heap_beginscan(heapRelation, SnapshotSelf, 1, scanKey);

	// check result
	heapTuple = heap_getnext(scanDesc, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		int ret;
		bool isNull;
		char fsid_buf[38];
		char *cluster_fsid;
		char *conf_path = NULL;
		TupleDesc tupleDesc;
		Datum fsid;
		Datum conf;

		tupleDesc = RelationGetDescr(heapRelation);

		fsid = heap_getattr(heapTuple, 2, tupleDesc, &isNull);
		cluster_fsid = TextDatumGetCString(fsid);

		conf = heap_getattr(heapTuple, 3, tupleDesc, &isNull);
		if (!isNull)
			conf_path = TextDatumGetCString(conf);

		OpenClusterConnection(&conn, conf_path);

		// verify stored fsid matches the cluster fsid
		ret = rados_cluster_fsid(conn.cluster, fsid_buf, 37);
		if (ret < 0)
		{
			ZLogConnShutdown(&conn);
			ereport(ERROR, (
				errcode(ERRCODE_CONNECTION_FAILURE),
				errmsg("rados_cluster_fsid failed with retval %d", ret),
				errdetail("conf path %s", conf_path)));
		}

		if (strncmp(cluster_fsid, fsid_buf, 37) != 0)
		{
			ZLogConnShutdown(&conn);
			ereport(ERROR, (
				errcode(ERRCODE_ASSERT_FAILURE),
				errmsg("cluster fsid does not match"),
				errdetail("expected \"%s\" got \"%s\"", cluster_fsid, fsid_buf)));
		}

		OpenPoolConnection(&conn, pool_name);
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
				errmsg("cluster not found")));
	}

	pool_id = rados_ioctx_get_id(conn.ioctx);

	ZLogConnShutdown(&conn);

	heap_endscan(scanDesc);
	relation_close(heapRelation, AccessShareLock);

	PG_RETURN_INT64(pool_id);
}

/*
 * ceph_cluster_fsid returns the fsid of a ceph cluster.
 */
PG_FUNCTION_INFO_V1(ceph_cluster_fsid);
Datum
ceph_cluster_fsid(PG_FUNCTION_ARGS)
{
	int ret;
	ZLogConn conn;
	char *conf_path;
	char fsid_buf[38];

	if (PG_ARGISNULL(0))
		conf_path = NULL;
	else
		conf_path = text_to_cstring(PG_GETARG_TEXT_P(0));

	ZLogConnInit(&conn);

	OpenClusterConnection(&conn, conf_path);

	ret = rados_cluster_fsid(conn.cluster, fsid_buf, 37);
	if (ret < 0)
	{
		ZLogConnShutdown(&conn);
		ereport(ERROR, (
			errcode(ERRCODE_CONNECTION_FAILURE),
			errmsg("rados_cluster_fsid failed with retval %d", ret),
			errdetail("config file path %s", conf_path)));
	}

	ZLogConnShutdown(&conn);

	PG_RETURN_TEXT_P(cstring_to_text(fsid_buf));
}
