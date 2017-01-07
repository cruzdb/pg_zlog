#include "postgres.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#include "pg_zlog.h"
#include <rados/librados.h>
#include <zlog/capi.h>

typedef int ConnCacheKey;

typedef struct ConnCacheEntry
{
	ConnCacheKey key;
	ZLogConn *conn;
} ConnCacheEntry;

static HTAB *ConnectionHash = NULL;

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

static ZLogConn *
OpenConnection(const char *name, const char *conf)
{
	ZLogConn *volatile conn = NULL;

	PG_TRY();
	{
		int ret;

		conn = MemoryContextAlloc(CacheMemoryContext, sizeof(*conn));

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

ZLogConn *
GetConnection(const char *name, const char *conf)
{
	bool found;
	ConnCacheEntry *entry;
	ConnCacheKey key;

	if (ConnectionHash == NULL)
	{
		HASHCTL ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ConnCacheKey);
		ctl.entrysize = sizeof(ConnCacheEntry);
		ctl.hcxt = CacheMemoryContext;

		ConnectionHash = hash_create("pg_zlog connections", 8, &ctl,
			HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		entry->conn = NULL;
	}

	if (entry->conn == NULL)
	{
		entry->conn = OpenConnection(name, conf);
	}

	return entry->conn;
}
