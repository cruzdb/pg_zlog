#include "postgres.h"

#include "pg_zlog.h"
#include <rados/librados.h>
#include <zlog/capi.h>

ZLogConn *GetConnection(const char *name, const char *conf)
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

void PutConnection(ZLogConn *conn)
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

