CREATE SCHEMA pgzlog_metadata

    /* ceph cluster connections */
    CREATE TABLE "cluster" (
        name text not null,
        fsid text not null,
        conf_path text default null,
        PRIMARY KEY (name)
    )

    /* storage pools */
    CREATE TABLE "pool" (
        name text not null,
        id bigint not null,
        cluster text not null,
        PRIMARY KEY (name),
        FOREIGN KEY (cluster) REFERENCES pgzlog_metadata.cluster (name)
    )

    /* zlog instances */
    CREATE TABLE "log" (
        name text not null,
        pool text not null,
        host text default null,
        port text default null,
        last_applied_pos bigint not null default -1,
        PRIMARY KEY (name),
        FOREIGN KEY (pool) REFERENCES pgzlog_metadata.pool (name)
    )

    /* tables that are replicated on logs */
	CREATE TABLE "replicated_tables" (
		table_oid regclass not null,
        log text not null,
		PRIMARY KEY (table_oid),
        FOREIGN KEY (log) REFERENCES pgzlog_metadata.log (name)
	);

/*
 * Register a cluster.
 */
CREATE FUNCTION pgzlog_add_cluster(name text, conf_path text)
RETURNS void
AS $BODY$
DECLARE
    fsid text;
BEGIN
    SELECT * FROM ceph_cluster_fsid(conf_path) INTO fsid;
    INSERT INTO pgzlog_metadata.cluster (name, fsid, conf_path)
        VALUES (name, fsid, conf_path);
END;
$BODY$ LANGUAGE 'plpgsql';

/*
 * Register a storage pool.
 */
CREATE FUNCTION pgzlog_add_pool(cluster_name text, pool_name text)
RETURNS void
AS $BODY$
DECLARE
    pool_id bigint;
BEGIN
    SELECT * FROM ceph_pool_id(cluster_name, pool_name) INTO pool_id;
    INSERT INTO pgzlog_metadata.pool (name, id, cluster)
    VALUES (pool_name, pool_id, cluster_name);
END;
$BODY$ LANGUAGE 'plpgsql';

/*
 * Register a zlog instance.
 */
CREATE FUNCTION pgzlog_add_log(pool_name text, log_name text, host text, port text)
RETURNS void
AS $BODY$
BEGIN
    INSERT INTO pgzlog_metadata.log (name, pool, host, port)
    VALUES (log_name, pool_name, host, port);
END;
$BODY$ LANGUAGE 'plpgsql';

/*
 * Register a table for replication on a log.
 */
CREATE FUNCTION pgzlog_replicate_table(log_name text, table_oid regclass)
RETURNS void
AS $BODY$
BEGIN
	INSERT INTO pgzlog_metadata.replicated_tables (table_oid, log)
	VALUES (table_oid, log_name);
END;
$BODY$ LANGUAGE 'plpgsql';

/*
 * Retrieve the unique identifer of a cluster.
 */
CREATE FUNCTION ceph_cluster_fsid(conf_path text)
RETURNS text
LANGUAGE C
AS 'MODULE_PATHNAME', $$ceph_cluster_fsid$$;

/*
 * Retrieve the identification number of the named pool.
 */
CREATE FUNCTION ceph_pool_id(cluster_name text, pool_name text)
RETURNS bigint
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$ceph_pool_id$$;

/*
 * zlog_execute executes a query locally, by-passing the query logging logic.
 */
CREATE FUNCTION zlog_execute(INTERNAL)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', $$zlog_execute$$;

/*
 *
 */
CREATE FUNCTION pgzlog_apply_log(log_name text, tail_log_pos bigint)
RETURNS void
AS $BODY$
DECLARE
    current_log_pos bigint;
    query text;
BEGIN
    PERFORM pg_advisory_xact_lock(29030, hashtext(log_name));
    SET LOCAL pg_zlog.enabled TO false;

    SELECT last_applied_pos INTO current_log_pos
    FROM pgzlog_metadata.log
    WHERE name = log_name;

    current_log_pos := current_log_pos + 1;

    ASSERT current_log_pos <= tail_log_pos;

    WHILE current_log_pos < tail_log_pos LOOP

        SELECT * FROM pgzlog_read_log(log_name, current_log_pos) INTO query;

        RAISE DEBUG 'Executing: %', query;

        BEGIN
            EXECUTE query;
            EXCEPTION WHEN others THEN
        END;

        UPDATE pgzlog_metadata.log
        SET last_applied_pos = current_log_pos
        WHERE name = log_name;

        current_log_pos := current_log_pos + 1;

    END LOOP;
END;
$BODY$ LANGUAGE 'plpgsql';

/*
 *
 */
CREATE FUNCTION pgzlog_read_log(log_name text, pos bigint)
RETURNS text
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$pgzlog_read_log$$;
