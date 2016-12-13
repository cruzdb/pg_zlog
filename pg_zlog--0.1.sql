CREATE SCHEMA pgzlog_metadata

    CREATE TABLE "log" (
        name text not null,
        pool text not null,
        conf text default null,
        last_applied_pos bigint not null default -1,
        PRIMARY KEY (name)
    )

	CREATE TABLE "replicated_tables" (
		table_oid regclass not null,
        log_name text not null,
		PRIMARY KEY (table_oid),
        FOREIGN KEY (log_name) REFERENCES pgzlog_metadata.log (name)
	);

CREATE FUNCTION pgzlog_create_log(log_name text, pool_name text, conf_path text)
RETURNS void
AS $BODY$
BEGIN
    INSERT INTO pgzlog_metadata.log (name, pool, conf)
    VALUES (log_name, pool_name, conf_path);
END;
$BODY$ LANGUAGE plpgsql;

CREATE FUNCTION pgzlog_replicate_table(log_name text, table_oid regclass)
RETURNS void
AS $BODY$
BEGIN
	INSERT INTO pgzlog_metadata.replicated_tables (table_oid, log_name)
	VALUES (table_oid, log_name);
END;
$BODY$ LANGUAGE plpgsql;

CREATE FUNCTION pgzlog_apply_update(log_name text, query text, pos bigint)
RETURNS void
as $BODY$
BEGIN
	PERFORM pg_advisory_xact_lock(29030, hashtext(log_name));

	SET LOCAL pg_zlog.enabled TO false;

    RAISE DEBUG 'Executing: %', query;

    BEGIN
        EXECUTE query;
    EXCEPTION WHEN others THEN
    END;

    UPDATE pgzlog_metadata.log
    SET last_applied_pos = pos
    WHERE name = log_name;
END;
$BODY$ LANGUAGE 'plpgsql';
