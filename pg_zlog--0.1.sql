CREATE SCHEMA pgzlog_metadata

	CREATE TABLE "replicated_tables" (
		table_oid regclass not null,
		PRIMARY KEY (table_oid)
	);

CREATE FUNCTION pgzlog_replicate_table(new_table_oid regclass)
RETURNS void
AS $BODY$
BEGIN
	INSERT INTO pgzlog_metadata.replicated_tables (table_oid)
	VALUES (new_table_oid);
END;
$BODY$ LANGUAGE plpgsql;

CREATE FUNCTION pgzlog_apply_update(query text)
RETURNS void
as $BODY$
BEGIN
	PERFORM pg_advisory_xact_lock(29030, hashtext('mylog'));

	SET LOCAL pg_zlog.enabled TO false;

    RAISE DEBUG 'Executing: %', query;

    BEGIN
        EXECUTE query;
    EXCEPTION WHEN others THEN
    END;
END;
$BODY$ LANGUAGE 'plpgsql';
