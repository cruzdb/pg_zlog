# pg_zlog - replicated PostgreSQL tables in Ceph

The `pg_zlog` extension provides logical table replication for PostgreSQL.
Replication is implemented by logging table mutations to a consistent
shared-log called [ZLog](https://github.com/noahdesu/zlog) that runs on the
[Ceph](https://github.com/ceph/ceph) distributed storage system. Strong
consistency is provided by rolling the log forward on any PostgreSQL node
before executing a query on a replicated table.

This extension is based on [pg_paxos](https://github.com/citusdata/pg_paxos)
that uses PL/pgSQL to implement a Paxos-based shared-log for recording table
mutations. The primary difference between `pg_zlog` and `pg_paxos` is that
`pg_zlog` decouples the log implementation from PostgreSQL, allowing it to
support the high-performance [ZLog](https://github.com/noahdesu/zlog) log
implementation, removing the overheads associated with maintaining a quorum of
PostgreSQL nodes, and supporting flexible storage options like replication and
erasure-coding that are provided by [Ceph](https://github.com/ceph/ceph).

# configuration

## add a cluster

Register a Ceph cluster with `pg_zlog` using the `pgzlog_add_cluster` function
by specifying a name (of your choosing) and a path to a Ceph configuration
file. Use `null` for the configuration file parameter to search default paths.

```sql
SELECT pgzlog_add_cluster('ceph', '/etc/ceph/ceph.conf');
/* OR */
SELECT pgzlog_add_cluster('ceph', null);
```

The set of registered clusters is stored in `pgzlog_metadata.cluster`:

```bash
postgres=# SELECT * from pgzlog_metadata.cluster;
 name  |                 fsid                 |     conf_path
-------+--------------------------------------+---------------------
 ceph  | b91e2af8-3d94-4fef-a314-1209cde6ea68 | /etc/ceph/ceph.conf
```

The `fsid` value is a unique cluster identifier that is used as a safety
mechanism to verify that connections are made to the correct cluster. It is
retrieved when `pgzlog_add_cluster` is called by establishing an initial
connection to the cluster. If a connection were made by `pg_zlog` to an
unexpected cluster than an error would be thrown, and manual intervention
would be required (e.g. verify and update the fsid, or the configuration
path).

## add a pool

Register a storage pool with `pg_zlog` using the `pgzlog_add_pool` function
by specifying the name of the pool in the Ceph cluster. Use the name of a
cluster already registered with `pgzlog_add_cluster`.

```sql
SELECT pgzlog_add_pool('ceph', 'pg_pool');
```

The set of registered storage pools is stored in `pgzlog_metadata.pool`:

```bash
postgres=# SELECT * from pgzlog_metadata.pool;
   name  | id | cluster 
---------+----+---------
 pg_pool |  0 | ceph
```

Like `fsid` above, the `id` field is a unique identifer for the pool that is
filled in automatically and checked each time a new connection is established.

## add a log

Register a distributed log with `pgzlog_add_log` by specifying the name of the
log and pool it should be stored it. The hostname and port of the sequencer
service can be specified, and using `null` will use the the default sequencer
connection. If the log does not exist it will be created.

```sql
SELECT pgzlog_add_log('pg_pool', 'pg_log', 'seq-host', 5678);
/* OR */
SELECT pgzlog_add_log('pg_pool', 'pg_log', null, null);
```

The set of registered logs is stored in `pgzlog_metadata.log`:

```bash
postgres=# SELECT * from pgzlog_metadata.log;
  name  |   pool  | host | port | last_applied_pos 
--------+---------+------+------+------------------
 pg_log | pg_pool |      |      |               -1
(1 row)
```

## replicate a table

Register a table to be replicated on a log using `pgzlog_replicate_table`:

```sql
CREATE TABLE coordinates (x int, y int);
SELECT pgzlog_replicate_table('pg_log', 'coordinates');
```

After the table is registered for replication any modification statement is
save to the log so that it may be replayed by all nodes that want to access
the replicated table.
