# inital cloudlab setup

1. run `cl-install.sh` on all nodes to install ceph and zlog dependencies.
2. run `micro-osd.sh /tmp/osd <iface>` to start a memstore osd.
3. copy `/tmp/osd/ceph.conf` to `/etc/ceph/ceph.conf` on all nodes
4. run `cl-install-db.sh` on all database nodes to install postgres,
   pg\_paxos, and pg\_zlos.

# experiments

metrics to record are insert rate and i/o activity. we expect baseline to
perform the best, with the insert rate of pg_paxos suffering from the slower
hdd nodes in the quorum even when those nodes are not handling the update
queries. the performance of pg_zlog should be slightly slower than baseline,
and the postgres replica nodes see no traffic unless they are running a query.

## baseline

1. single postgresql node with ssd

## pg_paxos

1. three node quorum with 1 ssd and 2 hdd

## pg_zlog

1. three postgresql nodes with 1 ssd and 2 hdd
2. updates flow through db node with ssd
