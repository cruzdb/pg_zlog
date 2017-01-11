1. run `cl-install.sh` on all nodes to install ceph and zlog dependencies.
2. run `micro-osd.sh /tmp/osd <iface>` to start a memstore osd.
3. copy `/tmp/osd/ceph.conf` to `/etc/ceph/ceph.conf` on all nodes
4. run `cl-install-db.sh` on all database nodes to install postgres,
   pg\_paxos, and pg\_zlos.
