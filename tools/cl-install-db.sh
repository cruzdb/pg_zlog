#!/bin/bash

set -e
set -x

PGZLOG_BRANCH="testing"

### install deps
apt-get update
apt-get install -y git postgresql postgresql-contrib postgresql-server-dev-all

### install pg_paxos
git clone https://github.com/citusdata/pg_paxos
pushd pg_paxos
make
sudo make install
popd

### install pg_zlog
git clone --branch $PGZLOG_BRANCH https://github.com/noahdesu/pg_zlog
pushd pg_zlog
make
sudo make install
popd

### fixup conf files
sed -i "/shared_preload_libraries/s/.*/shared_preload_libraries = 'pg_paxos,pg_zlog'/" /etc/postgresql/9.5/main/postgresql.conf
sed -i "/listen_addresses/s/.*/listen_addresses = '*'/" /etc/postgresql/9.5/main/postgresql.conf
echo "pg_paxos.node_id = '$(hostname --short)'" >> /etc/postgresql/9.5/main/postgresql.conf
echo "host    all      all      0.0.0.0/0     trust" >> /etc/postgresql/9.5/main/pg_hba.conf

systemctl restart postgresql
