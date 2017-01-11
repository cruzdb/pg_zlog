#!/bin/bash

set -e
set -x

HOST=$(hostname --short)

# install deps
apt-get update
apt-get install -y python-virtualenv protobuf-compiler libprotobuf-dev

# install ceph binaries and dev pkgs
git clone https://github.com/ceph/ceph-deploy
pushd ceph-deploy
./bootstrap
./ceph-deploy install --release kraken $HOST
./ceph-deploy pkg --install librados-dev $HOST
popd

# build and install ceph zlog plugin
git clone --branch zlog/master --recursive https://github.com/noahdesu/ceph
pushd ceph
./install-deps.sh
./do_cmake.sh
pushd build
make cls_zlog cls_zlog_client
cp lib/libcls_zlog_client.so /usr/lib/
cp -a lib/libcls_zlog.so* /usr/lib/rados-classes
cp ../src/cls/zlog/cls_zlog_client.h /usr/include/rados/
popd
popd

# build and install zlog
git clone --branch install --recursive https://github.com/noahdesu/zlog
pushd zlog
mkdir build
pushd build
cmake ..
make
make install
popd
popd
