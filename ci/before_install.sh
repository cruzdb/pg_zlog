#!/bin/bash

set -e
set -x

sudo apt-get update
sudo apt-get install -y python-virtualenv

#
# Install Stuff
#
WORKDIR=$HOME/workdir
mkdir $WORKDIR
pushd $WORKDIR

# ceph-deploy and ceph
ssh-keygen -f $HOME/.ssh/id_rsa -t rsa -N ''
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
git clone git://github.com/ceph/ceph-deploy
pushd ceph-deploy
./bootstrap
./ceph-deploy install --release kraken `hostname`
./ceph-deploy pkg --install librados-dev `hostname`
popd # ceph-deploy

# zlog plugin for ceph
git clone --branch zlog/master --recursive https://github.com/noahdesu/ceph
pushd ceph
./install-deps.sh
./do_cmake.sh
pushd build
make cls_zlog cls_zlog_client
cp lib/libcls_zlog_client.so /usr/lib/
cp -a lib/libcls_zlog.so* /usr/lib/rados-classes
cp ../src/cls/zlog/cls_zlog_client.h /usr/include/rados/
popd # build dir
popd # ceph source

# zlog
git clone --branch install --recursive https://github.com/noahdesu/zlog
pushd zlog
mkdir build
pushd build
cmake ..
make
make install
popd # build dir
popd # zlog source

popd # workdir
