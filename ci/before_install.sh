#!/bin/bash

set -e
set -x

sudo apt-get update
sudo apt-get install -y python-virtualenv

ssh-keygen -f $HOME/.ssh/id_rsa -t rsa -N ''
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

WORKDIR=$HOME/workdir
mkdir $WORKDIR
pushd $WORKDIR

git clone git://github.com/ceph/ceph-deploy
pushd ceph-deploy
./bootstrap

./ceph-deploy install --release kraken `hostname`
./ceph-deploy pkg --install librados-dev `hostname`

popd # ceph-deploy
popd # workdir
