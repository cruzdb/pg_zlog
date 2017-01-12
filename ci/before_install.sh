#!/bin/bash

set -e
set -x

WORKDIR=$HOME/workdir
mkdir $WORKDIR
pushd $WORKDIR

git clone git://github.com/ceph/ceph-deploy
pushd ceph-deploy
./bootstrap

popd # ceph-deploy
popd # workdir
