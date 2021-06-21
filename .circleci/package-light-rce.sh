#!/bin/bash

set -xe

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/.. && pwd)

BUILD_DIR=build-ftl

mkdir -p $BUILD_DIR
BUILD_DIR=$(cd $ROOT/$BUILD_DIR; pwd)

cd $BUILD_DIR

cmake -DCMAKE_BUILD_TYPE=RELWITHDEBINFO -DRS_MODULE_NAME=searchlight $ROOT/src/dep/RediSearch
make -sj$($ROOT/deps/readies/bin/nproc)

export ARTDIR=$ROOT/artifacts
export PACKAGE_NAME=redisearch-light
export RAMP_YAML=$ROOT/ramp-lightrce.yml
export RAMP_ARGS="-n searchlight"

$ROOT/src/dep/RediSearch/pack.sh $BUILD_DIR/redisearch.so
