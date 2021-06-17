#!/bin/bash

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/..; pwd)

mkdir -p build-ftl
cd build-ftl

cmake -DCMAKE_BUILD_TYPE=RELWITHDEBINFO -DRS_MODULE_NAME=searchlight $ROOT/src/dep/RediSearch
make -sj20

export ARTDIR=$ROOT/artifacts
export PACKAGE_NAME=redisearch-light
export RAMP_YAML=$ROOT/ramp-lightrce.yml
export RAMP_ARGS="-n searchlight"

$ROOT/src/dep/RediSearch/pack.sh $PWD/redisearch.so
