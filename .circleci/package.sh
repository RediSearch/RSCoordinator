#!/bin/bash

set -x
set -e

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/..; pwd)

BUILD_DIR=${BUILD_DIR:-build}

export ARTDIR=$ROOT/artifacts
export PACKAGE_NAME=redisearch
export RAMP_YAML=$ROOT/ramp.yml

DEPS=0 $ROOT/src/dep/RediSearch/pack.sh $ROOT/$BUILD_DIR/module-enterprise.so
