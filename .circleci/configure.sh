#!/bin/bash

set -xe

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/..; pwd)

BUILD_DIR=${BUILD_DIR:-build}

mkdir -p $ROOT/$BUILD_DIR
BUILD_DIR=$(cd $ROOT/$BUILD_DIR; pwd)

cd $BUILD_DIR

$ROOT/configure.py -j$($ROOT/deps/readies/bin/nproc)
