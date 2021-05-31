#!/bin/bash
set -x
set -e

PROJECT_ROOT=$PWD

pushd $PROJECT_ROOT/src/dep/rmr/hiredis/;git apply $PROJECT_ROOT/hiredis_patch;touch applied_hiredis_patch;popd;

mkdir -p $BUILD_DIR
cd $BUILD_DIR

$PROJECT_ROOT/configure.py -j8
