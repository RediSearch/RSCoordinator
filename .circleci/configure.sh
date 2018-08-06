#!/bin/bash
set -x
set -e

PROJECT_ROOT=$PWD
mkdir -p $BUILD_DIR
cd $BUILD_DIR

$PROJECT_ROOT/configure.py -j8