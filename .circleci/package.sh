#!/bin/bash
set -x
set -e

export MODULE_SO=$BUILD_DIR/module-enterprise.so
export PACKAGE_NAME="redisearch"

source src/dep/RediSearch/.circleci/ci_package.sh

# And let 'er rip
