#!/bin/bash

# Now let's also handle ftl..
mkdir -p build-ftl
cd build-ftl
cmake -DCMAKE_BUILD_TYPE=RELWITHDEBINFO -DRS_MODULE_NAME=ftl -DFORCE_CROS_SLOT=ON ../src/dep/RediSearch
make -sj20
# do we need to test?

export MODULE_SO=$PWD/redisearch.so
export RAMP_ARGS="-n ftl"
export RAMP_YML="ramp-light.yml"
export PACKAGE_NAME="redisearch-light"

cd ..

source src/dep/RediSearch/.circleci/ci_package.sh
