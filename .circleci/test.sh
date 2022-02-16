#!/bin/bash

set -xe

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/.. && pwd)

[[ -n "$REDISEARCH_CI_SKIP_TESTS" ]] && exit 0

BUILD_DIR=${BUILD_DIR:-build}
BUILD_DIR=$(cd $BUILD_DIR; pwd)

cd $BUILD_DIR
ctest -V

cd $ROOT
MODULE=$BUILD_DIR/module-oss.so
test_args="--env oss-cluster --env-reuse --clear-logs --shards-count 3"
test_cmd="$ROOT/src/dep/RediSearch/tests/pytests/runtests.sh $MODULE $test_args"

export EXT_TEST_PATH=src/dep/RediSearch/tests/ctests/ext-example/libexample_extension.so

REJSON=1 MODARGS="PARTITIONS AUTO" $test_cmd
REJSON=1 MODARGS="OSS_GLOBAL_PASSWORD password; PARTITIONS AUTO" $test_cmd --oss_password password
REJSON=1 MODARGS="PARTITIONS AUTO SAFEMODE" $test_cmd

tls_args="--tls \
	--tls-cert-file $ROOT/tests/tls/redis.crt \
	--tls-key-file $ROOT/tests/tls/redis.key \
	--tls-ca-cert-file $ROOT/tests/tls/ca.crt \
	--tls-passphrase foobar"

$ROOT/gen-test-certs.sh
REJSON=1 $test_cmd $tls_args
