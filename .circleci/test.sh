#!/bin/bash

set -x
set -e

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(readlink -f $HERE/..)

MODULE_OSS_SO=$BUILD_DIR/module-oss.so

if [[ -n "$REDISEARCH_CI_SKIP_TESTS" ]]; then
    exit 0
fi

cd $BUILD_DIR

cat > rmtest.config << EOF
[server]
module = $ROOT/$MODULE_OSS_SO
EOF

ctest -V
python -m RLTest --env oss-cluster --env-reuse -t $ROOT/src/dep/RediSearch/src/pytest/ --clear-logs --shards-count 3 --module $ROOT/$MODULE_OSS_SO --module-args "PARTITIONS AUTO"
python -m RLTest --env oss-cluster --env-reuse -t $ROOT/src/dep/RediSearch/src/pytest/ --clear-logs --shards-count 3 --module $ROOT/$MODULE_OSS_SO --module-args "PARTITIONS AUTO SAFEMODE"
