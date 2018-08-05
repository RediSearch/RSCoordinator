#!/bin/bash
set -x
set -e

MODULE_OSS_SO=$BUILD_DIR/module-oss.so

ROOT=$PWD
cd $BUILD_DIR

cat > rmtest.config << EOF
[server]
module = $ROOT/$MODULE_OSS_SO
EOF

ctest -V
nosetests -vs $ROOT/pytest