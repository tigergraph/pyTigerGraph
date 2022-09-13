#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace

read_ut_opt "$@"

# start zk
gadmin stop admin -y
gadmin stop -y
gadmin start zk

cd $PRODUCT

OLD_LD_PRELOAD=${LD_RELOAD}
if [ "$SANITIZER" == "asan" ]; then
  export LD_PRELOAD=/usr/lib64/libasan.so.5
fi
# run tests
cmake_build/release/test/zk_unittests
export LD_RELOAD=${OLD_LD_PRELOAD}
