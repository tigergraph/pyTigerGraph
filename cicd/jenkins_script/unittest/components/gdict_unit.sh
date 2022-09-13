#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace
read_ut_opt "$@"


cd $PRODUCT

gadmin stop admin -y
gadmin stop -y
gadmin start zk

OLD_LD_PRELOAD=${LD_RELOAD}
if [ "$SANITIZER" == "asan" ]; then
  export LD_PRELOAD=/usr/lib64/libasan.so.5
fi
# run tests
cmake_build/release/test/gdict_unittests

export LD_RELOAD=${OLD_LD_PRELOAD}
