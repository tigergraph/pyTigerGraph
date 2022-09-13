#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace
read_ut_opt "$@"


cd $PRODUCT

OLD_LD_PRELOAD=${LD_RELOAD}
if [ "$SANITIZER" == "asan" ]; then
  export LD_PRELOAD=/usr/lib64/libasan.so.5
fi
# run tests
if [ -z "$UT_PARAM" ]; then
  cmake_build/release/test/olgp_unittests --gtest_filter=GP* -GPE4UDFTEST.Partition_UD
  cmake_build/release/test/olgp_unittests --gtest_filter=UTILTEST.*
  cmake_build/release/test/olgp_unittests --gtest_filter=GNETTEST.*
else
  cmake_build/release/test/olgp_unittests $UT_PARAM
fi

export LD_PRELOAD=${OLD_LD_PRELOAD}
