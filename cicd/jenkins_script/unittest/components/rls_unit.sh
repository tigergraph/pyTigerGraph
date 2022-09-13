#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh
set +e

save_workspace
read_ut_opt "$@"

cd $PRODUCT
# start zk
gadmin stop admin -y
gadmin stop -y
gadmin start zk

# run tests
cmake_build/release/test/greplica_unittest --gtest_filter=-NamesTest.PartitionNameTest
