#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace

read_ut_opt "$@"

# start zk and gdict
#gadmin restart zk -y
#sleep 30
gadmin restart dict -y
sleep 5
gadmin start
sleep 20
gadmin stop gse restpp gpe -y

# clean up zk
$PROJECT_ROOT/zk/bin/zkCli.sh -server 127.0.0.1:19999 <<EOF
rmr /tigergraph/dict/objects/__services/RLS-GSE/_static_nodes
rmr /tigergraph/dict/objects/__services/RLS-GSE/_expelled_nodes
config
quit
EOF

cd $PRODUCT

# run tests
# build/release/olgp/unittests/gunit --gtest_filter=GSETEST.*

add_filter=""
if [ -n "ASAN_OPTIONS" ]; then
  # Add filter to ignore QA-710 since it is gcc issue
  add_filter=":-GSETEST.HFCompressor"
fi

OLD_LD_PRELOAD=${LD_RELOAD}
if [ "$SANITIZER" == "asan" ]; then
    export LD_PRELOAD=/usr/lib64/libasan.so.5
fi
cmake_build/release/test/olgp_unittests --gtest_filter=GSETEST.*${add_filter}:-GSETEST.High*
cmake_build/release/test/olgp_unittests --gtest_filter=GSETEST.High*
export LD_RELOAD=${OLD_LD_PRELOAD}
