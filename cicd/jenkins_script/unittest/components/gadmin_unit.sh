#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace
read_ut_opt "$@"

cd $PRODUCT

gadmin stop admin -y
gadmin stop -y
gadmin start zk

# clean up zk
$PROJECT_ROOT/zk/bin/zkCli.sh -server 127.0.0.1:19999 <<EOF
rmr /tigergraph/dict/objects/__services/RESTPP/_static_nodes
quit
EOF

# run tests
cmake_build/release/test/admin_server_unittest #UT
cmake_build/release/test/admin_server_smoketest #ST

# recover restpp config
gadmin __sync-config-to-dict
