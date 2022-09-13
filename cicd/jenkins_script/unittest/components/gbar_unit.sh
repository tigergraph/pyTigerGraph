#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace

gadmin start
sleep 20

# clean up zk
$PROJECT_ROOT/zk/bin/zkCli.sh -server 127.0.0.1:19999 <<EOF
rmr /tigergraph/dict/objects/__services/RESTPP/_command_nodes
quit
EOF

$PRODUCT/src/glive/test/GBAR/gbar_test
