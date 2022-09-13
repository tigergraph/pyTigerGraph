#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace

gadmin start
sleep 20
gadmin stop ${VIS_NAME:-gui} -y
sleep 5

cd $PRODUCT/src/document/gtest
bash run_all.sh
cd -
