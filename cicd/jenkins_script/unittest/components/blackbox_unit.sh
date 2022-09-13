#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace

gadmin start
sleep 20
gadmin stop ${VIS_NAME:-gui} -y
sleep 5

cd $PRODUCT/bigtest/tests/gtest/
bash run_all.sh
cd -
if [[ -d $PRODUCT/bigtest/tests/gpr_test ]]; then
  cd $PRODUCT/bigtest/tests/gpr_test/
  bash run_all.sh
  cd -
fi
