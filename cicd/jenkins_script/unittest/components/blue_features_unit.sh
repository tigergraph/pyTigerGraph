#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace

gadmin start
sleep 20

if [ -d ${PRODUCT}/src/engine ]; then
  cd $PRODUCT/src/engine/blue/features/gtest
else
  cd $PRODUCT/src/blue/features/gtest
fi
bash test_all.sh
cd -
