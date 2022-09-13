#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace

if [[ ! -f $PRODUCT/src/glive/test/TS3/ts3_test ]]; then
  echo 'ts3 is not included'
  exit 0
fi

gadmin start zk
sleep 5
gadmin start
sleep 5

cd $PRODUCT/src
if [[ -f MakePrebuiltThirdparty ]]; then
  make -f MakePrebuiltThirdparty
fi  
cd -

cd $PRODUCT/src/glive/test/TS3
bash ts3_test
cd -
