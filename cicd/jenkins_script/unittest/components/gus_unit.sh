#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace

gadmin start
sleep 20

sudo service docker start || true

cd $PRODUCT/src/vis/gus/test
if [[ -f unit-test.sh ]]; then
  bash unit-test.sh
elif [[ -f unit_test.sh ]]; then
  bash unit_test.sh
elif [[ -f test.sh ]]; then
  bash test.sh
else
  echo 'error: no test found'
fi
cd -
