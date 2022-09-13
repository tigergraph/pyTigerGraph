#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace

gadmin start
sleep 20

sudo service docker start || true

if [[ -d "$PRODUCT/src/vis/tools/apps" ]]; then  cd $PRODUCT/src/vis/tools/apps/gap/test; else  cd $PRODUCT/src/vis/gap/test; fi

if [[ -f unit-test.sh ]]; then
  bash unit-test.sh
elif [[ -f unit_test.sh ]]; then
  bash unit_test.sh
elif [[ -f test.sh ]]; then
  bash test.sh
else
  echo 'error: no test found'
  exit 1
fi
cd -
