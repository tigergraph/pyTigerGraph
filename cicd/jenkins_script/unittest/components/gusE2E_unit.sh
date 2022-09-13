#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace

gadmin start
sleep 20

sudo service docker start || true

cd $PRODUCT/src/vis/gus/test
if [[ -f e2e.sh ]]; then
  bash e2e.sh -a
else
  echo 'error: no test found!'
  exit 1
fi
cd -
