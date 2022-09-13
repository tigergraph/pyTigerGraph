#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace

gadmin start
sleep 20

sudo service docker start || true

if [[ -d "$PRODUCT/src/vis/tools/apps" ]]; then  cd $PRODUCT/src/vis/tools/apps/gap/test; else  cd $PRODUCT/src/vis/gap/test; fi

# cd $PRODUCT/src/vis/tools/apps/gap/test
if [[ -f e2e.sh ]]; then
  bash e2e.sh -a
else
  echo 'error: no test found!'
  exit 1
fi
cd -
