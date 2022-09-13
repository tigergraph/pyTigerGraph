#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace

gadmin start
sleep 20

sudo service docker start || true

if [[ -d "$PRODUCT/src/vis/tools/apps" ]]; then  cd $PRODUCT/src/vis/tools/apps/gst/test; else  cd $PRODUCT/src/vis/gst/test; fi
# cd $PRODUCT/src/vis/tools/apps/gst/test
if [[ -f e2e.sh ]]; then
  if [[ -n "$@" ]]; then
    bash e2e.sh $@
  else
    bash e2e.sh -a
  fi
else
  echo 'error: no test found!'
  exit 1
fi
cd -
