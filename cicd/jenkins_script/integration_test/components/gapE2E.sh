#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})

repo_name=gap

source ../env.sh &>> $LOG_FILE

test_list="$1"

if [[ -d "$PRODUCT/src/vis/tools/apps" ]]; then  cd $PRODUCT/src/vis/tools/apps/gap/test; else  cd $PRODUCT/src/vis/gap/test; fi
# cd $PRODUCT/src/vis/tools/apps/gap/test
if [[ -f e2e.sh ]]; then
  for test_name in $test_list; do
    run_test "bash e2e.sh ${test_name}" "${repo_name}" "" "${test_name}" ""
  done
else
  echo 'error: no test found!'
  exit 1
fi

cd -
