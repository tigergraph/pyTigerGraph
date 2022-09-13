#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})

repo_name=bigtest

source ../env.sh &>> $LOG_FILE

gadmin stop ${VIS_NAME:-gui} -y
sleep 5

cd $PRODUCT/bigtest/tests/gtest/

type="$1"; test_list="$2";

test_dir=test_case/gsql/${type}

[[ -z $class ]] && class=gsql

for test_id in $test_list; do
  run_test "bash run_all.sh -c ${class} -s ${type} -r ${test_id}" "${repo_name}" "${type}" "${test_id}" "${test_dir}"
done

cd -
