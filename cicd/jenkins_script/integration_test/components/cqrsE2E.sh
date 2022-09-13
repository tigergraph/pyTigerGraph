#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})

repo_name=cqrs

source ../env.sh &>> $LOG_FILE

cd $PRODUCT/src/cqrs
rm -rf dev_tool/src/*
GLOCAL=~/.glocal
bash ./install_base.sh &>> $LOG_FILE
export GOROOT=${GLOCAL}/go/go
PROTOC_ROOT=$(find $GLOCAL -type f -name protoc | head -1)
export PATH=$GOROOT/bin:${PROTOC_ROOT%%/protoc}:$PATH

type="$1"; test_list="$2";

sed -i 's/TimeOutMinutes=[0-9]*/TimeOutMinutes=60/g' e2e/config
sed -i 's/numThreads=[0-9]*/numThreads=1/g' e2e/config

test_dir=e2e/test_case/${type}


if [[ $type == "shell" ]]; then 
  alt_type=single
else
  alt_type=$type
fi

IT_PARAM="e2e_$alt_type"
if grep "^${IT_PARAM}:" makefile &> /dev/null; then
  for test_id in $test_list; do
    run_test "make ${IT_PARAM} regress=${test_id}" "${repo_name}" "${type}" "${test_id}" "${test_dir}"
  done
else
  echo "Warning: target ${IT_PARAM} is not supported, skipping"
fi

cd -
