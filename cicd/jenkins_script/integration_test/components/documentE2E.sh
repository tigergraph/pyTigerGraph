#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})

repo_name=document

source ../env.sh &>> $LOG_FILE

type="$1"; input_tests="$2";

cd $PRODUCT/src/document/gtest

data_folder="$(gadmin config get System.AppRoot)/document/examples/gsql_ref"
grun all "mkdir -p ${data_folder}"
gscp all "${data_folder}/data" "${data_folder}" || :

api_version=v2

gsql "set json_api = \"$api_version\" "
gadmin restart gsql -y
export json_api_version=$api_version

rm -rf output/*
rm -rf diff/*

rm -rf base_line
cp -rf ${api_version}_base_line base_line

test_dir=test_case/shell/${type}

for num in $input_tests; do
  setup_cmd="bash ./resources/shell/$type/regress$num/setup.sh"
  run_cmd="bash gtest.sh shell.sh ${type} ${num}"

  run_test "(${setup_cmd}) && (${run_cmd})" "${repo_name}" "${type}" "${num}" "${test_dir}" 
done

rm -rf ${api_version}_output ${api_version}_diff
cp -rf output ${api_version}_output
cp -rf diff ${api_version}_diff

cd -