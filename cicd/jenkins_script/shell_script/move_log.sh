#!/bin/bash

cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
source $cwd/../util.sh
set -ex

log_dir=$1
slave_name=$2
storage_machine=$3
basic_log_dir=$(dirname ${log_dir})
log_name=$(basename ${log_dir})
slave_dir=${log_dir}/${slave_name}

# tar log_dir for ssh
tar -czf ${log_dir}.tar.gz --sparse --directory=${basic_log_dir} ${log_name}
ssh ${storage_machine} << EOF
  mkdir -p ${slave_dir}
EOF
scp ${log_dir}.tar.gz ${storage_machine}:${slave_dir}/${log_name}.tar.gz
ssh ${storage_machine} << EOF
  cd ${slave_dir}
  tar xzf ${log_name}.tar.gz
  rm -rf ${log_name}.tar.gz
EOF
rm -rf ${log_dir}*
