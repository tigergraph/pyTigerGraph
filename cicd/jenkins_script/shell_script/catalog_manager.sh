#!/bin/bash

cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
source $cwd/../util.sh
set -ex

## Github and directory constant.
repo_name='bigtest'
repo_path="/tmp/${repo_name}$$"
backup_dir="${repo_path}/backup_catalog"
schema_content="${repo_path}/schema_content"
branch_name='QA-CATALOG'

## clone to /tmp/bigtest/backup_catalog
function clone_bigtest_catalog() {
  rm -rf $repo_path
  git_clone $repo_name $branch_name $repo_path
  cd ${repo_path} && git pull
}

## gadmin pull dict from admin server to /tmp/bigtest/backup_catalog
function backup_catalog() {
  clone_bigtest_catalog
  rm -rf ${backup_dir} && mkdir ${backup_dir}
  gadmin __pullfromdict ${backup_dir}
  # store schema information to file and push to github
  gsql ls > ${schema_content}
  cd ${repo_path}
  git config --global user.name "QA_auto-backup-catalog"
  git config --global user.email auto_catalog@QA
  git add . || true
  git commit -am "[QA-CATALOG] hourly test catalog backup at $(date +%Y-%m-%dT%H:%M:%S%z)" || true
  git push -f origin ${branch_name}
  rm -rf ${repo_path}
}

## gadmin push dict from /tmp/bigtest/backup_catalog to admin server
function restore_catalog() {
  clone_bigtest_catalog
  if [[ -d ${backup_dir} && -f ${schema_content} ]]; then
    gadmin __pushtodict ${backup_dir}
    echo -e "\n check gsql back compatible by running command: gsql ls"
    # store schema information to tmp file
    echo '-------------------------base------------------'
    cat ${schema_content}
    # changed download catalog logic, currently we only download necessary yamls,
    # have to restart gsql to reconstruct new graph from global catalog
    gadmin restart gsql -y
    echo '-------------------------log------------------'
    gsql "use graph computerNet ls" | tee /tmp/schema_content

    # compare current schema content with old schema content which is expected to be included
    python3 ${PYTHON3_SCRIPT_FOLDER}/compare_file.py ${schema_content} /tmp/schema_content
  else
    echo -e "${backup_dir} not found. The reason might be: " \
          "1. new system no backup_dir before 2. backup_dir is not there " \
          "3. gadmin push from dict error but exit code is still 0"
  fi
  rm -rf ${repo_path}
}

##############################################
# Arguments:
#   0: this script name
#   1: backup or restore tag
##############################################

if [ $# -ne 1 ]; then
  echo "backup or restore should be declare"
  exit 1
fi

if [ "$1" = "backup" ]; then
  backup_catalog
elif [ "$1" = "restore" ]; then
  restore_catalog
else
  echo "Invalid Parameter. It should be backup or restore "
  exit 1
fi
