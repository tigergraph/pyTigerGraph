#!/bin/bash
########################################################################################
# setup env
if [ -f ~/.bashrc_tg ]; then
  source ~/.bashrc_tg || true
fi

if [[ -z "$PRODUCT" ]]; then
  export PRODUCT=~/product
fi

export MIT_FOLDER=$(cd $(dirname ${BASH_SOURCE[0]})/../ && pwd)
export JENKINS_SCRIPT_FOLDER="${MIT_FOLDER}/jenkins_script"
export SHELL_SCRIPT_FOLDER="${JENKINS_SCRIPT_FOLDER}/shell_script"
export PYTHON_SCRIPT_FOLDER="${JENKINS_SCRIPT_FOLDER}/python_script"
export PYTHON3_SCRIPT_FOLDER="${JENKINS_SCRIPT_FOLDER}/python3_script"
export MAJOR_VERSION=$(echo $MIT_TG_VERSION | cut -d '.' -f 1)
export MINOR_VERSION=$(echo $MIT_TG_VERSION | cut -d '.' -f 2)

########################################################################################
# function to save and restore zk, kafka, and gsql config folder
if which gadmin &> /dev/null; then
  if [[ $MAJOR_VERSION -ge 3 ]]; then
    export PROJECT_ROOT=$(gadmin config get System.AppRoot)
    export CONFIG_DIR=$(gadmin config get System.TempRoot)
    export LOG_ROOT=$(gadmin config get System.LogRoot)
    export DATA_ROOT=$(gadmin config get System.DataRoot)
    export KAFKA_DIR="${DATA_ROOT}/kafka"
    export ZK_DIR="${DATA_ROOT}/zk"
  else
    export PROJECT_ROOT=$(grep tigergraph.root.dir $HOME/.gsql/gsql.cfg | cut -d " " -f 2)
    export CONFIG_DIR="$HOME/.gsql"
    export LOG_ROOT=$(grep tigergraph.log.root $HOME/.gsql/gsql.cfg | cut -d " " -f 2)
    export KAFKA_DIR="${PROJECT_ROOT}/kafka"
    export ZK_DIR="${PROJECT_ROOT}/zk"
  fi

  export GSQL_PATH=${PROJECT_ROOT}/dev/gdk/gsql 
fi

# LD_LIBRARY_PATH needs to be setup correctly
# NOTE:
#   1. cqrs e2e shell 13 needs /opt/rh/python27/root/usr/lib64
#   2. /usr/local/bin/curl needs /usr/local/lib/libcurl.so.4
#   3. grun needs system /usr/lib64/libcrypto.so.1.1
#   4. $PROJECT_ROOT/.syspre/usr/lib_ld2 is needed to run unit test, e.g. cmake_build/release/test/olgp_unittests
echo "PROJECT_ROOT: $PROJECT_ROOT"
export LD_LIBRARY_PATH=/opt/rh/python27/root/usr/lib64:/usr/local/lib:/usr/lib64:/usr/lib/x86_64-linux-gnu:$PROJECT_ROOT/bin:$PROJECT_ROOT/.syspre/usr/lib_ld1:$PROJECT_ROOT/.syspre/usr/lib_ld2:$PROJECT_ROOT/.syspre/usr/lib_ld3:$PROJECT_ROOT/.syspre/usr/lib_ld4:$LD_LIBRARY_PATH

export GSQL_TEMP_DIR="/tmp/${USER}_tigergraph_temp"
export KAFKA_TEMP_DIR="${GSQL_TEMP_DIR}/kafka"
export ZK_TEMP_DIR="${GSQL_TEMP_DIR}/zk"

export CONFIG_TEMP_DIR="/tmp/${USER}_gsql_conf_temp"


function stop_service() {
  if [[ $MAJOR_VERSION -ge 3 ]]; then
    gadmin start infra
    gadmin stop all -y
  else
    gadmin stop all admin ts3 -y
  fi
  sleep 5
  if which killall &> /dev/null; then
    killall -9 ts3agent ts3svc tg_infr_ts3d tg_infr_ts3m || true
    sleep 5
    killall -9 gadmin_server tg_infr_admind || true
    sleep 5
  elif which pkill &> /dev/null; then
    pkill -9 -f ts3agent ts3svc tg_infr_ts3d tg_infr_ts3m || true
    sleep 5
    pkill -9 -f gadmin_server tg_infr_admind || true
    sleep 5
  fi
}

function check_and_move() {
  grun all "
    if [ -d $1 ]
    then
      rm -rf $2
      mv $1 $2
    fi
  "
}

# save workspace
function save_workspace() {
  stop_service
  # backup zk, kafka folder
  grun all "rm -rf $GSQL_TEMP_DIR"
  grun all "mkdir -p $GSQL_TEMP_DIR"
  grun all "cp -rLp $KAFKA_DIR $KAFKA_TEMP_DIR"

  # only copy zk directory on m1 for 3.x.x since 3.x.x does NOT have zk directory on other nodes
  if [[ $MAJOR_VERSION -ge 3 ]]; then 
    cp -rLp $ZK_DIR $ZK_TEMP_DIR
  else
    grun all "cp -rLp $ZK_DIR $ZK_TEMP_DIR" 
  fi

  # backup gsql folder
  grun all "rm -rf $CONFIG_TEMP_DIR"
  grun all "cp -rLp $CONFIG_DIR $CONFIG_TEMP_DIR"
  gadmin start all
  sleep 10
}

# restore workspace
function restore_workspace() {
  stop_service
  # save pkg info status, needed since we will restore gsql folder
  # which contains old pkg info status
  #grun all "cp -Lp $CONFIG_DIR/fab_dir/logs/status $CONFIG_TEMP_DIR/fab_dir/logs/status"

  # recover zk, kafka folder
  check_and_move $ZK_TEMP_DIR $ZK_DIR
  check_and_move $KAFKA_TEMP_DIR $KAFKA_DIR

  # recover gsql folder
  check_and_move $CONFIG_TEMP_DIR $CONFIG_DIR

  # the order matter here! can not start admin before sync to dict
  #gadmin start dict
  #sleep 10
  #gadmin __sync-config-to-dict
  #sleep 5
  gadmin start all
  sleep 5
  if [[ $MAJOR_VERSION -ge 3 ]]; then
    gadmin config apply -y
  else
    gadmin config-apply
  fi
}

# clean up test case leftovers
function clean_up() {
  if [ -n "$PROJECT_ROOT" ]; then
  rm -rf $PROJECT_ROOT/config/endpoints
  rm -rf $PROJECT_ROOT/bin/scheduler.so
  rm -rf /tmp/unittest
  rm -rf /tmp/gsql

  # clean up zk
  $PROJECT_ROOT/zk/bin/zkCli.sh -server 127.0.0.1:19999 <<EOF || true
  rmr /tigergraph/dict/objects/__services/RLS-GSE/_static_nodes
  rmr /tigergraph/dict/objects/__services/RLS-GSE/_expelled_nodes
  quit
EOF
  fi
}

########################################################################################
# Functions and variables for address sanitizer
function read_ut_opt(){
  export UT_PARAM=""
  while [[ $# -gt 0 ]]; do
    #Set options for address sanitizer
    if [ $1 = "-sanitizer" ]; then
      set_asan_opt $2
      shift 2
    else
      export UT_PARAM="$UT_PARAM $1"
      shift 1
    fi
  done
}

function set_asan_opt() {
  local SANITIZER_TYPE="addr" # thread
  if [[ $# > 1 && "$1" == "thread" ]]; then
    SANITIZER_TYPE=$1
  fi
  echo 'Setting options for address sanitizer'
  export ASAN_OPTIONS="disable_coredump=0:unmap_shadow_on_exit=1:abort_on_error=1:symbolize=1"
  export ASAN_SYMBOLIZER_PATH=$(which llvm-symbolizer)
  export LSAN_OPTIONS="suppressions=${JENKINS_SCRIPT_FOLDER}/config/leak_blocker"
}

function git_clone() {
  local repo=$1
  local branch=$2
  local path=$3
  local option=$4
  git clone -b $branch --quiet --depth=1 $option \
      https://$GIT_USER:$GIT_TOKEN@github.com/TigerGraph/${repo}.git $path
}

function read_config() {
  echo $(jq -r .$1 ${JENKINS_SCRIPT_FOLDER}/config/config.json)
}

function read_test_config() {
  echo $(jq -r .$1 $2)
}

function get_base_branch() {
  local branch=$1
  local commit
  for commit in $(git rev-list $branch); do
    base_branch=$(git branch -r --contains $commit | grep "origin/tg_.*_dev$" | sort | head -1)
    if [ -n "$base_branch" ]; then
      echo ${base_branch#*origin/}
      break
    fi
  done
}

function gadmin_config_append() {
  #remove whitespaces
  local config_param=$(echo $1)
  local config_value=$(echo $2)
  [ -z "$config_param" -o -z "$config_value" ] && return
  local current_config=$(echo $(gadmin config get $config_param || true))
  if [ -n "$current_config" ]; then
    config_value="${current_config%;}; ${config_value%;};"
    gadmin config set $config_param "$config_value"
  else
    echo "Config parameter is not found, skipping"
  fi
}

function gadmin() {
  $(which gadmin) "$@"
  if [[ "$1" =~ start ]]; then
    sleep 10
  fi
}

export GIT_USER=$MIT_GIT_USER
export GIT_TOKEN=$MIT_GIT_TOKEN
########################################################################################
