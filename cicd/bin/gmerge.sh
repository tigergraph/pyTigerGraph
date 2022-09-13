#!/bin/bash
#####################################
# GMERGE function                   #
#   mit : to merge a pull request   #
#   wip : to test a pull request    #
#####################################

GMW_FILE=$HOME/.gmerge.sh

SELF_PATH=
if [[ ! -z "$ZSH_NAME" ]]; then
  SELF_PATH="${(%):-%N}"
elif [[ ! -z "$BASH" ]]; then
  SELF_PATH="${BASH_SOURCE[0]}"
else
  echo "Unsupported shell."
  exit 1
fi

FILE=$(readlink -n "$SELF_PATH")

if [[ "$FILE" != "$(readlink -n $GMW_FILE)" ]]; then
  cp -f $FILE $GMW_FILE
fi
CMD="[ -f $GMW_FILE ] && source $GMW_FILE"
if ! grep "$CMD" $HOME/.bashrc &> /dev/null
then
  # command not in ~/.bashrc
  echo $CMD >> $HOME/.bashrc
fi

default_base_branch='master'
mit_server="http://192.168.99.101:30088/api"
JENKINS_IP="192.168.99.101"
JENKINS_PORT="30080"

function __upgrade {
  branch=${TMD_BRANCH:-master}
  if [ $# -gt 0 ]; then
    branch=$1
  fi
  GIT_USER='graphTester'
  GIT_TOKEN='9ae9cfc006a4fed89078b850c1b2fe4aadaa0947'
  curl -s https://${GIT_USER}:${GIT_TOKEN}@raw.githubusercontent.com/TigerGraph/tmd/${branch}/mit/bin/gmerge.sh -o ${GMW_FILE}
  source ${GMW_FILE}
}

function __check_mit_wip_cmd() {
  if [ "$1" = "-u" ]; then
    GMERGE_USER=$2
    GMERGE_USER=$(__get_email_prefix $GMERGE_USER)
    echo "GMERGE_USER=$GMERGE_USER" > ~/.gmerge_user.conf
    shift
    shift
  fi

  if [ "$1" = "-abort" ]; then
    local job_id=$2
    __abort_job $job_id
  elif [ "$1" = "-renew" ]; then
    local id=$2
    local renew_time=$3
    __renew_node_job $id $renew_time
  elif [ "$1" = "-return" ]; then
    local id=$2
    __return_node_job $id
  elif [ "$1" = "-ls" ]; then
    shift
    __ls_user "$@"
  elif [ "$1" = "-detail" ]; then
    __detail
  elif [ "$1" = "-register" ]; then
    local loc=$2
    __register_user $loc
  else
    return 2
  fi
}

function __register_user() {
  local loc=$1
  __get_user
  [ -z ${GMERGE_USER} ] && return 1
  curl --fail -s -X GET "${mit_server}/users/${GMERGE_USER}/register?loc=$loc" 2>&1 >/dev/null
  if [[ "$?" == "0" ]]; then
    echo -e "\nUser ${GMERGE_USER} registration successfully!\n"
  else
    echo -e "\nUser ${GMERGE_USER} registration failed!\n"
  fi
}

function __abort_job() {
  local job_id=$1
  __get_user
  curl --fail -s -X GET "${mit_server}/${PIPELINE_SHORT_NAME}/${job_id}/abort?user=${GMERGE_USER}"
  if [[ "$?" == "0" ]]; then
    echo -e "\nabort ${PIPELINE_SHORT_NAME} ${job_id} successfully!\n"
  else
    echo -e "\nabort ${PIPELINE_SHORT_NAME} ${job_id} failed!\n"
  fi
}

function __renew_node_job() {
  local id=$1
  local renew_time=$2
  local num_str=$(echo $renew_time | tr -dc '0-9')
  if [[ "$renew_time" -ne "$num_str" || "$renew_time" -gt "72" ]]; then
    echo "You format is error or the input time is large than 72"
    return 1
  fi
  __get_user
  if [[ $id == *"."* ]]; then
    # renew node
    curl --fail -s -X GET "${mit_server}/nodes/${id}/renew/${renew_time}?user=${GMERGE_USER}"
    if [[ "$?" == "0" ]]; then
      echo -e "\nrenew node ${id} successfully!\n"
    else
      echo -e "\nrenew node ${id} failed!\n"
    fi
  else
    # renew job
    curl --fail -s -X GET "${mit_server}/test_job/${id}/renew/${renew_time}?user=${GMERGE_USER}"
    if [[ "$?" == "0" ]]; then
      echo -e "\nrenew job ${id} successfully!\n"
    else
      echo -e "\nrenew job ${id} failed!\n"
    fi
  fi
}

function __return_node_job() {
  local id=$1
  __get_user
  echo 'Uninstalling the node before. It might take one or two minutes, DO NOT CANCEL'
  if [[ $id == "all" ]]; then
    # return all jobs
    curl --fail -s -X GET "${mit_server}/users/${GMERGE_USER}/reclaim"
    if [[ "$?" == "0" ]]; then
      echo -e "\nreturn all jobs successfully!\n"
    else
      echo -e "\nreturn all jobs failed!\n"
    fi
  elif [[ $id == *"."* ]]; then
    # return node
    curl --fail -s -X GET "${mit_server}/nodes/${id}/reclaim?user=${GMERGE_USER}"
    if [[ "$?" == "0" ]]; then
      echo -e "\nreturn node ${id} successfully!\n"
    else
      echo -e "\nreturn node ${id} failed!\n"
    fi
  else
    # return job
    curl --fail -s -X GET "${mit_server}/test_job/${id}/reclaim?user=${GMERGE_USER}"
    if [[ "$?" == "0" ]]; then
      echo -e "\nreturn job ${id} successfully!\n"
    else
      echo -e "\nreturn job ${id} failed!\n"
    fi
  fi
}

function __ls_user() {
  __get_user
  local opt_job=""
  if [[ $# > 0 ]]; then
    opt_job="?job_id=$1&job_type=${PIPELINE_SHORT_NAME}"
  fi
  curl --fail -s -X GET "${mit_server}/users/${GMERGE_USER}/activity${opt_job}" | jq -r ".result"
}

function __detail() {
  curl --fail -s -X GET "${mit_server}/detail?j_type=${PIPELINE_SHORT_NAME}" | jq -r ".result"
}

function __get_user() {
  if [ -f ~/.gmerge_user.conf ]
  then
    source ~/.gmerge_user.conf
  fi
  if [ -z $GMERGE_USER ]
  then
    echo "Please enter your company email:"
    read GMERGE_USER
    GMERGE_USER=$(__get_email_prefix $GMERGE_USER)
    echo "GMERGE_USER=$GMERGE_USER" > ~/.gmerge_user.conf
  fi
}

function __get_mit_wip_option() {
  GMERGE_ARGUS=""
  while [[ $# -gt 0 ]]
  do
    if [ "$1" = "-u" ]; then
      GMERGE_USER=$2
      GMERGE_USER=$(__get_email_prefix $GMERGE_USER)
      echo "GMERGE_USER=$GMERGE_USER" > ~/.gmerge_user.conf
      [ ! -z "$GMERGE_USER" -a -z "$3" ] && echo "Email setted successfully!"
      shift
    elif [ "$1" = "-m" ]; then
      GMERGE_MACHINE=$2
      shift
    elif [ "$1" = "-n" ]; then
      GMERGE_NODE_NUM=$2
      shift
    elif [ "$1" = "-b" ]; then
      GMERGE_BASE_BRANCH=$2
      shift
    elif [ "$1" = "-bb" ]; then
      GMERGE_ARGUS="${GMERGE_ARGUS}tmd#${2};"
      shift
    elif [ "$1" = "-ut" ]; then
      GMERGE_UNIT_TEST=$2
      shift
    elif [ "$1" = "-it" ]; then
      GMERGE_INTEGRATION_TEST=$2
      shift
    elif [ "$1" = "-du" ]; then
      GMERGE_DEBUG_MODE=true
    elif [ "$1" = "-sa" ]; then
      GMERGE_SANITIZER=$2
      shift
    elif [ "$1" = "-ad" -o "$1" = "-asan" ]; then
      GMERGE_DEBUG_MODE=true
      GMERGE_SANITIZER="asan"
    elif [ "$1" = "-type" ]; then
      GMERGE_CLUSTER=$2
      shift
    elif [ "$1" = "-timeout" ]; then
      GMERGE_TIMEOUT=$2
      shift
    elif [ "$1" = "-no_fail" ]; then
      GMERGE_NO_FAIL=2
    elif [ "$1" = "-no_abort" ]; then
      GMERGE_NO_FAIL=1
    elif [ "$1" = "-build" ]; then
      GMERGE_BUILD_ONLY="true"
    elif [ "$1" = "-skip_build" ]; then
      GMERGE_SKIP_BUILD=$2
      shift
    elif [ "$1" = "-k8s" ]; then
      GMERGE_BUILD_ONLY="k8s"
    elif [ "$1" = "-scan" ]; then
      GMERGE_BUILD_ONLY="scan"
    elif [ "$1" = "-gen_download" ]; then
      GMERGE_BUILD_ONLY="gen_download"
      GMERGE_SKIP_BUILD="false"
    elif [ "$1" = "-release" ]; then
      GMERGE_BUILD_ONLY="release"
      GMERGE_SKIP_BUILD="false"
    elif [ "$1" = "-prebuild" ]; then
      GMERGE_BUILD_ONLY="prebuild"
      GMERGE_SKIP_BUILD="false"
    elif [ "$1" = "-cloud" ]; then
      GMERGE_BUILD_ONLY="cloud"
      GMERGE_SKIP_BUILD="false"
    elif [ "$1" = "-variant" -o "$1" = "-v" ]; then
      GMERGE_DEV=$2
      shift
    elif [ "$1" = "-cc" ]; then
      [ ! -z $GMERGE_USER ] && GMERGE_USER="${GMERGE_USER},$2"
      shift
    else
      local regex1="^[a-zA-Z_]+#[0-9]+\$"
      local regex2="^[a-zA-Z_]+[#=][0-9A-Za-z_.-]+\$"
      if ! [[ $1 =~ $regex1 || $1 =~ $regex2 ]]; then
        GMERGE_ARGUS=
        echo "Param $1 is not supported!"
        return
      fi
      GMERGE_ARGUS="${GMERGE_ARGUS}${1};"
    fi
    shift
  done

  __get_user
  if [ "$PIPELINE_SHORT_NAME" = "wip" ] && [ -n "$GMERGE_BASE_BRANCH" ] && [ "$GMERGE_BASE_BRANCH" != "default" ] && [ -z $GMERGE_ARGUS ]; then
    GMERGE_ARGUS="product#$GMERGE_BASE_BRANCH"
  fi
  GMERGE_ARGUS=${GMERGE_ARGUS//\#/\%23}
}

function __get_email_prefix() {
  EMAIL_ADDRESS=$1
  regex="^[a-z0-9!#\$%&'*+/=?^_\`{|}~-]+(\.[a-z0-9!#$%&'*+/=?^_\`{|}~-]+)*@([a-z0-9]([a-z0-9-]*[a-z0-9])?\.)+[a-z0-9]([a-z0-9-]*[a-z0-9])?\$"
  EMAIL_PREFIX=${EMAIL_ADDRESS%%@*}
  echo $EMAIL_PREFIX
}

function __send_to_jenkins() {
  JENKINS_JOB=$1
  if [ -n "$GMERGE_TIMEOUT" ]; then
    curl --fail -s -G -X POST "http://qa_build:11671f42b808804a46bf7b2af5b37c3cce@$JENKINS_IP:$JENKINS_PORT/job/$JENKINS_JOB/buildWithParameters" \
      --data "TEST_ENV=http://35.193.234.109:14240"
      --data "Branch=app-e2e-3.7.0"
  else
    curl --fail -s -G -X POST "http://qa_build:11671f42b808804a46bf7b2af5b37c3cce@$JENKINS_IP:$JENKINS_PORT/job/$JENKINS_JOB/buildWithParameters" \
      --data "TMD_BASE_BRANCH=${GMERGE_MIT_BRANCH}"  --data "MACHINE=${GMERGE_MACHINE}" --data "NUM_MACHINE=${GMERGE_NODE_NUM}" \
      --data "USER_NAME=${GMERGE_USER}" --data "PARAM=${GMERGE_ARGUS}" --data "BASE_BRANCH=${GMERGE_BASE_BRANCH}" \
      --data-urlencode "UNITTESTS=${GMERGE_UNIT_TEST}" --data-urlencode "INTEGRATION=${GMERGE_INTEGRATION_TEST}" \
      --data-urlencode "DEBUG_MODE=${GMERGE_DEBUG_MODE}" --data-urlencode "SANITIZER=${GMERGE_SANITIZER}" \
      --data-urlencode "CLUSTER_TYPE=${GMERGE_CLUSTER}" --data-urlencode "NO_FAIL=${GMERGE_NO_FAIL}" \
      --data-urlencode "IS_DEV=${GMERGE_DEV}" --data-urlencode "BUILD_ONLY=${GMERGE_BUILD_ONLY}" --data-urlencode "SKIP_BUILD=${GMERGE_SKIP_BUILD}"
  fi
  if [ $? -ne 0 ]; then
    echo -e "Fail to submit request, please update MIT and WIP to the latest version.\n"
    echo -e "To update MIT and WIP:\n"
    echo -e "  1. cd tmd"
    echo -e "  2. git checkout master (only needed if your tmd repository is not on the master branch)"
    echo -e "  3. git pull"
    echo -e "  4. source bin/gmerge.sh\n"
    echo "If issue reoccurs after updating, please contact QA team."
  else
    echo "Request submitted successfully, you will receive a Zulip message once job starts."
  fi
}

function __help() {
  echo "1. If MIT or WIP does not work as expected, please goto tmd repo and"
  echo "   do a git pull. Then source bin/gmerge.sh"
  echo "2. To set up email : $1 -u your_company_email_id"
  echo "3. To start your testing pipeline: $1 [-u user_name] [-b base_branch] [-bb mit-branch] [-cc other_user_name] [-type [single|cluster]] rep1#pull_number1 rep2#pull_number2"
  echo "      for example, $1 gle#111 gpe#222 gse#333, or wip gle#GLE-1 gpe#GPE-2 gse#GSE-3"
  if [ "$1" = "wip" ]; then
    echo "   By default, wip will run default unittests and integration tests, you can customize it"
    echo "   To specify unittests, use option: -ut 'repo1 repo2 repo3 ...'"
    echo "        for example -ut 'gle gpe gse'"
    echo "        use option: -ut 'default' or -ut 'all' or -ut 'none' to run default/all/none unittests"
    echo "   To specify integration tests, use option: -it 'type1: num1 num2 num3; type2: num4 num5; ...'"
    echo "        for example -it 'shell:10 11 12; gquery: all; loader:200; docExampleTest: all'"
    echo "        for each type regress, you can use 'all' to test all regresses of this type"
    echo "        use option: -it 'default' or -it 'all' or -it 'none' to run default/all/none integration tests"
    echo "   To build binary only use option: -build"
  fi
  echo "4. To abort a $1: -abort mit_wip_id"
  echo "   To renew reserved time for debugging: -renew node_ip additonal_hours"
  echo "   To return debugging node: -return node_ip"
  echo "   To list your current running mit/wip and debugging test_jobs(and node_name): -ls"
  echo "   To list details of all unittests and expected time: -detail"
}

function mit() {
   __upgrade
   __pipeline "mit_test" "mit" "$@"
}

function mle2e() {
#   __upgrade
   __pipeline "mlwb_e2e" "e2e" "$@"
}

function __pipeline() {
  PIPELINE_NAME=$1
  PIPELINE_SHORT_NAME=$2
  shift
  shift
   if [ $# -eq 0 ] || [ "$1" = "-h" ]; then
     __help $PIPELINE_SHORT_NAME
     return
   fi
   
   if [[ "$JENKINS_ID" == "stg"* ]]; then
     # switch traffic to staging jenkins
     JENKINS_IP="35.224.41.133"
     JENKINS_PORT="8080"
     mit_server="http://34.122.5.19:8888/api"
   fi
   
   GMERGE_ARGUS=
   GMERGE_USER=
   GMERGE_MACHINE="MIT"
   GMERGE_NODE_NUM="default"
   GMERGE_BASE_BRANCH="default"
   GMERGE_MIT_BRANCH=$default_base_branch
   # UNITTESTS is "none", so by default it will skip unittests
   GMERGE_UNIT_TEST="default"
   GMERGE_INTEGRATION_TEST="default"
   GMERGE_DEBUG_MODE="false"
   GMERGE_SANITIZER="none"
   GMERGE_CLUSTER="default"
   GMERGE_TIMEOUT=""
   GMERGE_DEV="false"
   GMERGE_NO_FAIL=0
   GMERGE_BUILD_ONLY="false"
   GMERGE_SKIP_BUILD="default"

   __check_mit_wip_cmd "$@"
   cmd_res=$?
   if [[ "$cmd_res" != "2" ]]; then
     # if mit/wip has some cmds other than submit, just return 0 to skip submitting.
     return $cmd_res
   fi
   __get_mit_wip_option "$@"

   if [[ "$PIPELINE_SHORT_NAME" == "mit" ]]; then
     # those values are set after __get_mit_wip_option, so those values will always be "default"
     GMERGE_UNIT_TEST="default"
     #GMERGE_INTEGRATION_TEST="default"
     GMERGE_DEBUG_MODE="false"
     GMERGE_SANITIZER="none"
     GMERGE_BUILD_ONLY="false"
     GMERGE_NO_FAIL=0
   fi


   if [[ "$GMERGE_BUILD_ONLY" != "false" ]]; then
     GMERGE_UNIT_TEST="none"
     GMERGE_INTEGRATION_TEST="none" 
   fi


   if [ ! -z $GMERGE_ARGUS ] && [ ! -z $GMERGE_USER ];
   then
     # has both user name and pull request
     __send_to_jenkins $PIPELINE_NAME

   elif [ -z $GMERGE_ARGUS ] && [ ! -z $GMERGE_USER ]; then
     # merge argument is empty, set name only
     echo "Provide argements to run the job"

   else
     # user name not found
     echo -e "\033[31mInvalid argument or username not set \"$@\" !!\033[0m"
     __help $PIPELINE_SHORT_NAME
   fi
}
