#!/bin/bash

# Script to check the availability of build essential dependencies before actually running build job

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PYTHON3_SCRIPT_FOLDER="${DIR}/../python3_script"
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'
#Github Actions id dict, realtime actions is not in use
declare -A repo_actions_dict
repo_actions_dict['engine']='14329404'
repo_actions_dict['realtime']='14364244'

if [ $# -lt 6 ]; then
  echo -e "\nUsage: \t$0 github_log rest_server_address mit_server_address user_notified mark_tag_name \n"
  exit 1
fi
github_log=$1
rest_server_address=$2
mit_server_address=$3
user_notified=$4
build_url=$5
mark_tag_name=$6

yell() { echo "$0: $*" >&2; }
die() { yell "$*"; exit 111; }
try() { "$@" || die "cannot $*"; }

# Retry a command a configurable number of times with exponential backoff.
# The retry count is given by ATTEMPTS (default 5), the initial backoff
# timeout is given by TIMEOUT in seconds (default 1).
# Successive backoffs double the timeout.
backoff() {
  local max_attempts=${ATTEMPTS-5}
  local timeout=${TIMEOUT-30}
  local attempt=1

  while ! "$@"; do
    if (( attempt < max_attempts )); then
      yell "$attempt failure(s), retrying in $timeout second(s)..."
      sleep "$timeout"
      attempt=$(( attempt + 1 ))
      timeout=$(( timeout * 2 ))
    else
      die "cannot $* after $max_attempts attempts"
    fi
  done
}

# function to check the health of mit database and restful api
check_api_status() {
    local url=$1
    local status
    status=$(backoff curl -sfI "$url" | grep 200 -c)
    if [[ $status -eq 0 ]]; then
      echo -e "\\n${RED}ERROR:${NC}"
      echo -e " - API $url seemed unheathy, please try again later..."
      notification "FAIL" "API $url seemed unheathy, please check..."
      exit 1
    else
      echo -e "- Heath check passed for API $url "
    fi
}
#fucntion to list aws s3 bucket files
list_s3_bucket() {
  local bucket=$1
  aws s3 ls $bucket
}

# function to check the package availability in aws bucket
check_s3_bucket() {
    local bucket_path=$1
    local status
    local err_message
    status=$(backoff list_s3_bucket $bucket_path|wc -l)
    if [[ $status -eq 0 ]]; then
      echo -e "\\n${RED}ERROR:${NC}"
      echo -e " - Could not find the package in S3 bucket $bucket_path ..."
      if [[ "${bucket_path}" =~ "kafka" ]]; then
        err_message="Could not find kafka-plugins package in S3 bucket $bucket_path, please check with qe team"
      else
        err_message="Could not find GUI package in S3 bucket $bucket_path..."
      fi
      notification "FAIL" "${err_message}"
      exit 1
    else
      echo -e "- Heath check passed for package $bucket_path in S3"
    fi
}
# function to trigger actions and check runs status
trigger_github_actions() {
  local repo="$1"
  local workflow_id=${repo_actions_dict[$repo]}
  if [[ $workflow_id == "" ]]; then
    echo -e "\\n${RED}ERROR:${NC}"
    echo -e " - Failed to retrive the github actions workflow ID for repo ${repo}..."
    exit 1
  fi

  # trigger github actions
  local actions_api_url="https://api.github.com/repos/tigergraph/${repo}/actions"
  local actions_url="https://github.com/tigergraph/${repo}/actions/runs"
  try curl -s -X POST -H "Accept: application/vnd.github.v3+json" -H "Authorization: token ${MIT_GIT_TOKEN}" \
    "${actions_api_url}/workflows/${workflow_id}/dispatches" -d "{\"ref\":\"${mark_tag_name}\"}"
  sleep 10
  
  # retrive the runs job id
  local runs_id=$(curl -s -X GET -H "Accept: application/vnd.github.v3+json" -H "Authorization: token ${MIT_GIT_TOKEN}" \
    "${actions_api_url}/runs?branch=${mark_tag_name}"|jq -r ".workflow_runs[0].id")
  notification "START" "Github Actions of ${repo} repo for tag ${mark_tag_name} is running as ${actions_url}/${runs_id}"
  
  # wait for the runs job to be completed
  local runs_status=""
  local attempt=0
  local fail_status="timed_out failure cancelled"
  while [ "$attempt" -le "20" ]; do
    runs_status=$(curl -s -X GET -H "Accept: application/vnd.github.v3+json" -H "Authorization: token ${MIT_GIT_TOKEN}" \
    "${actions_api_url}/runs?branch=${mark_tag_name}"|jq -r ".workflow_runs[0].status")
    if [[ "$runs_status" == "completed" ]]; then
      echo -e "- GitHub Actions of ${repo} repo for tag ${mark_tag_name} is completed, job url is ${actions_url}/${job_id}"
      notification "PASS" "GitHub Actions of ${repo} repo for tag ${mark_tag_name} is completed, job url is ${actions_url}/${job_id}"
      break
    fi
    if [[ "$fail_status" =~ "$runs_status" ]]; then
      echo -e "\\n${RED}ERROR:${NC}"
      echo -e " - GitHub Actions of ${repo} repo for tag ${mark_tag_name} was failed or timed out, please further check ${actions_url}/${job_id} "
      notification "FAIL" "GitHub Actions of ${repo} repo for tag ${mark_tag_name} was failed or timed out, please further check ${actions_url}/${job_id} "
      exit 1
    fi
    sleep 30
    attempt=$(( attempt + 1 ))
  done
}
# function to get commit of repo from github gworkspace log
get_commit_from_log() {
  local repo="$1"
  local github_log="$2"
  local commits
  local commit
  commits=$(cat "${github_log}" | grep "Fetched commits for branches" | tail -1)
  commit=$(echo "${commits}"|sed "s/.*${repo}': .\?'\(.[a-z0-9]*\)'.*/\1/")
  if [ "$commit" = "" ]; then
    echo -e "\\n${RED}ERROR:${NC}"
    echo -e "  - Commit not find from github log $github_log"
    exit 1
  else
    echo "$commit"
  fi
}
# function to send fail notification to zulip channel
notification() {
  local status="$1"
  local message="$2"
  notification_message=$(cat <<-END
    {
      "url": "${BUILD_URL}", 
      "name": "Pre-build Check",
      "Comment": "${message}"
    }
END
)
  python3 ${PYTHON3_SCRIPT_FOLDER}/notification.py '' "$status" "${user_notified}" 'TigerGraph Testing Status' 'Pre-build Check' "${notification_message}"
}

if [ ! -f $github_log ]; then
  yell "$github_log not found, bypass prebuild health check..."
  exit 0
fi

# check mit restful api
echo -e "\\n${GREEN}Step 1/4 : Heath check for MIT Restful API ${NC}"
rest_api_url="http://${rest_server_address}/api/version"
check_api_status $rest_api_url

# check mit TG database api
echo -e "\\n${GREEN}Step 2/4 : Heath check for MIT Database API ${NC}"
mit_db_api_url="http://${mit_server_address//:9000/}:14240"
check_api_status $mit_db_api_url

# check gui tarball
echo -e "\\n${GREEN}Step 3/4 : Heath check for gui tarball ${NC}"
gui_bucket="tigergraph-build-artifacts"
repo_pr_commit=$(cat "${github_log}" | grep "Fetched commits for branches" | tail -1)
exTools=$(echo $repo_pr_commit | grep "tools")
if [[ "$exTools" == "" ]]; then
  gap_id=$(get_commit_from_log "gap" $github_log)
  check_s3_bucket "$gui_bucket/tigergraph/gap/${gap_id}/release/gap.tar.gz"
  gst_id=$(get_commit_from_log "gst" $github_log)
  check_s3_bucket "$gui_bucket/tigergraph/gst/${gst_id}/release/gst.tar.gz"
else
  tools_id=$(get_commit_from_log "tools" $github_log)
  check_s3_bucket "$gui_bucket/tigergraph/tools/${tools_id}/release/gap.tar.gz"
  check_s3_bucket "$gui_bucket/tigergraph/tools/${tools_id}/release/gst.tar.gz"
  check_s3_bucket "$gui_bucket/tigergraph/tools/${tools_id}/release/family_of_tools.tar.gz"
  check_s3_bucket "$gui_bucket/tigergraph/tools/${tools_id}/release/gshell.tar.gz"
  check_s3_bucket "$gui_bucket/tigergraph/tools/${tools_id}/release/insights.tar.gz"
  check_s3_bucket "$gui_bucket/tigergraph/tools/${tools_id}/release/graphql.tar.gz"
fi

gus_id=$(get_commit_from_log "gus" $github_log)
check_s3_bucket "$gui_bucket/tigergraph/gus/${gus_id}/release/tg_app_guid"


# check kafka
echo -e "\\n${GREEN}Step 4/4 : Heath check for kafka plugins pkg ${NC}"
kafak_bucket="tigergraph-kafka-prebuild-package"
[[  $(grep "engine" $github_log) ]] && kafak_repo="engine" || kafak_repo="realtime"
kafka_plugins_id=$(get_commit_from_log $kafak_repo $github_log)
id=${kafka_plugins_id:0:7}
# for realtime repo, the kafka plugins will be built in mit pipeline, not actions 
bucket_path=$kafak_bucket/kafka-plugins-${id}.tgz
if [[ "$(list_s3_bucket $bucket_path | wc -l)" == "0" && "$kafak_repo" == "engine" ]]; then
  echo -e "\\n${YELLOW}Warning:${NC}"
  echo -e " - Could not find kafka-plugins package in S3 bucket $bucket_path, trying to trigger github actions with tag ${mark_tag_name}"
  trigger_github_actions $kafak_repo
else
  check_s3_bucket "$kafak_bucket/kafka-plugins-${id}.tgz"
fi
