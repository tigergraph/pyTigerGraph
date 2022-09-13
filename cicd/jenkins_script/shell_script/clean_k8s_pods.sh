#!/bin/bash

#This script is to release the resource of k8s test pods by pod name
set -ex

if [ $# -lt 3 ]; then
  echo -e "\nUsage: \t$0 log_dir image_version podName1 ... podName_n\n"
  exit 1
fi

#defin log path, default path is /mnt/nfs_datapool/mitLogs
log_dir=$1
image_version=$2
pods_name=${@:3}
now=$(date +"%Y-%m-%d_%H:%M:%S")
pid=$$
log=$log_dir/clean_k8s_pods_${pid}_${now}.log
touch $log
# make sure anyone can write to the log
chmod 777 $log
echo "Log file create at $now" >> $log
echo "image_version recieved: ${image_version}" >> $log
echo "pods_name recieved: ${pods_name}" >> $log

#clean pod
JENKINS_IP="mit.graphsql.com"
JENKINS_PORT="8080"
JENKINS_JOB="cleanup-k8s"
JENKINS_USER="qa_build"
JENKINS_TOKEN="11671f42b808804a46bf7b2af5b37c3cce"

curl -X POST "http://$JENKINS_IP:$JENKINS_PORT/job/$JENKINS_JOB/buildWithParameters" \
--data "POD_NAME=${pods_name}" --user $JENKINS_USER:$JENKINS_TOKEN >> $log 2>&1
