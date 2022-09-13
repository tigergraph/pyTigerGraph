#!/bin/bash
#This script is to uninstall tigergraph using official uninstaller
# This script is to uninstall tigergraph using official uninstaller
##############################################
cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
set -ex

if [[ -z "$TERM" ]]; then
  export TERM="dumb"
  export tput_flag="-Txterm-256color"
fi

# get tg_user for parameter if given. This is to make sure
# uninstall is executed successfully during nodeOnline as
# script is copied to the test server and test server
# can't see config.json
if [[ $# != 0 ]]; then
  tg_user=$1
else
  tg_user=$(jq -r .test_machine_user ${cwd}/../config/config.json)
fi

# services need to be stopped before platform install
services_set=('gadmin_server' 'gdict_server' 'poc_gpe_server' 'poc_rest_server' 'poc_rest_loader' 'ids_worker' \
              '\-Dzookeeper.log.dir=.*zk/bin/../conf/zoo.cfg' '\-Dkafka.logs.dir.*kafka/bin' \
              'gsql_server' 'gsql_server.jar' 'gsql_client' 'gsql_client.jar' 'server/src/index.js' 'glive_agent.py' \
              'glive/rest-server/app.js' 'glive/rest-server/loadKafkaData.js' 'tmp_gsql.jar' \
              'nginx/sbin/nginx' 'bin/ts3agent' 'bin/ts3svc' 'gsql_admin' 'gadmin' 'gsql.jar' \
              'tg_infr_' 'tg_dbs_' 'tg_app_' 'controller')

# files that need to be deleted before platform install
files_set=(".bash_tigergraph" ".bashrc_tg" ".gium" ".gsql*" ".ssh/tigergraph_rsa.*"\
           ".syspre" ".tg.cfg" ".tigergraph*" ".venv" "tigergraph*")

#function to get service pids
get_service_pids(){
  local service_name=$1
  local server_pids=$(ps -ef | grep -v grep | grep $service_name | awk '{print $2}')
  if [ ! -z "$server_pids" ]; then
    echo "$server_pids"
  fi
}

# function to stop old services
stop_services(){
  echo "Stopping services ..."
  crontab -l | grep -v admin_crontab | crontab -
  for i in "${!services_set[@]}"; do
    local service="${services_set[$i]}"
    local server_pids=$(get_service_pids $service)
    for pid in $server_pids; do
      echo "Checking service $service..."
      if [ ! -z "$pid" ]; then
        echo "Service $service detected. Stopping.."
        sudo pkill -TERM -P $pid &>/dev/null
        sudo kill -9 $pid >/dev/null 2>&1
        # nginx master process may start new slave process, need to be killed
        sudo pkill -g $pid &>/dev/null
        echo "Service $service stopped successfully!"
      fi
    done
  done
  sleep 1
}

#function to remove all files that need to be removes
function remove_files(){
  echo "Removing files..."
  local tg_home=/home/$tg_user
  local files_set=(".bash_tigergraph" ".bashrc_tg" ".gium" ".gsql*" ".ssh/tigergraph_rsa*"\
    ".syspre" ".tg.cfg" ".tigergraph*" ".venv" "tigergraph*")

  for i in "${files_set[@]}"; do
    echo "Removing $i..."
    if [[ -n $tg_user && -n $tg_home && -n $i ]]; then
      sudo rm -rf $tg_home/$i
    fi
  done
  echo "All files removed!"
}

#function to restore bachrc to default MIT bachrc
function restore_bashrc(){
  echo "restoring bashrc..."
  if [[ -f /etc/skel/.bashrc ]]; then
    cp /etc/skel/.bashrc /home/$tg_user/.bashrc
    echo "if [ ! \`echo \$PATH | grep \"/sbin\"\` ]; then" >> /home/$tg_user/.bashrc
    echo "  PATH=\$PATH:/sbin:/usr/sbin:/usr/local/sbin" >> /home/$tg_user/.bashrc
    echo "fi" >> /home/$tg_user/.bashrc
  else
    echo "Error: /etc/skel/.bashrc not found! Restore .bashrc failed!"
    return 1
  fi
}

#function to perform manual uninstall just in case automated uninstall fails.
function manual_uninstall(){
  #kill all leftover processes used by tigergraph
  stop_services
  local stop_res=$?

  #kill all nc processes used in 3.x.x installer
  if sudo pgrep -f "^/tmp/netcat/nc -k -l"; then
    echo "Killing all netcat processes..."
    sudo pkill -9 -f "^/tmp/netcat/nc -k -l"
  else
    echo "No nc processes detected. Skipping..."
  fi
  local nc_res=$?

  #remove all files used by tigergraph if they exist
  remove_files
  local rm_res=$?

  #restore bashrc
  restore_bashrc
  local bashrc_res=$?

  #check if uninstall is successful
  if [[ $stop_res = 0 && $nc_res = 0 && $rm_res = 0 && $bashrc_res = 0 ]]; then
    echo "Manual uninstallation successful!"
    return 0
  else
    echo "Manual uninstallation failed!"
    return 1
  fi
}

#main starts here

#cleanup gtest related directories
rm -rf ${PRODUCT}/gtest/output/*
rm -rf ${PRODUCT}/gtest/diff/*
rm -rf ${PRODUCT}/gtest/.working_dir/*
rm -rf ${PRODUCT}/build
sudo rm -rf ${PRODUCT}/*-offline/
rm -rf ~/.glocal/*

#clear tmp using find to make sure it's successful even if tmp directory
#has a large number of files. This may cause rm -rf * to fail
sudo find /tmp -not -path "/tmp" -and -exec rm -rf {} + || true

#clear firewall
sudo iptables -F || true

#Get path of tigergraph uninstaller if one exists
set +e
if ls /home/graphsql/tigergraph/app/[0-9].[0-9].[0-9]/cmd/guninstall > /dev/null 2>&1; then
    uninstall_path=$(realpath $(ls /home/graphsql/tigergraph/app/[0-9].[0-9].[0-9]/cmd/guninstall))
elif [[ -f /home/$tg_user/.gium/guninstall ]]; then
  uninstall_path=/home/$tg_user/.gium/guninstall
else
  echo "Guninstall not found on machine!"
  AUTO_UNINSTALL_STATUS=1
fi

#Try auto uninstall first
if [[ -n $uninstall_path ]]; then
  echo "Uninstaller found at $uninstall_path! Doing auto uninstall!"
  sudo /bin/su -c "$uninstall_path -sy" - $tg_user
  AUTO_UNINSTALL_STATUS=$?
fi

#Try to manually uninstall if one doesn't exist
if [[ $AUTO_UNINSTALL_STATUS != 0 ]]; then
  echo "Auto install fail or uninstaller not found on machine! Doing manual uninstall!"

  for file in ${files_set[@]}; do

    #If files used by tigergraph is found, do manual uninstall
    if [ -e /home/$tg_user/$file ]; then
      manual_uninstall
      UNINSTALL_STATUS=$?
      set -e

      #Check if uninstall successful
      if [[ $UNINSTALL_STATUS = 0 ]]; then
        echo "Manual uninstall successful!"
      else
        echo "Manual uninstall failed!"
      fi
      exit $UNINSTALL_STATUS
    fi
  done
fi

#If no files used by tigergraph exists. Skip uninstall and exit 0
echo "No directories or files used by tiergraph is found. Environment is clean. Skipping uninstall..."
set -e
exit 0
