#!/bin/bash
# folders and files under home directory that need to be removed. 
declare -a to_remove=('.bash_tigergraph' '.gium' '.gsql' '.syspre' '.venv' '.gsql_fcgi' 'tigergraph')

# services need to be stopped before uninstall platform in case of license expired
declare -a services_set=('gadmin_server' 'gdict_server' 'poc_gpe_server' 'poc_rest_server' 'poc_rest_loader' 'ids_worker' \
              '\-Dzookeeper.log.dir=.*zk/bin/../conf/zoo.cfg' '\-Dkafka.logs.dir.*kafka/bin' \
              'gsql_server' 'gsql_server.jar' 'gsql_client' 'gsql_client.jar' 'server/src/index.js' 'glive_agent.py' \
              'glive/rest-server/app.js' 'glive/rest-server/loadKafkaData.js' 'tmp_gsql.jar' \
              'bin/nginx/sbin/nginx' 'bin/ts3agent' 'bin/ts3svc' 'gsql_admin' 'gadmin' 'gsql.jar' \
              'tg_infr_' 'tg_dbs_' 'tg_app_')

# function to get service pid
# param: service string
# return: service pid
get_service_pids(){
  service=$1
  server_pids=$(ps -ef | grep -v grep | grep $service | awk '{print $2}')
  if [ ! -z "$server_pids" ]; then
    echo "$server_pids"
  fi
}

# function to stop old services
# param: NONE
# step 1, stop all services by gadmin
# step 2, remove admin_server cron job
# step 3, kill services in case of step 1 failed (license expired, user removed scripts)
stop_services(){
  echo "Stopping services ..."
  ~/.gium/tg_crontab -l | grep -v admin_crontab | ~/.gium/tg_crontab -;
  ~/.gium/gadmin stop admin -y >/dev/null 2>&1
  ~/.gium/gadmin stop -y >/dev/null 2>&1
  for i in "${!services_set[@]}"; do
    service="${services_set[$i]}"
    server_pids=$(get_service_pids $service)
    for pid in $server_pids; do
      if [ ! -z "$pid" ]; then
        sudo pkill -TERM -P $pid &>/dev/null
        sudo kill -9 $pid >/dev/null 2>&1
        # nginx master process may start new slave process, need to be killed
        sudo pkill -g $pid &>/dev/null
      fi
    done
  done
  sleep 1
}

#stops all services and removes tigergraph
remove_tigergraph(){
  stop_services
  echo "Removing files from ~ directory..."
  for i in "${to_remove[@]}"
  do
    echo "Removing file $i..."
    rm -rf $i
  done

  echo "Clearing /tmp directory..."
  sudo rm -rf /tmp/*
  echo "Finished removing tigergraph from MIT machine..."
}

remove_tigergraph
