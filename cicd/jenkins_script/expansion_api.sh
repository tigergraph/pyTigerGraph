#!/bin/bash
set -e

export EXPANSION_LOGS="/tmp/expansion"

function expand_cluster() {
  if [ -n $1 ]; then
    python3 $PYTHON3_SCRIPT_FOLDER/create_k8s_pods.py --num $1
  else
    echo "Please specify the node num for expansion..."
    exit 5 
  fi
}

function install_tg() {
  if [[ -n "$1" ]]; then
    IFS=',' read -a ip_list <<< "$1"
  else
    echo "Please input the cluster ip address list for installation..."
    exit 5 
  fi
  if [[ -n "$2" ]]; then
    EXPANSION_LOGS=$2
  fi

  function parse_ip() {
    printf "["
    for i in "${!ip_list[@]}"; do
      if [ $((i+1)) == "${#ip_list[@]}" ]; then
        printf "\"m%s: %s\"" "$(($i+1))" "${ip_list[$i]}"
      else
        printf "\"m%s: %s\"," "$(($i+1))" "${ip_list[$i]}"
      fi
    done
    printf "]" 
  }
  ## bash $SHELL_SCRIPT_FOLDER/install_pkg.sh /mnt/nfs_datapool/mitLogs/wip_test_8649/test_job_57879_10.244.1.182/mit_log/install_config.json 3.2.0 cluster
 
  NodeList=$(parse_ip)
  if [ ! -d ${PRODUCT}/*-offline ]; then
    echo "Tigergraph install package doesn't exist..."
    exit 20
  fi
  set +e
  date
  cd ${PRODUCT}/*-offline 
  [ -f /tmp/tmp_install_conf.json ] || touch /tmp/tmp_install_conf.json
  sed -i 's/"P": "sudoUserPassword",\?/"P": "graphsql"/1' ./install_conf.json
  cat install_conf.json |jq ".BasicConfig.NodeList = $NodeList" > /tmp/tmp_install_conf.json
  cp -f /tmp/tmp_install_conf.json ./install_conf.json
  ./install.sh -n
  INSTALL_STATUS=$?
  if [ -n $EXPANSION_LOGS ]; then
    [ -d $EXPANSION_LOGS ] || mkdir -p $EXPANSION_LOGS
    cp -r ./logs $EXPANSION_LOGS
    env > $EXPANSION_LOGS/env.log
    cp /tmp/tmp_install_conf.json $EXPANSION_LOGS
  fi
  [ "$INSTALL_STATUS" != 0 ] && exit $INSTALL_STATUS
  date
}

function shrink_cluster() {
  if [ -n "$1" ]; then
    python3 $PYTHON3_SCRIPT_FOLDER/cluster_shrink.py --name "$1"
  else
    echo "Please input the node name list for shrinking..."
    exit 5
  fi
}

export -f expand_cluster install_tg shrink_cluster