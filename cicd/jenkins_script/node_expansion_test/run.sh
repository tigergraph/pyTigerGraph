#!/bin/bash
########################################################
cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
cd $cwd

function usage() {
  echo "usage: $0 [-h] [-c num_nodes] <-n \"IP1,IP2, ... ,IPn\"> [-u sudo_user] [-p sudo_user_password]"
  echo "-h  displays this message"
  echo "-c  sepecifies number of nodes in cluster (default 2)"
  echo "-n  sepecifies the IPs of the extra nodes seperated by commas"
  echo "-u  sepecifies the sudo user (default $USER)"
  echo "-p  sepecifies the sudo user password (default: empty)"
  echo "-l  sepecifies the log to use (default: /dev/stdout)"
}

function check_if_fail() {
  local res_code="$1"
  local msg_pre="$2"

  if [[ $res_code != 0 ]]; then
    echo -e "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    echo "            ${msg_pre} failed! at $(date +'%F %T.%6N')"
    echo -e "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    exit 1
  fi
}

cluster_num=2
IPs=""
PRODUCT=~/product
TG_ROOT=~/tigergraph
SUDO_USER=$USER
SUDO_PSWD=""
LOG_DIR="/dev/stdout"
LOG_FILE=""

#read parameters
while getopts ":hc:n:u:p:l:" arg; do
  case "$arg" in
    h)
      usage
      exit 0
      ;;
    c)
      cluster_num=$OPTARG
      ;;
    n)
      IPs=$OPTARG
      ;;
    u)
      SUDO_USER=$OPTARG
      ;;
    p)
      SUDO_PSWD=$OPTARG
      ;;
    l)
      LOG_DIR=$OPTARG
      ;;
    :) 
      echo "Invalid option: $OPTARG requires an argument"
      usage
      exit 1
      ;;
    *)
      echo "Invalid option $OPTARG"
      usage
      exit 1
      ;;
  esac
done

if [[ -z $IPs ]]; then
  echo "Error: Please sepecify at least one IP for expansion with option -n"
  usage
  exit 1
fi

source ~/.bashrc || true

if [ $LOG_DIR != "/dev/stdout" ]; then
  LOG_FILE="expansion_test.log"
fi

echo Node expansion test starts &>> $LOG_DIR/$LOG_FILE
echo Nodes used for expansion: $IPs &>> $LOG_DIR/$LOG_FILE

cd $PRODUCT/gtest

# Run setup for gquery regresss 13
echo -e "\nsetup for gquery regress13 running at $(date +'%F %T.%6N')" &>> $LOG_DIR/$LOG_FILE
./resources/end2end/gquery/regress13/setup.sh &>> $LOG_DIR/$LOG_FILE
res_code=$?
check_if_fail $res_code "Setup for gquery regress13"
###################################################################

# Run node expansion test
echo -e "\nNode expansion test running at $(date +'%F %T.%6N')" &>> $LOG_DIR/$LOG_FILE

# Set up extra nodes and put IPs into format required 
# by gbar expand (i.e. m3:x.x.x.x m4:x.x.x.x ... mx:x.x.x.x) 
cd $TG_ROOT/pkg_pool/syspre_pkg
if [[ -f ~/.gsql/gsql.cfg ]]; then
  yes | ./set_syspre.sh -i $IPs -u $SUDO_USER -P $SUDO_PSWD &>> $LOG_DIR/$LOG_FILE
else
  ./set_syspre.sh -i $IPs -u $SUDO_USER -P $SUDO_PSWD &>> $LOG_DIR/$LOG_FILE
fi
expansion_ips=""
cluster_num=$((cluster_num+1))
IFS=',' read -ra ipaddr <<< "$IPs"
for i in "${ipaddr[@]}"; do
  if [[ -z $expansion_ips ]]; then
    expansion_ips="m$cluster_num:$i"
  else
    expansion_ips="$expansion_ips,m$cluster_num:$i"
  fi
  cluster_num=$((cluster_num+1)) 
done
cd -

echo "IPs passed to gbar expand function: $expansion_ips" &>> $LOG_DIR/$LOG_FILE

# Setup up backup directory for gbar expand
mkdir -p ~/node_expansion || true
cp $cwd/conf_gbar.yaml ~/.gsql/conf_gbar.yaml

# run gbar expand
gbar expand $expansion_ips -y &>> $LOG_DIR/$LOG_FILE
res_code=$?
check_if_fail $res_code "Node expansion test"

#Clean up after running node expansion
rm -rf ~/node_expansion
###################################################################

# Run regress 13
echo -e "\n$gquery regress13 running at $(date +'%F %T.%6N')" &>> $LOG_DIR/$LOG_FILE
./gtest end2end.sh gquery 13 &>> $LOG_DIR/$LOG_FILE
res_code=$?
check_if_fail $res_code "gquery regress 13"
###################################################################

