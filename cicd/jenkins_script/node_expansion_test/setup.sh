#!/bin/bash

function usage() {
  echo "usage: $0 [-h] [-u] [-o] [-t]"
  echo "-h  displays this message"
  echo "-u  specifies the installation url to use"
  echo "-o  specifies the os to use"
  echo "-l  specifies log dir"
  echo "-t  specifies the tigergraph user (default tigergraph)"
}

cd `dirname $0`
BASE_DIR=$(pwd)
URL=""
OS=""
LOG_DIR="/dev/stdout"
LOG_FILE=""
TIGER_USER="tigergraph"

#Read arguments
while getopts ":hu:o:l:t:" arg; do
  case "$arg" in
    h)
      usage
      exit 0
      ;;
    u)
      URL=$OPTARG
      ;;
    o)
      OS=$OPTARG
      ;;
    l)
      LOG_DIR=$OPTARG
      ;;
    t)
      TIGER_USER=$OPTARG
      ;;
    :)
      echo "Invalid option: $OPTARG requires an argument"
      usage
      exit 1
      ;;
    *)
      echo "Invalid option $arg"
      usage
      exit 1
      ;;
  esac
done

if [[ $EUID -ne 0 ]]; then
  echo "Sudo or root rights are requqired to run this script."
  exit 1
fi

if [ -z $URL ]; then
  echo "Error: please sepecify the installation url with the -u option"
  exit 1
fi

if [ -z $OS ]; then
  echo "Please sepecify the os with the -o option"
  exit 1
fi

if [ $LOG_DIR != "/dev/stdout" ]; then
  LOG_FILE="setup.log"
  touch $LOG_DIR/$LOG_FILE
  chmod 777 $LOG_DIR/$LOG_FILE
fi

set -x
echo "--------------Setting up cluster for expansion test--------------" &>> $LOG_DIR/$LOG_FILE
sudo -i -u $TIGER_USER bash << EOF
set -x
if [[ -d ~/.gsql ]]; then
  echo "Pervious platform detected. Uninstalling..." &>> $LOG_DIR/$LOG_FILE
  sed -i -e "s:< /dev/tty::g" ~/.gium/guninstall
  echo "y" | ~/.gium/guninstall &>> $LOG_DIR/$LOG_FILE
else
  echo "No previous platform detected. Skipping uninstall..." &>> $LOG_DIR/$LOG_FILE
fi
EOF

echo "Installing package $URL..." &>> $LOG_DIR/$LOG_FILE
cd ~
sudo rm -rf tigergraph-*
curl --fail -O $URL &>> $LOG_DIR/$LOG_FILE
tar -xzf tigergraph-*.tar.gz &>> $LOG_DIR/$LOG_FILE
cd tigergraph-*
sed -i -e 's:< /dev/tty::g' utils/input_help
sed -i -e 's:< /dev/tty::g' utils/check_utils

lic_key=$(curl --fail -L ftp://192.168.11.10/lic/license.txt)
if [ -z "$lic_key" ]; then
  echo "Download license.key failed. License key is empty."  &>> $LOG_DIR/$LOG_FILE
  exit 1
fi

# modify the config file
sed -i -e "s/\"license.key\": .*$/\"license.key\": \"$lic_key\",/g" $BASE_DIR/config_files/*config.json
config_file=cluster_config.json

# set environmental variables for setting up NTP and FIREWALL
SETUP_NTP=y
SETUP_FIREWALL=y

# run installation
if [ "$OS" = 'centos7' ]; then
  cp -f $BASE_DIR/config_files/centos7_config.json ./$config_file
  bash install.sh -u $TIGER_USER -cn &>> $LOG_DIR/$LOG_FILE
elif [ "$OS" = 'ubuntu16' ]; then
  cp -f $BASE_DIR/config_files/ubuntu16_config.json ./$config_file
  bash install.sh -u $TIGER_USER -cn &>> $LOG_DIR/$LOG_FILE
else
  echo "Unsupported OS: $OSG" &>> $LOG_DIR/$LOG_FILE
  exit 1
fi

#Check if installation was successful
if [[ $? != 0 && $? != 90 ]]; then
  echo "Setup cluster Failed" &>> $LOG_DIR/$LOG_FILE
else
  cd $BASE_DIR
  echo "--------------Finished Setting Up Cluster--------------" &>> $LOG_DIR/$LOG_FILE
fi
