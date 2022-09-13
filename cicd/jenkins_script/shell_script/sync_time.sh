#!/bin/bash

function get_os(){
  if [ -f "/etc/apt/sources.list" ]; then
    if [ -f "/etc/lsb-release" ]; then
      os_version=$(cat /etc/lsb-release | grep  "DISTRIB_RELEASE" | cut -d= -f2)
      echo "UBUNTU $os_version"
    elif [ -f "/etc/os-release" ]; then
      os_version=$(cat /etc/os-release | grep  "VERSION_ID" | cut -d= -f2)
      os_version=${os_version//\"}  # remove all double quotes
      echo "DEBIAN $os_version"
    fi
  elif [ -d "/etc/yum.repos.d" ]; then
    os_version="$(cat /etc/system-release | grep -o '[0-9]\.[0-9]')"
    echo "RHEL $os_version"
  else
    echo "UNKOWN"
  fi
}

timestamp() {
  date +"%D %T"
}

function do_time_sync(){
  if [[ -z $2 ]]; then
    echo "$(timestamp): Error: unsupported OS"
    echo "$(timestamp): Supported OSs: Centos 6 7, RHEL 6 7, Ubuntu 14 16"
    exit 1
  elif [[ $1 = "RHEL" ]]; then
    if [[ "$2" > "6" && "$2" < "7" ]]; then
      #centos 6:
      echo "$(timestamp): Syncing time for RHEL/Centos 6..."
      echo "$(timestamp): Checking that ntpd is enabled..."
      sudo /sbin/chkconfig ntpd on
      echo "$(timestamp): Stopping ntpd service..."
      sudo /sbin/service ntpd stop
      echo "$(timestamp): Syncing time with master node..."
      sudo /usr/sbin/ntpd -gq
      echo "$(timestamp): Restarting ntpd service..."
      sudo /sbin/service ntpd start
    elif [[ "$2" > "7" && "$2" < "8" ]]; then
      #centos 7:
      echo "$(timestamp): Syncing time for RHEL/Centos 7..."
      echo "$(timestamp): Checking that ntpd is enabled..."
      sudo /usr/bin/timedatectl set-ntp 1
      echo "$(timestamp): Stopping ntpd service..."
      sudo /bin/systemctl stop ntpd
      echo "$(timestamp): Syncing time with master node..."
      sudo /sbin/ntpd -gq
      echo "$(timestamp): Restarting ntpd service..."
      sudo /bin/systemctl start ntpd
    else
      echo "$(timestamp): Error: unsupported RHEL/Centos version..."
      exit 2
    fi
  elif [[ "$1" = "UBUNTU" ]]; then
    if [[ "$2" > "14" && "$2" < "15" ]]; then
      #ubuntu 14:
      echo "$(timestamp): Syncing time for Ubuntu 14..."
      echo "$(timestamp): Stopping ntpd service..."
      sudo service ntp stop
      echo "$(timestamp): Syncing time with master node..."
      sudo ntpd -gq
      echo "$(timestamp): Restarting ntpd service..."
      sudo service ntp start
    elif [[ "$1" = "UBUNTU" && "$2" > "16" && "$2" < "17" ]]; then
      #ubuntu 16:
      echo "$(timestamp): Syncing time for Ubuntu 16..."
      sudo apt-get install ntpdate
      echo "$(timestamp): Stopping ntpd service..."
      sudo service ntp stop
      echo "$(timestamp): Syncing time with master node..."
      sudo ntpd -gq
      echo "$(timestamp): Restarting ntpd service..."
      sudo service ntp start
    else
      echo "$(timestamp): Error: unsupported Ubuntu version..."
      exit 3
    fi
  else
    echo "$(timestamp): Error: Unsupported OS Version..."
    echo "$(timestamp): Supported OSs: Centos 6 7, RHEL 6 7, Ubuntu 14 16"
    exit 4
 fi
}


OSG=`get_os`
echo "$(timestamp): OS obtained: $OSG"
OS=`echo "$OSG" | cut -d ' ' -f 1`
version=`echo "$OSG" | cut -d ' ' -f 2`
echo "$(timestamp): Starting time sync..." 
do_time_sync $OS $version


