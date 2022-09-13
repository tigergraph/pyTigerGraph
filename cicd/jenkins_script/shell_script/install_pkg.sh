#!/bin/bash
# This script is to install tigergraph using official installer
##############################################
cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
set -ex
source $cwd/../util.sh
date

if [[ -z "$TERM" ]]; then
  export TERM="dumb"
  export tput_flag="-Txterm-256color"
fi

install_conf=$1
tg_version=$2
version=$(echo $2 | cut -d "." -f 1) #get major version only
mode=$3 #need for versions prior to 3.0.0

if [ ! -f ${PRODUCT}/tigergraph-offline.tar.gz ]; then
  echo "${PRODUCT}/tigergraph-offline.tar.gz doesn't exist"
  exit 1
fi

if [ ! -f "$install_conf" ]; then
  echo "Install config file $install_conf doesn't exist"
  exit 1
fi

if [[ $version -lt 3 && $mode != "cluster" && $mode != "single" ]]; then
  echo "Mode must be single or cluster for versions prior to 3.0.0"
  exit 1
fi

install_log_root="${install_conf%%mit_log*}/install_logs"
mkdir -p $install_log_root
ulimit -n

date
if [[ $version -ge 3 ]]; then
  #3.x.x and later installation
  TG_APP_ROOT=$(jq -r '.["BasicConfig"]["RootDir"]["AppRoot"]' $install_conf)
  TG_LOG_ROOT=$(jq -r '.["BasicConfig"]["RootDir"]["LogRoot"]' $install_conf)
  echo "export PATH=${TG_APP_ROOT}/cmd:\$PATH" > ~/.bashrc_tg
  source ~/.bashrc_tg

  #install tigergraph
  cd ${PRODUCT} && tar xzf tigergraph-offline.tar.gz
  export UNINSTALL_PLATFORM=1
  date
  set +e
  cd *-offline/ && cp -f $install_conf ./install_conf.json && ./install.sh -n
  INSTALL_STATUS=$?
  date

  JAVA_BINS=$(find ${TG_APP_ROOT}/*/.syspre -name java -type f)
  JAVA_BINS_TMP=$(find "$JAVA_BINS" | grep jre)
  if [ $? -eq 0 ]; then
    JAVA_BINS="$JAVA_BINS_TMP"
  fi
  JAVA_PATH=$(find "$JAVA_BINS" | head -1)
  echo "export PATH=$(dirname $JAVA_PATH):\$PATH" >> ~/.bashrc_tg
  source ~/.bashrc_tg

  cp -rf utils/user_config $install_log_root
  cp -rf logs $install_log_root
  env > $install_log_root/env.log
else
  #2.x.x and earlier installation
  #install tigergraph
  cd ${PRODUCT} && tar xzf tigergraph-offline.tar.gz
  date
  set +e
  cd *-offline/
  cp -f $install_conf ./cluster_config.json
  if [[ $mode = "cluster" ]]; then
    export UNINSTALL_PLATFORM=true
    ./install.sh -cn | tee -a $install_log_root/mit_install.log
  else
    license=$(jq -r '.["license.key"]' cluster_config.json)
    tg_user=$(jq -r '.["tigergraph.user.name"]' cluster_config.json)
    tg_password=$(jq -r '.["tigergraph.user.password"]' cluster_config.json)
    tg_root_dir=$(jq -r '.["tigergraph.root.dir"]' cluster_config.json)
    # sudo does not pass env variables
    sudo UNINSTALL_PLATFORM=true ./install.sh -sn -u $tg_user -p $tg_password -r $tg_root_dir -l $license | tee -a $install_log_root/mit_install.log
  fi
  INSTALL_STATUS=$?
  date
fi

#Check if install successful
# cd - && rm -f tigergraph-offline.tar.gz && sudo rm -rf *-offline/ && sudo rm -f tigergraph.bin || :
cd - && rm -f tigergraph-offline.tar.gz || :
set -e
[ "$INSTALL_STATUS" != 0 ] && exit $INSTALL_STATUS
date

#Set options
if [[ $version -ge 3 ]]; then

  if [ "$SANITIZER" == "asan" ]; then
    if cat /etc/os-release | grep Ubuntu 2>&1 >/dev/null; then
      ASAN_LIB="${TG_APP_ROOT}/${tg_version}/.syspre/usr/lib/x86_64-linux-gnu/libasan.so.5"
    else
      ASAN_LIB="${TG_APP_ROOT}/${tg_version}/.syspre/usr/lib64/libasan.so.5"
    fi
    echo "export ASAN_LIB=${ASAN_LIB}" >> ~/.bashrc_tg

    gadmin config set GPE.BasicConfig.Env "LD_PRELOAD=$ASAN_LIB:\$LD_PRELOAD; LD_LIBRARY_PATH=\$LD_LIBRARY_PATH; CPUPROFILE=/tmp/tg_cpu_profiler; CPUPROFILESIGNAL=12; MALLOC_CONF=prof:true,prof_active:false;"
    gadmin config set GSE.BasicConfig.Env "LD_PRELOAD=$ASAN_LIB:\$LD_PRELOAD; LD_LIBRARY_PATH=\$LD_LIBRARY_PATH;"
    gadmin config set RESTPP.BasicConfig.Env "LD_PRELOAD=$ASAN_LIB:\$LD_PRELOAD; LD_LIBRARY_PATH=\$LD_LIBRARY_PATH;"
    gadmin config set GSQL.BasicConfig.Env "LD_PRELOAD=$ASAN_LIB:\$LD_PRELOAD; CPATH=\$CPATH; LD_LIBRARY_PATH=\$LD_LIBRARY_PATH;"
    gadmin config set Admin.BasicConfig.Env "LD_PRELOAD=$ASAN_LIB:\$LD_PRELOAD"
    gadmin config set Dict.BasicConfig.Env "LD_PRELOAD=$ASAN_LIB:\$LD_PRELOAD"
    #w/a for CORE-1432
    gadmin_config_append GPE.BasicConfig.Env "ASAN_OPTIONS=alloc_dealloc_mismatch=0;"
    gadmin_config_append GSE.BasicConfig.Env "ASAN_OPTIONS=alloc_dealloc_mismatch=0;"
    gadmin_config_append RESTPP.BasicConfig.Env "ASAN_OPTIONS=alloc_dealloc_mismatch=0;"
  fi

  #force debug mode
  export DEBUG=true
  #turn on debug log for GPE and GSE and RESTPP
  if [ "$DEBUG" = "true" ]; then
    gadmin_config_append GPE.BasicConfig.Env GInfo_v=0xaaaa
    gadmin_config_append GSE.BasicConfig.Env GInfo_v=0xaaaa
    gadmin_config_append RESTPP.BasicConfig.Env GInfo_v=0xaaaa
    gadmin config set GPE.BasicConfig.LogConfig.LogLevel DEBUG
    gadmin config set GSE.BasicConfig.LogConfig.LogLevel DEBUG
    gadmin config set RESTPP.BasicConfig.LogConfig.LogLevel DEBUG
  fi
  gadmin_config_append GPE.BasicConfig.Env "GCOUT_SizeLimit=100;"
  gadmin_config_append GSE.BasicConfig.Env "GCOUT_SizeLimit=100;"
  gadmin_config_append GSQL.BasicConfig.Env "GCOUT_SizeLimit=100;"
  gadmin_config_append RESTPP.BasicConfig.Env "GCOUT_SizeLimit=100;"
  gadmin config set Controller.BasicConfig.LogConfig.LogLevel DEBUG
  gadmin config set Executor.BasicConfig.Env NO_LOG_FSYNC=true
  gadmin config set RESTPP.Factory.DefaultQueryTimeoutSec 1800
  gadmin config set GSQL.BasicConfig.LogConfig.LogLevel DEBUG
  gadmin config set GSQL.BasicConfig.LogConfig.LogRotationFileNumber 500
  gadmin config set GPE.BasicConfig.LogConfig.LogRotationFileNumber 500
  gadmin config set GSE.BasicConfig.LogConfig.LogRotationFileNumber 500
  gadmin config set RESTPP.BasicConfig.LogConfig.LogRotationFileNumber 500
  gadmin config set Gadmin.StartStopRequestTimeoutMS 300000
  if [[ ! $tg_version =~ ^3\.0\. ]]; then
    #default value start from 3.1.1 is 6000/5
    #gadmin config set Controller.LeaderElectionHeartBeatIntervalMS 2000
    #gadmin config set Controller.LeaderElectionHeartBeatMaxMiss 4
    gadmin config set GSQL.EnableStringCompress true
  fi

  gadmin config apply -y
  gadmin restart -y
else
  echo "export PATH=\$PATH:~/.gium/" > ~/.bashrc_tg
  source ~/.bashrc_tg
  #turn on debug log for GPE and GSE and RESTPP
  GSQL_CONFIG=$(echo $(gadmin --dump-config | grep gdev.path | cut -d' ' -f2)/gdk/gsql/config)
  grun_p all "sed -i 's/GPE: \"\(.*\)\"/GPE: \"GInfo_v=0xaaaa \1\"/g' /home/tigergraph/.gsql/fab_dir/configs/runtime_config.yaml"
  grun_p all "sed -i 's/GSE: \"\(.*\)\"/GSE: \"GInfo_v=0xaaaa \1\"/g' /home/tigergraph/.gsql/fab_dir/configs/runtime_config.yaml"
  grun_p all "sed -i 's/RESTPP: \"\(.*\)\"/RESTPP: \"GInfo_v=0xaaaa \1\"/g' /home/tigergraph/.gsql/fab_dir/configs/runtime_config.yaml"
  grun_p m1 "sed -i '$ a RefreshRestpp=10' $GSQL_CONFIG"

  ~/.gium/gadmin config-apply -v
fi
