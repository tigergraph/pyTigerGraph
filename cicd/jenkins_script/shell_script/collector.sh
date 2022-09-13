#!/bin/bash
# This script is a post-testing log collector.
# Arguments: log_dir
# We will collect following items:
#    1. ~/tigergraph/logs
#    2. ~/tigergraph/zk/zookeeper.out*
#    3. ~/tigergraph/zk/data_dir
#    4. ~/tigergraph/zk/conf
#    5. ~/tigergraph/kafka/kafka.out*
#    6. ~/tigergraph/dev/gdk/gsql/output/
#    7. GSQL_LOG
#    8. gadmin status -v > $log_dir/service_status
#    9. IUM version and ~/.gium
#    10. gadmin fab log (~/.gsql/fab_dir/cmd_logs)
#    11. ~/.gsql/
#    12. ~/tigergraph/dev/gdk/gsdk/allVERSIONS.txt
#    13. gtest/output
#    14. gtest/base_line
#    15. gtest/diff
#    16. tsar info
#    17. /var/log/message or syslog, df, dmesg
#    18. Add ut logs
##############################################
cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
source $cwd/../util.sh
set -x

if [ -z $NO_COLLECTION ]; then
  exit 0
fi

# validate
if [ $# -ne 1 ] || [ ! -d $1 ];then
  echo "Usage: $0 LOG_DIR"
  echo "LOG_DIR must be an existing directory"
  exit 1
fi

log_dir=$1
log_dir=$(cd $log_dir && pwd)

function copy_gtest () {
  local source_log_folder=$1
  local target_log_folder=$2
  if [[ ! -d "$source_log_folder" ]]; then
    return
  fi
  mkdir -p $target_log_folder
  find ${source_log_folder}/{output,base_line,diff} -type d -empty -delete
  cp -RLp ${source_log_folder}/output    $target_log_folder 2>/dev/null
  cp -RLp ${source_log_folder}/base_line $target_log_folder 2>/dev/null
  cp -RLp ${source_log_folder}/diff      $target_log_folder 2>/dev/null
  #Do not follow symbolic links when copying .working_dir or MIT take forever here
  cp -Rp ${source_log_folder}/.working_dir $target_log_folder/working_dir 2>/dev/null
}

# collect environment
env > ${log_dir}/env_variables.log

# collect info of each machine in cluster
grun all """
  machine_name=\$(cat ~/.machine_name || echo "")
  [ -z \$machine_name ] && machine_name=\$(hostname -I | cut -d' ' -f1)
  machine_log=${log_dir}/\$machine_name
  mkdir -p \${machine_log}

  # copy ~/tigergraph/logs/
  cp -RLp ${LOG_ROOT} \${machine_log}/

  if [[ $MAJOR_VERSION -ge 3 ]]; then
    #copy addtional logs for 3.x.x or above
    cd ${LOG_ROOT/%log/data} && cp -rf \${ls | grep -v gstore} \${machine_log}/ && cd -
    cp -RLp ${CONFIG_DIR} \${machine_log}/
  else
    #copy zk logs for 2.x.x or below
    mkdir \${machine_log}/zk
    cp -Lp ${PROJECT_ROOT}/zk/zookeeper.out* \${machine_log}/zk/
    cp -RLp ${PROJECT_ROOT}/zk/data_dir   \${machine_log}/zk/
    cp -RLp ${PROJECT_ROOT}/zk/conf       \${machine_log}/zk/

    # copy kafka logs for 2.x.x
    mkdir \${machine_log}/kafka
    cp -Lp ${PROJECT_ROOT}/kafka/kafka.out* \${machine_log}/kafka/
  fi

  # collect system info
  sys_dir=\${machine_log}/sys
  mkdir -p \${sys_dir}
  # tsar info
  tsar --me -i 1 -n 1 &> \${sys_dir}/tsar_me.out
  tsar --io -i 1 -n 1 &> \${sys_dir}/tsar_io.out
  # system message/log
  dmesg -T &> \${sys_dir}/dmesg.out
  df -h &> \${sys_dir}/disk_info
  # this requires sudo permission
  sudo cp -p /var/log/messages \${sys_dir} 2>/dev/null && sudo chown \$(whoami):\$(whoami) \${sys_dir}/messages
  sudo cp -p /var/log/syslog   \${sys_dir} 2>/dev/null && sudo chown \$(whoami):\$(whoami) \${sys_dir}/syslog

  # change permission
  sudo chmod -R 777 \${machine_log}
"""


# keep service status
gadmin status -v &> ${log_dir}/service_status || true

# copy GSQL log
mkdir ${log_dir}/gsql_log
cp -Lp ${GSQL_PATH}/logs/GSQL_LOG* ${log_dir}/gsql_log/ 2>/dev/null || true
cp -RLp ${GSQL_PATH}/output      ${log_dir}/gsql_log/ 2>/dev/null || true
#cp -RLp ${GSQL_PATH}/.tmp         ${log_dir}/gsql_log/tmp 2>/dev/null

#####################################################################
#### collect default expansion logs
if [ -d $EXPANSION_LOGS ]; then 
  cp /tmp/logs/k8s.log $EXPANSION_LOGS
  cp -r $EXPANSION_LOGS ${log_dir}
fi
#####################################################################
#### collect unit test logs
ut_log=${log_dir}/unit_test_logs
mkdir -p ${ut_log}

if [ -d ${PRODUCT}/src/engine ]; then
  # blue_feature ut log
  copy_gtest ${PRODUCT}/src/engine/blue/features/gtest ${ut_log}/blue_feature || true
  
  # rest ut
  cp -RLp ${PRODUCT}/src/engine/realtime/integrationtest/*.log ${ut_log}/realtime || true
  copy_gtest ${PRODUCT}/src/engine/realtime/integrationtest/gtest ${ut_log}/realtime || true
else
  # blue_feature ut log
  copy_gtest ${PRODUCT}/src/blue/features/gtest ${ut_log}/blue_feature || true
  
  # rest ut
  cp -RLp ${PRODUCT}/src/realtime/integrationtest/*.log ${ut_log}/realtime || true
  copy_gtest ${PRODUCT}/src/engine/realtime/integrationtest/gtest ${ut_log}/realtime || true
fi
cp -RLp /tmp/${USER} ${ut_log}/realtime || true

# ium ut log
mkdir -p ${ut_log}/gium
copy_gtest ${PRODUCT}/src/gium/gtest ${ut_log}/gium/gtest || true
cp -rf /tmp/ium_test ${ut_log}/gium || true

# gui log
mkdir -p ${ut_log}/gui
# cp -RLp ${PRODUCT}/src/vis/gap/reports ${ut_log}/gui/gap || true
# cp -RLp ${PRODUCT}/src/vis/gst/reports ${ut_log}/gui/gst || true
if [[ -d "$PRODUCT/src/vis/tools/apps" ]]; then
  cp -RLp ${PRODUCT}/src/vis/tools/apps/gap/reports ${ut_log}/gui/gap || true
  cp -RLp ${PRODUCT}/src/vis/tools/apps/gst/reports ${ut_log}/gui/gst || true
else
  cp -RLp ${PRODUCT}/src/vis/gap/reports ${ut_log}/gui/gap || true
  cp -RLp ${PRODUCT}/src/vis/gst/reports ${ut_log}/gui/gst || true
fi
#####################################################################
#### collect integration test logs
it_log=${log_dir}/integration_test_logs
mkdir -p ${it_log}

# copy gtest output
if [ -d ${PRODUCT}/gtest ]; then
  copy_gtest ${PRODUCT}/gtest ${it_log}/gle/gtest || true
else
  copy_gtest ${PRODUCT}/src/gle/regress ${it_log}/gle/gtest || true
fi

# black_box it log
copy_gtest ${PRODUCT}/bigtest/tests/gtest ${it_log}/bigtest || true
copy_gtest ${PRODUCT}/bigtest/tests/gpr_test ${it_log}/bigtest/gpr_test || true

# document it log
copy_gtest ${PRODUCT}/src/document/gtest ${it_log}/document || true

# cqrs it (e2e) log
copy_gtest ${PRODUCT}/src/cqrs/e2e ${it_log}/cqrs || true

# gui log
mkdir -p ${it_log}/gui
if [[ -d "$PRODUCT/src/vis/tools/apps" ]]; then
  cp -RLp ${PRODUCT}/src/vis/tools/apps/gap/reports ${it_log}/gui/gap || true
  cp -RLp ${PRODUCT}/src/vis/tools/apps/gst/reports ${it_log}/gui/gst || true
else
  cp -RLp ${PRODUCT}/src/vis/gap/reports ${it_log}/gui/gap || true
  cp -RLp ${PRODUCT}/src/vis/gst/reports ${it_log}/gui/gst || true
fi
#####################################################################
# archive and compress logs
# since zk's data_dir contains sparse file,
# we need tar to keep the sparse file information
# tar -czf ${log_dir}.tar.gz --sparse --directory=${log_dir}/.. $(basename ${log_dir})
# echo -e "\033[31mlog collection finish\033[0m"
# echo -e "\033[31mYou can download log.tar.gz by \
#   'curl ftp://$(hostname -I | cut -d ' ' -f 1)/$(basename ${log_dir}.tar.gz) \
#   -o $(basename ${log_dir}.tar.gz)'\033[0m"

# #save detailed data (kafka data and binaries)
# mkdir ${log_dir}/data
# cp -Rp ${PROJECT_ROOT}/kafka/log_dir ${log_dir}/data/kafka_log_dir
# cp -Rp ${PROJECT_ROOT}/bin ${log_dir}/data/
# tar -cf ${log_dir}.data.tar --sparse --directory=${log_dir} data

# echo -e "\033[31mbinary collection finish\033[0m"
# echo -e "\033[31mYou can download data.tar.gz by \
#   'curl ftp://$(hostname -I | cut -d ' ' -f 1)/$(basename ${log_dir}.data.tar) \
#   -o $(basename ${log_dir}.data.tar)'\033[0m"
