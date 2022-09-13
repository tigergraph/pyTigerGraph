#!/bin/bash
########################################################################################
source $(cd $(dirname ${BASH_SOURCE[0]}) && pwd)/util.sh
set -eo pipefail
set +x
########################################################################################
# collect log informations and binaries
function collect_log () {
  if [ -z $LOG_FOLDER ]; then
    LOG_FOLDER="/tmp/${USER}_test/"
    rm -rf $LOG_FOLDER
    mkdir -p $LOG_FOLDER || true
  fi
  $SHELL_SCRIPT_FOLDER/collector.sh $LOG_FOLDER &> $LOG_FOLDER/mit_log/collector.log
}

function finally () {
  exit_code=$?
  if [[ $exit_code == 0 ]]; then
    echo "Success!"
    if [[ $MAJOR_VERSION -ge 3 ]]; then
      gadmin restart all -y
      gadmin stop gsql -y
      LD_PRELOAD=$ASAN_LIB $(gadmin config get System.AppRoot)/dev/gdk/gsql/lib/.tg_dbs_gsqld.jar -c ~/.tg.cfg -r 1 --reset
      gadmin start gsql
    else
      gadmin restart -y
      gsql --reset
    fi
  else
    if [[ "$exit_code" == "143" ]]; then
      echo "Pipeline Cancelling due to timeout, Failed"
    else
      echo "Fail!"
    fi
    set +e
    if [[ -z $test_name ]]; then
      last_row=$(tac $summary_file | egrep -m 1 . | grep running 2>/dev/null)
      words=($last_row)
      test_name="${words[@]:0:2}"
    fi

    if [ ! -z "$test_name" ]; then
      sed -i "/${test_name} (running)/d" $summary_file
      run_t=$(echo "scale=1; ($(date +%s) - $start_t) / 60" | bc -l)
      if [[ "$exit_code" == "143" ]]; then
        echo "${test_name} ${run_t} min (uncompleted)" >> $summary_file
      else
        echo "${test_name} ${run_t} min (failed)" >> $summary_file
      fi
    fi
    all_run_t=$(echo "scale=1; ($(date +%s) - $all_start_t) / 60" | bc -l)
    echo "Total ${all_run_t} min" >> $summary_file
    collect_log
    set -e
  fi
}
trap finally exit
trap 'exit 143' TERM INT
########################################################################################

# configure timeout for test
if [[ $MAJOR_VERSION -ge 3 ]]; then
  gadmin config set RESTPP.Factory.DefaultQueryTimeoutSec 1000
  gadmin config apply -y
  gadmin restart restpp -y
  export VIS_NAME=gui
else
  timeout_t=1000
  ~/.gium/gadmin --set Restpp.timeout_seconds $timeout_t > /dev/null
  gadmin config-apply -y
  export VIS_NAME=vis
fi

########################################################################################
## source node expansion functions for test
source $JENKINS_SCRIPT_FOLDER/expansion_api.sh
