#!/bin/bash
########################################################
if [[ -d "$PRODUCT/src/vis/tools/apps" ]]; then
  sed -i 's/192.168.11.23:5000/us-central1-docker.pkg.dev\/tigergraph-mit\/mit/g' $PRODUCT/src/vis/tools/apps/gap/test/setup.sh
  sed -i 's/192.168.11.23:5000/us-central1-docker.pkg.dev\/tigergraph-mit\/mit/g' $PRODUCT/src/vis/tools/apps/gap/test/utils.sh
  sed -i 's/192.168.11.23:5000/us-central1-docker.pkg.dev\/tigergraph-mit\/mit/g' $PRODUCT/src/vis/tools/apps/gst/test/setup.sh
  sed -i 's/192.168.11.23:5000/us-central1-docker.pkg.dev\/tigergraph-mit\/mit/g' $PRODUCT/src/vis/tools/apps/gst/test/utils.sh
else
  sed -i 's/192.168.11.23:5000/us-central1-docker.pkg.dev\/tigergraph-mit\/mit/g' $PRODUCT/src/vis/gap/test/setup.sh
  sed -i 's/192.168.11.23:5000/us-central1-docker.pkg.dev\/tigergraph-mit\/mit/g' $PRODUCT/src/vis/gap/test/utils.sh
  sed -i 's/192.168.11.23:5000/us-central1-docker.pkg.dev\/tigergraph-mit\/mit/g' $PRODUCT/src/vis/gst/test/setup.sh
  sed -i 's/192.168.11.23:5000/us-central1-docker.pkg.dev\/tigergraph-mit\/mit/g' $PRODUCT/src/vis/gst/test/utils.sh
fi

grep -rlZ "192.168.55.60" $PRODUCT/src | xargs -0 sed -i 's/192.168.55.60/rdbms.graphtiger.com/g'

########################################################
cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
cd $cwd
# test validation
if [[ $# > 5 || $# == 0 ]]; then
  echo "Usage: ./run.sh /path/to/folder [-h] [-i integration_tests] [-skip_bc]"
  exit 1
fi
########################################################
# helper function to run tests print passing messages

function run_test {
  run_cmd="$1"; repo="$2"; test_type="$3"; test_id="$4"; test_dir="$5";

  if [[ " gap gst gus " =~ $repo ]]; then
    test_name="${repo} ${test_id}"
    CUR_LOG=${IT_LOG}/gui/${repo}
    is_gtest=false
  else
    test_name="${repo}_${test_type} regress${test_id}"
    CUR_LOG=${IT_LOG}/${repo}
    is_gtest=true
  fi
  
  [[ -d $CUR_LOG ]] || mkdir -p $CUR_LOG

  #it_log_name=$CUR_LOG/${test_name// /_}_it.log
  it_log_name=$CUR_LOG/${repo}_integration_tests.log
  
  if $is_gtest; then
    if [[ ! -d $test_dir/regress${test_id} ]]; then
      echo -e "\nTEST NOT FOUND, SKIPPING $(pwd)/$test_dir/regress${test_id}\n"
      return 0
    fi
  fi

  echo -e "=====================================================================================" |& tee -a $it_log_name
  echo "        $test_name integration test started at $(date +'%F %T.%6N')"                      |& tee -a $it_log_name
  echo -e "=====================================================================================" |& tee -a $it_log_name
  echo "${test_name} (running)" &>> $summary_file
  sync

  [[ -d $test_dir ]] && tester=$(grep Tester ./${test_dir}/regress${test_id}/* 2>/dev/null)
  [[ ! -z $tester ]] && echo -e "\n${tester}"

  start_t=$(date +%s)

  echo -e "\n$(pwd)\n${run_cmd}"
  eval "$run_cmd" &>> $it_log_name
  res_code=$?

  stop_t=$(date +%s)
  run_t=$(echo "scale=1; ($stop_t - $start_t) / 60" | bc -l)
  
  sed -i "/${test_name} (running)/d" $summary_file
  echo -e "\nrun time: ${run_t} min \n"

  if [[ $res_code != 0 ]]; then 
    test_result=failed
    echo -e "${test_result^^} \n"
    echo -e "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" |& tee -a $it_log_name
    echo "        $test_name integration test ${test_result} at $(date +'%F %T.%6N')"                       |& tee -a $it_log_name
    echo -e "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" |& tee -a $it_log_name
    echo "${test_name} ${run_t} min (${test_result})" &>> $summary_file

    if [[ "$NO_FAIL" -ge "2" ]]; then
      echo "$res_code" > $LOG_FOLDER/really_fail_flag
    else      
      [[ $(echo "$run_t >= 40.0" | bc -l) -eq 1 && $repo == "gle" && $test_type == "shell" ]] && to_driver=true
      if [[ ! $to_driver ]]; then
        exit ${res_code}
      fi      
    fi

  else
    test_result=passed
    echo -e "${test_result^^} \n"
    echo -e "=====================================================================================" |& tee -a $it_log_name
    echo "        $test_name integration test ${test_result} at $(date +'%F %T.%6N')"               |& tee -a $it_log_name
    echo -e "=====================================================================================" |& tee -a $it_log_name
    echo "${test_name} ${run_t} min" &>> $summary_file
  fi
}
########################################################
LOG_FOLDER=$1
if [ -f $LOG_FOLDER ]
then
  echo -e "Error : $LOG_FOLDER is not a folder!"
  exit 1
fi
mkdir -p $LOG_FOLDER || true
LOG_FILE=$LOG_FOLDER/integration_test.log
shift 1
########################################################
source $cwd/../run_common.sh &>> $LOG_FILE
source $cwd/../env_setup.sh  &>> $LOG_FILE
########################################################
# some common setup that is shared with gsql unit test
script_name='integration'

hourly=false
skip_bc=false

while [[ $# -gt 0 ]]; do
  if [ "$1" = "-h" ]; then
    hourly=true
    shift 1
  elif [ "$1" = "-skip_bc" ]; then
    skip_bc=true
    shift 1
  elif [ $1 = "-i" ]; then
    if [[ "$2" != "all" ]]; then
      integration_tests=$2
    fi
    shift 2
  else
    echo "Usage: ./run.sh /path/to/folder [-h] [-i integration_tests]"
    exit 1
  fi
done

IT_LOG=$LOG_FOLDER/integration_test_logs
mkdir -p $IT_LOG

summary_file=$LOG_FOLDER/integration_test_summary
rm -rf $gle_summary_file $summary_file

export LOG_FOLDER=$LOG_FOLDER
export LOG_FILE=$LOG_FILE
export IT_LOG=$IT_LOG
export summary_file=$summary_file
export -f run_test

#########################################################
# parse integration_tests into arrays

declare -A it_map
IFS=';' read -r -a arr <<< "$integration_tests"
for ele in "${arr[@]}"; do
  regress_type=$(echo ${ele} | cut -d ':' -f1 | cut -d ' ' -f 1)
  [[ -z "${regress_type}" ]] && continue
  
  it_map[$regress_type]=" $(echo ${ele} | cut -d ':' -f2) "  
done

echo "LOG_FOLDER = $LOG_FOLDER"
echo "integration_tests: " "$integration_tests"

# clear old output and diff
rm -rf $PRODUCT/gtest/diff/* 2>/dev/null
rm -rf $PRODUCT/gtest/output/* 2>/dev/null

#########################################################
# for now don't run backward compatibility testing for 3.x.x
if [[ $skip_bc != "true" && $MAJOR_VERSION -lt 3 ]]; then
  # gsql back compatible testing
  echo -e "\n run gsql back compatible testing"
  echo -e "\n restore catalog from previous backup"
  bash $cwd/../shell_script/catalog_manager.sh 'restore' &> "$LOG_FOLDER/mit_log/catalog_manager.log"
  res_code=$?
  if [[ "$NO_FAIL" -ge "2" && $res_code != 0 ]]; then
    echo 'gsql backward compatibility test failed'
    echo "$res_code" > $LOG_FOLDER/really_fail_flag
  fi
fi
#########################################################
## end2end regresses

all_start_t=$(date +%s)

repos=$(get_repos "$integration_tests")

echo "REPOS" "$repos"

for repo in $repos; do
  echo -e "*****************************************************************"
  echo "         ${repo^^} integration testing started at $(date +'%F %T')"
  echo -e "*****************************************************************"

  test_types="${ALL_IT[$repo]}"

  if [[ -z $test_types ]]; then
    full_key=$repo
    test_list="${it_map[$full_key]}"    
    bash $cwd/components/${repo}E2E.sh "$test_list" |& tee -a $LOG_FILE

  else
    for type in $test_types; do
      full_key="${repo}_${type}"
      if [[ "${it_map[$full_key]+v}" ]]; then      
        test_list="${it_map[$full_key]}"
        bash $cwd/components/${repo}E2E.sh "$type" "$test_list" |& tee -a $LOG_FILE
      fi
    done
  fi

  echo -e "*****************************************************************"
  echo "        ${repo^^} integration testing finished at $(date +'%F %T')"
  echo -e "*****************************************************************"  
done

all_end_t=$(date +%s)
all_run_t=$(echo "scale=1; ($all_end_t - $all_start_t) / 60" | bc -l)
echo "Total ${all_run_t} min" &>> "$summary_file"

if [[ -f $LOG_FOLDER/really_fail_flag ]]; then
  echo -e "\n\nIntegration test failed!"
  echo -e "\nfailed tests:\n$(grep failed $summary_file)"
  exit 1
else
  echo -e "\n\nIntegration test passed!"
fi
