#!/bin/bash
########################################################
gle_cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)/..
cd $gle_cwd

repo_name="gle"
########################################################
# some common setup that is shared with gsql unit test
source $gle_cwd/env.sh &>> $LOG_FILE
source $gle_cwd/components/gle_setup.sh &>> $LOG_FILE
#########################################################
# run test

type="$1"
integration_tests="$2"

if [ -d $PRODUCT/gtest ]; then
  cd $PRODUCT/gtest
else
  cd $PRODUCT/src/gle/regress
fi

driver_home=$(pwd)

DRIVER="bash $driver_home/gtest.sh"

sed -i 's/TimeOutMinutes=[0-9]*/TimeOutMinutes=40/g' $driver_home/config
sed -i 's/numThreads=[0-9]*/numThreads=1/g' $driver_home/config

#########################################################
## end2end regresses

if [[ $type == "shell" ]]; then
  test_dir=test_case/${type}
else
  test_dir=test_case/end2end/${type}
fi
  

for num in ${integration_tests}; do

  setup_cmd=:
  [[ $type != "shell" ]] && setup_cmd="bash ./resources/end2end/${type}/regress$num/setup.sh"

  if [[ $type == "shell" ]]; then
    run_cmd="$DRIVER shell.sh $num"
  else
    run_cmd="$DRIVER end2end.sh $type $num"
  fi

  run_test "(${setup_cmd}) && (${run_cmd})" "${repo_name}" "${type}" "${num}" "${test_dir}"

  # re-run if the test fails with timeout
  if [[ $res_code != 0 && $type == "shell" ]]; then
    if (( $(echo "$run_t >= 40.0" | bc -l) )); then
      mkdir -p $LOG_FOLDER/rerun_log
      cp -rf test_case/shell/regress$num $LOG_FOLDER/rerun_log 2>/dev/null || :
      cp -rf diff/shell/regress$num $LOG_FOLDER/rerun_log 2>/dev/null || :
      cp -rf output/shell/regress$num $LOG_FOLDER/rerun_log 2>/dev/null || :

      start_t=$(date +%s)

      echo -e "\nre-run ${repo_name}_${type} regress $num"
      echo -e "\nre-run ${repo_name}_${type} $num at $(date +'%F %T.%6N')"

      notification_message=$(cat <<-END
        {
          "url": "${BUILD_URL}", 
          "name": "[${JOB_NAME}#${BUILD_NUMBER}]($BUILD_URL) of [${T_JOB_ID}#${T_BUILD_NUMBER}](${JENKINS_URL}job/${T_JOB_NAME}/${T_BUILD_NUMBER})", 
          "Comment": "Integration test ${repo_name}_${type} regress${num} failed due to timeout, but it will not block this job, it will re-run once. cc @**Wenbing Sun** @**CHENGBIAO JIN** "
        }
END
)
      python3 ${PYTHON3_SCRIPT_FOLDER}/notification.py '' 'STATUS' "${USER_NAME}@tigergraph.com" 'TigerGraph Testing Status' 'Test Timeout' "${notification_message}"
      
      run_test "(${setup_cmd}) && (${run_cmd})" "${repo_name}" "${type}" "${num}" "${test_dir}"

      [[ $res_code != 0 ]] && exit $res_code
    fi

    # if unit test failed with no_fail option, print info and record in summary_file
    if [[ "$NO_FAIL" -ge "2" ]]; then
      # Workaround QA-1607
      pkill -SIGKILL -f gsql || true
      gadmin restart -y
    fi  
  fi
done

sed -i 's/numThreads=[0-9]*/numThreads=8/g' $driver_home/config
sed -i 's/TimeOutMinutes=[0-9]*/TimeOutMinutes=20/g' $driver_home/config

cd -
