#!/bin/bash
############################################################################################
cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
cd $cwd
if [[ -d "$PRODUCT/src/vis/tools/apps" ]]; then
  ### change docker registry to gcp
  sed -i 's/192.168.11.23:5000/us-central1-docker.pkg.dev\/tigergraph-mit\/mit/g' $PRODUCT/src/vis/tools/apps/gap/test/setup.sh
  sed -i 's/192.168.11.23:5000/us-central1-docker.pkg.dev\/tigergraph-mit\/mit/g' $PRODUCT/src/vis/tools/apps/gap/test/utils.sh
  sed -i 's/192.168.11.23:5000/us-central1-docker.pkg.dev\/tigergraph-mit\/mit/g' $PRODUCT/src/vis/tools/apps/gst/test/setup.sh
  sed -i 's/192.168.11.23:5000/us-central1-docker.pkg.dev\/tigergraph-mit\/mit/g' $PRODUCT/src/vis/tools/apps/gst/test/utils.sh
else
  ### change docker registry to gcp
  sed -i 's/192.168.11.23:5000/us-central1-docker.pkg.dev\/tigergraph-mit\/mit/g' $PRODUCT/src/vis/gap/test/setup.sh
  sed -i 's/192.168.11.23:5000/us-central1-docker.pkg.dev\/tigergraph-mit\/mit/g' $PRODUCT/src/vis/gap/test/utils.sh
  sed -i 's/192.168.11.23:5000/us-central1-docker.pkg.dev\/tigergraph-mit\/mit/g' $PRODUCT/src/vis/gst/test/setup.sh
  sed -i 's/192.168.11.23:5000/us-central1-docker.pkg.dev\/tigergraph-mit\/mit/g' $PRODUCT/src/vis/gst/test/utils.sh
fi

grep -rlZ "192.168.55.60" $PRODUCT/src | xargs -0 sed -i 's/192.168.55.60/rdbms.graphtiger.com/g'

############################################################################################
# helper function to run tests print passing messages
function rununit()
{
  rm -rf $UT_LOG/$1_ut.log
  echo -e "====================================================================================="
  echo "        $1  unittests begin at $(date +'%F %T.%6N')!"
  echo -e "====================================================================================="
  if [ "$1" = "ium" ]; then
    cmd="bash $2 $ium_branch"
  else
    # configure LD_PRELOAD
    if [[ "$SANITIZER" == "asan" && "$2" =~ ^(gse|gpe) ]]; then
      cmd="LD_PRELOAD=/usr/lib64/libasan.so.5 bash $2 $3 $4"
    else
      cmd="bash $2 $3 $4"
    fi
  fi

  echo -e "\n${cmd}"
  eval "${cmd}" &> $UT_LOG/$1_ut.log

  res_code=$?
  run_t=$(echo "scale=1; ($(date +%s) - $start_t) / 60" | bc -l)
  
  sed -i "/${test_name} (running)/d" $summary_file
  echo -e "run time: ${run_t} min \n"

  # if unit test failed with no_fail option, print info and record in summary_file
  if [[ "$NO_FAIL" -ge "2" && $res_code != 0 ]]; then
    echo -e "FAILED \n"
    echo -e "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    echo "        $1  unittests failed! at $(date +'%F %T.%6N')"
    echo -e "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    echo "${test_name} ${run_t} min (failed)" >> $summary_file
    really_fail=true
  else
    echo -e "PASSED \n"
    echo -e "====================================================================================="
    echo "        $1  unittests passed! at $(date +'%F %T.%6N')"
    echo -e "====================================================================================="
    echo "${test_name} ${run_t} min" >> $summary_file
  fi
}

############################################################################################
# test validation
if [[ $# > 7 || $# == 0 ]]; then
  echo "Usage: ./run.sh /path/to/folder [-u unittests] [-b ium_branch] [-sanitizer type]"
  exit 1
fi
############################################################################################
# get log folder
LOG_FOLDER=$1
if [ -f $LOG_FOLDER ]
then
  echo -e "Error : $LOG_FOLDER is not a folder!"
  exit 1
fi
mkdir -p $LOG_FOLDER || true
shift 1

#########################################################
# some common setup
script_name='unittest'
really_fail=false
source $cwd/../env_setup.sh &>/dev/null
#########################################################
set +x
#########################################################

ium_branch='master'
unittests=''
sanitizer=''

UT_LOG=$LOG_FOLDER/unit_test_logs
mkdir -p $UT_LOG

# record integration tests each regress time cost
summary_file=$LOG_FOLDER/unit_test_summary
rm -rf $summary_file

while [[ $# -gt 0 ]]; do
  if [ $1 = "-u" ]; then
    unittests=$2
    shift 2
  elif [ $1 = "-b" ]; then
    ium_branch=$2
    shift 2
  elif [ $1 = "-sanitizer" ]; then
    sanitizer="$1 $2"
    shift 2
  else
    echo "Usage: ./run.sh /path/to/folder [-u unittests] [-b ium_branch] [-sanitizer type]"
    exit 1
  fi
done

echo "LOG_FOLDER = $LOG_FOLDER"
echo "unittests = $unittests"

# clear old output and diff
rm -rf $PRODUCT/gtest/diff/*
rm -rf $PRODUCT/gtest/output/*


# if no_fail is no smaller than 2, it will not exit for failure
if [[ "$NO_FAIL" -ge "2" ]]; then
  set +e
fi
############################################################################################
# Run unittest! Run!!!!!!!
function run_a_unit() {
  local unit=$1
  local cmd=${2:-${cwd}/components/${unit}_unit.sh}
  start_t=$(date +%s)
  test_name=$unit
  sync
  date
  echo "${test_name} (running)" >> $summary_file
  rununit $unit "$cmd" $sanitizer
}

test_name=""
all_start_t=$(date +%s)
for unit in $unittests; do
  [ "$unit" = "none" ] && continue
  if grep -Eq "^\s*${unit}(_|\s*:)" ${cwd}/unittests.conf; then
    suites=$(grep -E "^\s*${unit}(_|\s*:)" ${cwd}/unittests.conf)
    while read -r one_suite; do
      one_unit=$(echo ${one_suite%%:*})
      one_cmd=$(echo ${one_suite#*:})
      if [[ "$one_cmd" != /* ]]; then
        one_cmd="${cwd}/${one_cmd}"
      fi
      one_driver=$(echo ${one_cmd%% *})
      if [[ -f "$one_driver" ]]; then
        run_a_unit $one_unit "$one_cmd"
      fi
    done <<< "$suites"
  elif [[ -f "${cwd}/components/${unit}_unit.sh" ]]; then
    run_a_unit $unit
  elif [[ "${unit}" != "none" ]]; then
    echo "Error: Unit test ${unit} is not found!"
    exit 1
  fi
done
all_run_t=$(echo "scale=1; ($(date +%s) - $all_start_t) / 60" | bc -l)
echo "Total ${all_run_t} min" >> $summary_file

# check really_fail variable and touch a file "really_fail_flag" for checking and exiting later.
if [[ "$NO_FAIL" -ge "2" && $really_fail == true ]]; then
  touch $LOG_FOLDER/really_fail_flag
  echo -e "\n\nUnit tests failed, but script will not exit due to no_fail option enabled"
else
  echo -e "\n\nAll unittests passed!"
fi
############################################################################################
