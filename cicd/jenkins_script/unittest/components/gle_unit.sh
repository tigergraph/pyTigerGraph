#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace

# run gsql-server JUnit test
cd $PRODUCT/src/gle
# skip for 2.x
if [ -f 'gradlew' ] && ./gradlew :gsql-server:clean &>/dev/null; then
  # since 3.2.0, setup_java_home from gle
  if [ -f tools/setup_java_home.sh ]; then
    source tools/setup_java_home.sh && setup_java_home "$PRODUCT/src/gle"
    export JAVA_HOME=$JAVA_HOME
  fi
  # pass -POFFLINE to be compatibile with pre-3.2 branches
  ./gradlew :gsql-server:check -POFFLINE
  IS_JUNIT=1
fi
cd - >/dev/null 2>&1

# run only for 2.x
if [ $IS_JUNIT -ne 1 ]; then
  # setup for other test suite
  source gle_setup.sh
  gadmin stop ${VIS_NAME:-gui} -y

  cd $PRODUCT/gtest

  # change the number of threads to 1
  sed -i 's/numThreads=[0-9]*/numThreads=1/g' config

  # run gsql/ddl test
  if [[ -d test_case/gsql/ddl ]]; then
    all_regress=$(ls -d test_case/gsql/ddl/regress*)
    for file in ${all_regress}
    do
      num=${file##*regress}
      echo -e "\nrun ddl regress $num at $(date +'%F %T.%6N')"
      ./gtest gsql.sh ddl $num
    done
  fi

  # run transform test
  if [[ -d test_case/transform ]]; then
    all_regress=$(ls -d test_case/transform/regress*)
    for file in ${all_regress}
    do
      num=${file##*regress}
      echo -e "\nrun transform regress $num at $(date + '%F %T.%6N')"
      ./resources/transform/regress${num}/setup.sh
      ./gtest transform.sh $num
    done
  fi

  # change the number of threads back to 16
  sed -i 's/numThreads=[0-9]*/numThreads=16/g' config
fi
