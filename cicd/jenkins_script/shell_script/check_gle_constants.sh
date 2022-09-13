#/bin/bash

cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
source $cwd/../util.sh
set -ex

# dir to javacc (before 2.5)
CONSTANT_DIR="com/tigergraph/schema/javacc"
# dir to javacc (since 2.5)
CONSTANT_DIR_GRADLE="src-generated/main/java/$CONSTANT_DIR"
# root dir to gsql-server (since 3.0)
PREFIX_300="gsql-server"
# javacc Gradle task
GRADLE_TASK_JAVACC="javacc"

CHECKSUM=
function getChecksum {
  cd ${PRODUCT}/src/gle
  if [ -f 'gradlew' ]; then
    local target_dir=$CONSTANT_DIR_GRADLE
    local target_task=$GRADLE_TASK_JAVACC
    if ! [[ "${BASE_BRANCH}" = tg_2.* ]]; then
      # if $PREFIX_300 exsits, update vars accordingly
      target_dir="$PREFIX_300/$CONSTANT_DIR_GRADLE"
      target_task=":$PREFIX_300:$GRADLE_TASK_JAVACC"
      # since 3.2.0, setup_java_home from gle
      if [ -f tools/setup_java_home.sh ]; then
        source tools/setup_java_home.sh && setup_java_home "$PRODUCT/src/gle"
        export JAVA_HOME=$JAVA_HOME
      fi
    fi
    ./gradlew $target_task
    CHECKSUM=$(md5sum ${target_dir}/TigerGraphConstants.java | awk '{print $1}')
  else # backward compatibility
    cd ${CONSTANT_DIR} && ./compile
    CHECKSUM=$(md5sum TigerGraphConstants.java | awk '{print $1}')
  fi
}

getChecksum
new_sum=$CHECKSUM
echo "new_sum is ${new_sum}"

cd ${PRODUCT}/src
rm -rf gle_backup && cp -rf gle gle_backup
cd gle && git reset --hard && git prune && git fetch --all --prune && git checkout ${BASE_BRANCH} && git clean -f -d

getChecksum
original_sum=$CHECKSUM
echo "original_sum is ${original_sum}"

# to keep env unchanged
cd ${PRODUCT}/src
rm -rf gle && mv gle_backup gle

if [[ "$new_sum" != "$original_sum" ]]; then
  echo 'check sums of TigerGraphConstants.java are not equal !!!!'
fi
