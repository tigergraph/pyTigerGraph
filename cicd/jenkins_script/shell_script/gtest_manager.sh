#!/bin/bash
timestamp() {
  date +"%D %T"
}

function usage() {
  echo "usage: $0 [-h] <-t tarball_name -p product_dir>"
  echo "-h  displays this message"
  echo "-a  specifies the action to take. Must be \"backup\" or \"restore\""
  echo "-t  specifies the tarball name to use"
  echo "-p  specifies the path to the product directory (must be absolute path)"
}

#Read arguments
while getopts ":ha:t:p:" arg; do
  case "$arg" in
    h)
      usage
      exit 0
      ;;
    a)
      action=$OPTARG
      ;;
    t)
      tarball_name=$OPTARG
      ;;
    p)
      product_dir=$OPTARG
      ;;
    :)
      echo "$(timestamp): Invalid option: $OPTARG requires an argument"
      usage
      exit 1
      ;;
    *)
      echo "$(timestamp): Invalid option $arg"
      usage
      exit 1
      ;;
  esac
done

if [[ -z $action || -z $tarball_name || -z $product_dir ]]; then
  echo "$(timestamp): Error: Parameters action, tarball_name and product_dir are all required! \
    Please specify them with the -a, -t and -p options respectively!"
  usage
  exit 1
fi

if [[ $tarball_name != *.tar.gz ]]; then
  echo "$(timestamp): Error: Invalid tarball ${tarball_name}. Tarball must end with .tar.gz!"
  usage
  exit 1
fi

if [[ $product_dir != "/"* ]]; then
  echo "$(timestamp): Error: Product directory must be an absolute path!"
  usage
  exit 1
fi

echo "Listing contents under product..."
ls -l "$product_dir"

#Test directories to copy from builder or to create in test machine
#Directories must be under product repo
DIR_LIST=("blackbox:bigtest/tests"\
          "blue_features:src/engine/blue/features/gtest,src/blue/features/gtest" \
          "cqrs:src/cqrs" \
          "document:src/document/gtest,src/document/doc" \
          "gap:src/vis/tools/apps/gap,src/vis/gap" \
          "gbar:src/glive/test/GBAR" \
          "gle:src/gle,src/engine/gle,src/er/buildenv/gsql_release_version" \
          "gst:src/vis/tools/apps/gst,src/vis/gst" \
          "gus:src/vis/gus/test,src/vis/gus/*.json,src/vis/gus/e2e,src/vis/gus/tools" \
          "gus:src/vis/gus/scripts,src/vis/gus/Jenkinsfile,src/vis/gus/yarn.lock" \
          "gus:src/vis/gus/src/utils,src/vis/gus/src/config" \
          "integration:gtest,src/gle/regress" \
          "ium:src/gium/gtest" \
          "product:enginecfg.yaml,product_version,src/customer/core_impl/gsql_impl/TokenLib,src/customer/core_impl/gsql_impl/ReducerLib" \
          "rest:src/engine/realtime/integrationtest,src/realtime/integrationtest" \
          "test_bin:cmake_build/release/test" \
          "thirdparty:src/thirdparty/prebuilt/bin,src/thirdparty/prebuilt/dynamic_libs,src/third_party" \
          "ts3:src/glive/test/TS3,src/glive/ts3,src/glive/test/testframework" \
          "utility:src/engine/utility/tools,src/engine/utility/admin_server/proto,src/utility/tools,src/utility/admin_server/proto")

EXCLUDE_DIR_LIST=("src/cqrs/.git" \
                  "src/cqrs/.thirdparty" \
                  "src/cqrs/.cache/govendor" \
                  "src/third_party/.git")

action_lower=$(echo "$action" | tr '[:upper:]' '[:lower:]')

echo "$(timestamp): Changing to directory $product_dir..."
cd "$product_dir"
echo "Listing contents under src..."
ls -l src

#copy needed directories to MIT gtest folder
if [[ $action_lower = "backup" ]]; then
  backup_dir=${tarball_name%.tar.gz}

  if [[ -e $backup_dir ]]; then
    rm -rf "$backup_dir"
  fi

  #creating directory for collecting test directories
  echo "$(timestamp): Creating test folder directory $backup_dir..."
  mkdir -p "$backup_dir"

  #copy needed test directories
  echo "$(timestamp): Copying needed test folders..."
  
  allPath=""

  for dir in ${DIR_LIST[*]}; do
      test_name=${dir%:*}
      needed_dirs=${dir#*:}
      OLD_IFS="$IFS"
      IFS=','
      for needed_dir in ${needed_dirs[*]}; do
        if [[ -e $needed_dir ]]; then
          allPath=${allPath}" "${needed_dir}
        else
          echo "$(timestamp): $needed_dir not found. Skipping..."
        fi
      done
    done
    IFS="${OLD_IFS}"
    for exclude_dir in ${EXCLUDE_DIR_LIST[*]};do
      allPath=${allPath}" --exclude="${exclude_dir}
    done

    #make tarball and remove creation directory
    echo "$(timestamp): Making tarball $tarball_name and cleaning up..."
    OLD_IFS="$IFS"
    IFS=" "
    path_array=($allPath)
    tar -czf "$tarball_name" ${path_array[*]}

    cd - >> /dev/null

    echo "$(timestamp): Gtest tarball $tarball_name created successfully!"
elif [[ $action_lower = "restore" ]]; then
#Unzip tar file and remove top directory

  #Check if given tarball exists in given product directory
  echo "$(timestamp): Checking tarball $tarball_name..."
  if ! [[ -e $tarball_name ]]; then
    echo "$(timestamp): Error: $tarball_name does not exist under $product_dir! \
Please make sure it's there and try again"
    cd - >> /dev/null
    usage
    exit 1
  fi
  echo "$(timestamp): Tarball $tarball_name check pass!"

  #unzip tarball
  echo "$(timestamp): Unzipping $tarball_name..."

  # tar -xzf "$tarball_name" --strip-components 1
  tar -xzf "$tarball_name"

  #remove tarball
  echo "$(timestamp): Removing $tarball_name..."
  rm -rf $tarball_name

  cd - >> /dev/null

  echo "$(timestamp): Successfully restored all needed directories!"
else
  cd - >> /dev/null
  echo "$(timestamp): Error: action must be \"backup\" or \"restore\""
  usage
  exit 1
fi
