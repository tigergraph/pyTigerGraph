#!/bin/bash
#
# To change product/config/pkg.config
# add sanitizer or debug option when cmake compiles

cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
cd ${cwd}

# define error code
E_Not_Match=1
E_File_Not_Exist=2

function usage(){ 
  echo -e "\nUsage: \t$0  <-p product_dir> [-d -s]"
  echo -e "-p specify product directory"
  echo -e "-d specify debug mode"
  echo -e "-s specify sanitizer mode \n"
}

OPTIONS=""
SANITIZER=""
DEBUG_MODE="false"
product_dir=""

#Read arguments
while getopts "p:ds:" arg; do
  case "$arg" in
    p)
      product_dir=$OPTARG
      ;;
    d)
      DEBUG_MODE="true"
      ;;
    s)
      SANITIZER=$OPTARG
      ;;
    :)
      echo "Invalid option: $OPTARG requires an argument"
      usage
      exit $E_Not_Match
      ;;
    *)
      error "Invalid option $arg"
      usage
      exit $E_Not_Match
      ;;
  esac
done

if [[ -z $product_dir ]]; then
  echo "Please sepecify product directory with -p option"
  usage 
  exit $E_Not_Match
fi

pkg_config=$product_dir/config/pkg.config

# check if pkg.config file exist
if [[ ! -f "$pkg_config" ]]; then
  echo "Cannot find $pkg_config"
  exit $E_File_Not_Exist
fi

echo "Sanitizer option: $SANITIZER"
echo "Debug option: $DEBUG_MODE"

# add sanitizer flag in pkg.config
if [[ -n $SANITIZER ]]; then 
  echo "[NOTE] adding sanitizer flag in $pkg_config"
  OPTIONS="${OPTIONS}${SANITIZER}"
fi

# add debug flag in pkg.config
if [[ "$DEBUG_MODE" == "true" ]]; then 
  echo "[NOTE] add debug flag in $pkg_config"
  OPTIONS="${OPTIONS}Debug"
fi

if [[ -n $OPTIONS ]]; then
  sed -i "s#cmake \"\${GSQL_PROJ_HOME}/src\" -DCMAKE_INSTALL_PREFIX=./#cmake \"\${GSQL_PROJ_HOME}/src\" -DCMAKE_INSTALL_PREFIX=./ -DCMAKE_BUILD_TYPE=${OPTIONS}#g" $pkg_config
else  
  echo "No flags are set. Skipping add..."
fi
