#!/bin/bash
#############################################
##This is a script to setup gsql testing env.
#############################################
source $(dirname ${BASH_SOURCE[0]})/../../util.sh
source ~/.bashrc || true

ulimit -n
#############################################
## link to lib/gle
if [ -d $PRODUCT/gtest ]; then
  cd $PRODUCT/gtest
  ln -s -f ../lib/gle/regress/base_line
  ln -s -f ../lib/gle/regress/drivers
  ln -s -f ../lib/gle/regress/lib
  ln -s -f ../lib/gle/regress/resources
  ln -s -f ../lib/gle/regress/test_case
  cd -
fi
############################################
# turn off zk disk writes
gadmin start all
sleep 20
gadmin status -v
gsql 'set json_api = "v2" '
#############################################
