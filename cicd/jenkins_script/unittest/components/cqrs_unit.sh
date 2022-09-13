#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace
read_ut_opt "$@"


cd $PRODUCT/src/cqrs
rm -rf dev_tool/src/*
GLOCAL=~/.glocal
bash ./install_base.sh
export GOROOT=${GLOCAL}/go/go
PROTOC_ROOT=$(find $GLOCAL -type f -name protoc | head -1)
export PATH=$GOROOT/bin:${PROTOC_ROOT%%/protoc}:$PATH
if [ -z "$UT_PARAM" ]; then
  make test
else
  if grep "^$(echo ${UT_PARAM}):" makefile &> /dev/null; then
    make $UT_PARAM
  else
    echo "Warning: target ${UT_PARAM} is not supported, exiting..."
    exit 0
  fi
  #make e2e
fi
