#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace

gadmin start
sleep 20

sudo service docker start || true

cd $PRODUCT/src/vis/gui/test
bash test.sh
cd -
