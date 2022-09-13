#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

# save_workspace
#
# gadmin start zk
# sleep 20
# gadmin start
#
# sleep 10
#
# cp -rf $PROJECT_ROOT/glive/rest-server/nodejs $PRODUCT/src/glive/glive/rest-server
# cp -rf $PROJECT_ROOT/glive/rest-server/node_modules $PRODUCT/src/glive/glive/rest-server/
#
# cd $PRODUCT/src/glive/test/Glive
# bash test.sh
# cd -
echo 'skip glive'
