#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
source ../env.sh

save_workspace

gadmin start
sleep 20

if [ -d ${PRODUCT}/src/engine ]; then
  cd $PRODUCT/src/engine/realtime/integrationtest
else
  cd $PRODUCT/src/realtime/integrationtest
fi
echo -e '--------------------------------restppreg test start--------------------------------'
python main.py --config-path config.json --product-path $PRODUCT"/" \
    --deployment-path $PROJECT_ROOT"/" --tests restppreg --regmode query,loading,end2end \
    --reset-gstore --run-for-jenkins
echo -e '--------------------------------restppreg test end--------------------------------\n\n\n'

echo -e '--------------------------------correctness test start--------------------------------'
python main.py --config-path config.json --product-path $PRODUCT"/" \
    --deployment-path $PROJECT_ROOT"/" --tests correctness --regmode query,loading,end2end \
    --reset-gstore --run-for-jenkins
echo -e '--------------------------------correctness test end--------------------------------'
