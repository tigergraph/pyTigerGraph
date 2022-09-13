#!/bin/bash
cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
cd $cwd
source ../env.sh

save_workspace

gadmin start
sleep 20
gadmin stop ${VIS_NAME:-gui} -y
sleep 5

cd $PROJECT_ROOT/document/DEMO
echo '-------start demo test for json api v1-----------'
gsql 'set json_api = "v1" '
bash RUN_DEMO.sh | tee $CONFIG_DIR/demo_v1.out
echo '-------start demo test for json api v2-----------'
gsql 'set json_api = "v2" '
bash RUN_DEMO.sh | tee $CONFIG_DIR/demo_v2.out
cd -

python3 $cwd/demo_test/compare_out.py $cwd/demo_test/demo_v1.base $CONFIG_DIR/demo_v1.out
python3 $cwd/demo_test/compare_out.py $cwd/demo_test/demo_v2.base $CONFIG_DIR/demo_v2.out
#rm -rf $CONFIG_DIR/demo_v1.out $CONFIG_DIR/demo_v2.out
