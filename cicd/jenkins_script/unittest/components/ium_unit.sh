#!/bin/bash
cd $(dirname ${BASH_SOURCE[0]})
branch='master'
if [[ $# -ge 1 ]]; then
  branch=$1
  shift
fi

source ../env.sh
#####################################################################
save_workspace
######################################################################
# test part 1: gtest
ium_test_folder=${PRODUCT}/src/gium
rm -rf $ium_test_folder
git_clone gium $branch $ium_test_folder
if [[ -d "$ium_test_folder/gtest" ]]; then
  cd $ium_test_folder/gtest
  bash test_all.sh
  cd -
fi

######################################################################
# test part 2: original ium test
# copy current package for ium test
rm -rf /tmp/ium_test
mkdir -p  /tmp/ium_test/pkg
TG_BIN=$(ls -t $PRODUCT/*-offline/tigergraph.bin | head -1)
cp $TG_BIN /tmp/ium_test/pkg/
PKG=`ls ${PROJECT_ROOT}/pkg_pool/*.tar.gz -atr | tail -n -1`
cp -f $PKG /tmp/ium_test/pkg/poc4.4_base.tar.gz
#####################################################################
cd $PRODUCT/bigtest/tests/ium_regression/
cp ~/.gsql/gsql.cfg.commited ./config_sample/MultiNode.cfg
./run_all.sh -b $branch -m MULTI
cd -
#####################################################################
# recover ium configuration
#yes | gadmin --config dummy || true
#echo -e '500\n500\n500\n500\n500\n500\nn\ny' | gadmin --configure timeout &> /dev/null
#gadmin config-apply
#####################################################################
# reinstall
bash $TG_BIN -y
