#!/bin/bash

cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
set -ex

iumBranch='master'
if [[ $# -ge 1 ]]; then
  iumBranch=$1
fi

base_dir=~

#Get git token from Jenkins credential if possible
#If git token is empty here, set it
if [[ -z $GIT_TOKEN ]]; then
  TOKEN='5D4F3079B50C3C25AD015EF68FBA7B20B46B714D'
  GIT_TOKEN=$(echo $TOKEN |tr '97531' '13579' |tr 'FEDCBA' 'abcdef')
fi

#add IUM
curl --fail -H "Authorization: token $GIT_TOKEN" -L \
    https://api.github.com/repos/TigerGraph/gium/tarball/$iumBranch -o $base_dir/gium.tar.gz
if [ $? != 0 ]; then
  echo "Download IUM failed"
  exit 1
fi
rm -rf $base_dir/tigergraph-gium*
tar xzf $base_dir/gium.tar.gz -C $base_dir
if [ $? != 0 ]; then
  echo "Uncompress IUM failed"
  exit 1
fi
cd $base_dir/tigergraph-gium*
bash install.sh
cd -
rm -rf $base_dir/tigergraph-gium*
