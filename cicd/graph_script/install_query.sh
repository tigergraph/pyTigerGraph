#!/bin/bash

cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
set -ex
cd $cwd

function __install_query(){
  for f in $(ls); do
    echo $f
    gsql $f
  done
}

cd query

cd util_query
__install_query
cd -

cd internal_query
__install_query
cd -

cd admin_query
__install_query
cd -

cd user_query
__install_query
cd -
