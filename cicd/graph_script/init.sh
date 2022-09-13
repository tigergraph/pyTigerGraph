#!/bin/base_branch
cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
cd $cwd

~/.gium/gsql_shell mit_schema.gsql
~/.gium/gsql_shell mit_load.gsql

bash install_query.sh
