#!/bin/bash
########################################################################################
source $(cd $(dirname ${BASH_SOURCE[0]}) && pwd)/../util.sh
set -exo pipefail
########################################################################################
function safexit () {
  exit_code=$?
  ps aux | grep tigergraph || true
  if [ $exit_code == 0 ]; then
    echo "Component test Success!"
    # clean up
    clean_up
    # restore worspace, this is needed since some ut cleans up kafka and zk
    restore_workspace
  else
    echo "Component test Fail!"

    # if no_fail option is enabled, the test will continue. So it requires to do the cleanup.
    if [[ "$NO_FAIL" -ge "2" ]]; then
      clean_up
      restore_workspace
    fi
  fi
}
trap safexit exit
trap 'exit 143' TERM INT
