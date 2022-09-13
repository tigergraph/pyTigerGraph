#!/bin/bash
########################################################################################
source $(cd $(dirname ${BASH_SOURCE[0]}) && pwd)/../util.sh
set -o pipefail
set +x
########################################################################################

echo "${repo_name^^} DRIVER"

save_workspace

function safexit {
  exit_code=$?

  exec &>> $LOG_FILE

  if [[ $exit_code == 0 || "$NO_FAIL" -ge "2" ]]; then
    clean_up
    restore_workspace
  fi
}
trap safexit exit
trap 'exit 143' TERM INT
