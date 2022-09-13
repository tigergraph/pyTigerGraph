#!/bin/bash
cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)

if [[ $# < 1 ]]; then
  echo -e "\nUsage: \t$0 config_file mode cmd_content\n"
  exit 1
fi

user=$1
config_file=$2
mode=$3
cmd_content=$4
cmd_content2=$5

while read -r line; do
  if [[ ! $line =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    continue
  fi
  line="$user@$line"
  echo -e "\n---------------On machine $line-------------------------"
  if [[ "$mode" == "scp" ]]; then
    scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no "$cmd_content" $line:$cmd_content2
  elif [[ "$mode" == "file" ]]; then
    remote_f="/tmp/remote_cmd.sh"
    scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no "$cmd_content" $line:$remote_f
    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -n $line "bash $remote_f; rm -rf $remote_f"
  elif [[ "$mode" == "cmd" ]]; then
    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -n $line "$cmd_content"
  fi
  echo -e "\n---------------machine $line end-------------------------"
done < $config_file

echo "Successfully ran command on all machines in the config file"
