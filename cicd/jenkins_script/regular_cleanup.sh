#!/bin/bash
# This script is to cleanup ~/jenkins_log dir regularly
##############################################
cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
cd ${cwd}
node_name='master'
if [[ $# -ge 0 ]]; then
   node_name=$1
fi

log_dir=$(jq -r .log_dir ${cwd}/config/config.json)
if [[ -d ${log_dir} && "$node_name" == "master" ]]; then
  # find and rm file that modified 40 days ago
  echo -e "\033[34mdelete log files older than 40 days in log_dir \033[0m"
  find ${log_dir} -maxdepth 1 -mindepth 1 -type d -mtime +30 ! -name 'config' -print | xargs sudo rm -rf
  echo -e "\033[34mdelete empty directories in log_dir \033[0m"
  find ${log_dir} -maxdepth 1 -mindepth 1 -type d -empty ! -name 'config' -print | xargs sudo rm -rf
fi

# find and rm files in /tmp that modified 15 days ago
echo -e "\033[34mdelete /tmp/* files older than 15 days\033[0m"
find /tmp -type f -user ${USER} -mtime +15 -print | xargs sudo rm -rf

# print disk usage
df ~

# analysis disk usage, if usage is larger than 80%
# report error and exit 2
disk_usage=$(df -Ph ~ | tail -1 | awk '{print $5}' | cut -d'%' -f 1)
if [ ${disk_usage} -gt 80 ]; then
  echo "Disk usage of /home/tigergraph larger than 80% !!!!!!"
  exit 2
fi

disk_usage=$(df -Ph /tmp | tail -1 | awk '{print $5}' | cut -d'%' -f 1)
if [ ${disk_usage} -gt 80 ]; then
  echo "Disk usage of /tmp larger than 80% !!!!!!"
  exit 3
fi

echo "Disk is OK."
