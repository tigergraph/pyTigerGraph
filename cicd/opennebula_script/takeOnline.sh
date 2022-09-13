#!/bin/bash
cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
source ${cwd}/util.sh

if [ $# -lt 1 ]; then
  echo -e "\nUsage: \t$0 log_dir image_version vmName1 ... vmName_n\n"
  exit 1
fi
curr_dir=$(cd `dirname $0` && pwd)
cd $curr_dir

log_dir=$1
[ ! -d $log_dir ] && mkdir -p $log_dir
now=$(date +"%Y-%m-%d_%H:%M:%S")
pid=$$
log=$log_dir/takeOnline_${pid}_${now}.log
touch $log

# make sure anyone can write to the log
# as both mit server and target server need access
# to log during takeOnline
chmod 777 $log

# get tigergraph user for uninstall script
# as uninstall script on target machine can't read config.json
tg_user=$(jq -r .test_machine_user ${cwd}/../jenkins_script/config/config.json)

echo "Log file create at $now" > $log
echo "vmName recieved: $@" >> $log

# before take the machine online, we need to delete gium in that machine.
# Because we need to remove the effect from its previous cluster.
test_machine_user=$(jq -r .test_machine_user ${config_file})
uninstall_script_name="uninstall_pkg.sh"
uninstall_script=$cwd/../jenkins_script/shell_script/${uninstall_script_name}
for i in "${@:3}"; do
  machine_name=$i >> $log 2>&1
  ip="$(echo $machine_name | rev | cut -d'_' -f1 | rev)"
  timeout 60 scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $uninstall_script $test_machine_user@$ip:~/
  timeout 600 ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -n $test_machine_user@$ip \
      "bash ~/${uninstall_script_name} ${tg_user} >> $log 2>&1 && rm -rf ~/${uninstall_script_name} >> $log 2>&1"
done

echo "Runing enableSlave.py ..." >> $log
python enableSlave.py "$@" >> $log 2>&1

result=$(grep "Exception:" $log | awk -F": " '{print $2}')
echo "DONE" >> $log
cd - >/dev/null 2>&1
if [ "E$result" != "E" ]; then
  echo "Failed! Enable jenkins node failed" | tee -a $log
  exit 444
else
  echo "Successfully! Enabled jenkins node" | tee -a $log
fi
