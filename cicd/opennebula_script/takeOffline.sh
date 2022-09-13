#!/bin/bash
if [ $# -lt 3 ]; then
  echo -e "\nUsage: \t$0 log_dir vmName1 mesg1 ... vmName_n mesg_n \n"
  exit 1
fi
curr_dir=$(cd `dirname $0` && pwd)
cd $curr_dir

log_dir=$1
[ ! -d $log_dir ] && mkdir -p $log_dir
now=$(date +"%Y-%m-%d_%H:%M:%S")
pid=$$
log=$log_dir/takeOffline_${pid}_${now}.log
touch $log
echo "Log file create at $now" > $log
echo "Parameters recieved: $@" >> $log

echo "Runing disableSlave.py ..." >> $log
python disableSlave.py "$@" >> $log 2>&1
if [ $? != 0 ]; then
  echo "Parameters not match"
  exit 333
fi

result=$(grep "Exception:" $log | awk -F": " '{print $2}')
echo "DONE" >> $log
cd - >/dev/null 2>&1
if [ "E$result" != "E" ]; then
  echo "disable jenkins node failed" >> $log
  exit 333
fi
