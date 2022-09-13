#!/bin/bash
if [ $# -ne 1 ]; then
  echo -e "\nUsage: \t$0 vmName log_dir\n"
  exit 1
fi
curr_dir=$(cd `dirname $0` && pwd)
cd $curr_dir

log_dir=$2
[ ! -d $log_dir ] && mkdir -p $log_dir
now=$(date +"%Y-%m-%d_%H:%M:%S")
pid=$$
log=$log_dir/poweroffVMInstance_${pid}_${now}.log
touch $log
echo "Log file create at $now" > $log
vmName=$1
echo "vmName recieved: $vmName" >> $log

#echo "Compiling deleteVMInstance.java ..." >> $log
#javac -cp $curr_dir/java-jar/lib/*:$curr_dir/java-jar/jar/*:. deleteVMInstance.java >> $log 2>&1
echo "Runing poweroffVMInstance ..." >> $log
java -cp $curr_dir/java-jar/lib/*:$curr_dir/java-jar/jar/*:. poweroffVMInstance $vmName >> $log 2>&1

result=$(grep "Poweroff VM result:" $log | awk -F": " '{print $2}')
echo "Poweroff VM result: $result" >> $log
echo "DONE" >> $log

cd - >/dev/null 2>&1
if [ "$result" != "succeed" ]; then
  echo "Poweroff VM failed" >> $log
  exit 333
fi
