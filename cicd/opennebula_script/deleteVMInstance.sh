#!/bin/bash
if [ $# -ne 1 ]; then
  echo -e "\nUsage: \t$0 vmName\n"
  exit 1
fi
curr_dir=$(cd `dirname $0` && pwd)
cd $curr_dir

[ ! -d ~/opennebula_logs ] && mkdir ~/opennebula_logs
now=$(date +"%Y-%m-%d_%H:%M:%S")
pid=$$
log=~/opennebula_logs/deleteVMInstance_${pid}_${now}.log
touch $log
echo "Log file create at $now" > $log
vmName=$1
echo "vmName recieved: $vmName" >> $log

echo "Runing deleteSlave.py .." >> $log
python deleteSlave.py $vmName >> $log 2>&1

#echo "Compiling deleteVMInstance.java ..." >> $log
#javac -cp $curr_dir/java-jar/lib/*:$curr_dir/java-jar/jar/*:. deleteVMInstance.java >> $log 2>&1
echo "Runing deleteVMInstance ..." >> $log
java -cp $curr_dir/java-jar/lib/*:$curr_dir/java-jar/jar/*:. deleteVMInstance $vmName >> $log 2>&1

result=$(grep "Delete VM result:" $log | awk -F": " '{print $2}')
echo "Delete VM result: $result" >> $log
echo "DONE" >> $log
cd - >/dev/null 2>&1
if [ "$result" != "succeed" ]; then
  echo "Delete VM failed" >> $log
  exit 111
fi
