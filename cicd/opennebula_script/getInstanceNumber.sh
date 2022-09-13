#!/bin/bash
curr_dir=$(cd `dirname $0` && pwd)
cd $curr_dir

[ ! -d ~/opennebula_logs ] && mkdir ~/opennebula_logs
now=$(date +"%Y-%m-%d_%H:%M:%S")
pid=$$
log=~/opennebula_logs/getInstanceNumber_${pid}_${now}.log
touch $log
echo "Log file create at $now" > $log

#echo "Compiling getInstanceNumber.java ..." >> $log
#javac -cp $curr_dir/java-jar/lib/*:$curr_dir/java-jar/jar/*:. getInstanceNumber.java >> $log 2>&1
echo "Runing getInstanceNumber ..." >> $log
java -cp $curr_dir/java-jar/lib/*:$curr_dir/java-jar/jar/*:. getInstanceNumber >> $log 2>&1

number=$(grep "InstanceNumber:" $log | awk -F": " '{print $2}')

echo "Return value:" >> $log
echo "InstanceNumber: $number" >> $log
echo "$number"   # slave_id_ip

echo "DONE" >> $log
cd - >/dev/null 2>&1
