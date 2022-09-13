#!/bin/bash
if [ $# -ne 1 ]; then
  echo -e "\nUsage: \t$0 OS\n"
  exit 1
fi
curr_dir=$(cd `dirname $0` && pwd)
cd $curr_dir

[ ! -d ~/opennebula_logs ] && mkdir ~/opennebula_logs
now=$(date +"%Y-%m-%d_%H:%M:%S")
pid=$$
log=~/opennebula_logs/createVMInstances_${pid}_${now}.log
touch $log
echo "Log file create at $now" > $log
os=$1
echo "OS recieved: $os" >> $log

#echo "Compiling createVMInstances.java ..." >> $log
#javac -cp $curr_dir/java-jar/lib/*:$curr_dir/java-jar/jar/*:. createVMInstances.java >> $log 2>&1
echo "Runing createVMInstances ..." >> $log
java -cp $curr_dir/java-jar/lib/*:$curr_dir/java-jar/jar/*:. createVMInstances $os >> $log 2>&1

vmName=$(grep "Created one VM Instance, vmName:" $log | awk -F": " '{print $2}')
ip=$(echo $vmName | awk -F"_" '{print $3}')
echo "VM IP: $ip" >> $log

if [ "$vmName" = "none" ]; then
  echo "$vmName"
  exit 1
fi

function finish {
  # delete VM in cases of terminated or canceled
  echo "Delete VM in case trap EXIT" >> $log
  nohup bash $curr_dir/deleteVMInstance.sh $vmName >> $log 2>&1 &
  echo "Deleted VM in case trap EXIT"
}
trap finish TERM INT

ready=false
count=0
while [ "$ready" = "false" ]; do
  sleep 60
  if ping -c2 -i0.5 -W1 -q $ip >> $log 2>&1; then
    echo "the new VM: $vmName is ready to launch" >> $log
    ready=true
  fi
  count=$[$count+60]
 # if [ $count -gt 900 ]; then
    echo "launch VM past: $count [sec]" >> $log
 #   echo "none"
 #   exit 1
 # fi
done

echo "Runing addSlave.py .." >> $log
python addSlave.py $vmName >> $log 2>&1

echo "Return value:" >> $log
echo "vmName: $vmName" >> $log
echo "$vmName"   # slave_id_ip

cd - >/dev/null 2>&1
