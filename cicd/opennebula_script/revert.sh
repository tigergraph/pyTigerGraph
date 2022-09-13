#!/bin/bash
if [ $# -ne 3 ]; then
  echo -e "\nUsage: \t$0 log_dir vmIP version\n"
  exit 1
fi

curr_dir=$(cd `dirname $0` && pwd)
cd $curr_dir

log_dir=$1
[ ! -d $log_dir ] && mkdir -p $log_dir
now=$(date +"%Y-%m-%d_%H:%M:%S")
pid=$$
log=$log_dir/revert_${pid}_${now}.log
touch $log
echo "Log file create at $now" > $log
vmIP=$2
echo "vmIP to revert is : $vmIP" >> $log
version=$3
[ -z $version ] && version='TigerGraph'

echo "Compiling revert.java ..." >> $log
javac -cp $curr_dir/java-jar/lib/*:$curr_dir/java-jar/jar/*:. revert.java >> $log 2>&1
echo "Runing revert ..." >> $log
java -cp $curr_dir/java-jar/lib/*:$curr_dir/java-jar/jar/*:. revert $vmIP $version >> $log 2>&1
vmName=$(grep "Switched snapshot, vmName:" $log | awk -F": " '{print $2}')
ip=$(echo $vmName | awk -F"_" '{print $3}')
echo "VM IP: $ip" >> $log

if [ "E$ip" = "E" ]; then
  echo "Switch snapshot VM failed" >> $log
  echo "none"
  exit 0
fi

ready=false
count=0
while [ "$ready" = "false" ]; do
  sleep 30
  if ping -c2 -i0.5 -W1 -q $ip >> $log 2>&1; then
    echo "the new VM: $vmName is ready to launch" >> $log
    ready=true
  fi
  count=$[$count+30]
 # if [ $count -gt 900 ]; then
    echo "launch VM past: $count [sec]" >> $log
 #   echo "none"
 #   exit 1
 # fi
done

java -jar java-jar/jenkins-cli.jar -s http://192.168.55.21:8080 connect-node \
"$vmName" --username qa_master --password Node >> $log 2>&1
if [ $? != 0 ]; then
  echo "connect-node: $vmName failed" >> $log
fi
echo "Return value:" >> $log
echo "vmName: $vmName" >> $log
echo "$vmName"   # slave_id_ip

cd - >/dev/null 2>&1
