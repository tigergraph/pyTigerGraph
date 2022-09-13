#!/bin/bash
# this file is to periodically check system info: like process info, internet connection info

# delete old check internet process, script and logs, will delete those two lines later
ps aux | grep bash | grep check_internet.sh | awk '{ print $2 }' | xargs kill -9 || true
rm -rf ~/check_internet*

log_dir=~/period_check_logs
mkdir -p $log_dir
interval=$1
if [ -z "$interval" ]; then
  interval=5
fi

get_cur_time() {
  echo $(date +%Y-%m-%d:%H:%M:%S)
}

# check internet info
check_internet(){
  echo -e "---------check Internet---------\n" >> $log_file
  ping -c2 -i0.5 -W1 -q www.github.com >> $log_file 2>&1
  if [[ $? != 0 ]]; then
    echo "xxxxxxxxxxxxxxxxxxxxxxx--ping Github error $(get_cur_time)--xxxxxxxxxxxxxxxxxxxxx" >> $log_file
  fi
}

# print process info
check_ps_info(){
  echo -e "---------check ps info---------\n" >> $log_file
  ps aux | grep -e "tiger" -e "graph" -e "bash" -e "git" -e "jenkins" -e "ssh" >> $log_file 2>&1
}

counter=0
# one day is one big loop, use one log file
one_big_loop=$((60 * 60 * 24 / $interval))

while true; do
  if [[ "$(($counter % $one_big_loop))" == "0" ]]; then
    # create log file and delete log file 3 days ago when entering one big loop
    log_file=$log_dir/$(get_cur_time).log
    find ${log_dir} -type f -mtime +3 -delete
  fi
  echo -e "\n\n------------------------Start test at $(get_cur_time)--------------------\n" >> $log_file
  echo "counter is $counter" >> $log_file
  check_internet
  check_ps_info
  echo -e "----------------------End test at $(get_cur_time)-------------------------\n\n" >> $log_file

  counter=$(($counter + 1))
  sleep $interval
done
