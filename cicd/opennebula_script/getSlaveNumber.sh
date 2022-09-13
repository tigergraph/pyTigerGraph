#!/bin/bash

function usage() {
  echo "usage: $0 [-h] <-l || -a> [-p] [-t]"
  echo "-h  displays this message"
  echo "-l  specifies the log directory to use (required)"
  echo "-a  specifies the action for the script. (Note: must be either \"number\" or \"list\".) (required)"
  echo "-p  specifies the prefixes in the machine name to search for sperated by spaces. (If not specified prefix test will be used)"
  echo "-u  specifies the machine labels to search for seperated by spaces. (If not specified no labels will be used"
}

node_labels=""

#Read arguments
while getopts ":hl:a:p:t:" arg; do
  case "$arg" in
    h)
      usage
      exit 0
      ;;
    l)
      log_dir=$OPTARG 
      ;;
    a)
      action=$OPTARG
      ;;
    p)
      node_prefixes=$OPTARG
      ;;
    t)
      node_labels=$OPTARG
      ;;
    :)
      echo "Invalid option: $OPTARG requires an argument"
      usage
      exit 1
      ;;
    *)
      error "Invalid option $arg"
      usage
      exit 1
      ;;
  esac
done

if [[ -z $log_dir || -z $action ]]; then
  echo The log directory and action for the script are required. Please sepecify them with the -l and -a flags respectively.
  usage
  exit 1
fi

if [[ -z $node_prefixes ]]; then
  node_prefixes="test"
fi

if [[ $action != "value" && $action != "list" ]]; then
  echo "Error action must be either \"value\" or \"list\"."
  usage
  exit 1
fi

curr_dir=$(cd `dirname $0` && pwd)
cd $curr_dir

[ ! -d $log_dir ] && mkdir -p $log_dir
now=$(date +"%Y-%m-%d_%H:%M:%S")
pid=$$
log=$log_dir/getSlaveNumber_${pid}_${now}.log
touch $log
echo "Log file create at $now" > $log

echo "Runing getSlaveNumber.py ..." >> $log
python getSlaveNumber.py -p "$node_prefixes" -t "$node_labels">> $log 2>&1

number=$(grep "SlaveNumber:" $log | awk -F": " '{print $2}')
machine_list=$(grep "MachineList:" $log | awk -F": " '{print $2}')

echo "Return value:" >> $log
echo "SlaveNumber: $number" >> $log
if [ "$action" = "value" ]; then
  echo "$number"
else
  echo "$machine_list"
fi

echo "DONE" >> $log
cd - >/dev/null 2>&1
