#!/bin/bash
cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
cd $cwd
set -e

log_dir=$1

summary_name="unit_test_summary"
summary_file=${log_dir}/${summary_name}
echo "Generating $summary_file"
rm -rf $summary_file
for file in ${log_dir}/*/${summary_name}; do
  if [ -f $file ]; then
    echo "On machine $(basename $(dirname $file)):" >> $summary_file
    cat $file >> $summary_file
    echo -e "\n" >> $summary_file
  fi
done

summary_name="integration_test_summary"
summary_file=${log_dir}/${summary_name}
echo "Generating $summary_file"
rm -rf $summary_file
for file in ${log_dir}/*/${summary_name}; do
  if [ -f $file ]; then
    echo "On machine $(basename $(dirname $file)):" >> $summary_file
    cat $file >> $summary_file
    echo -e "\n" >> $summary_file
  fi
done

set +e
