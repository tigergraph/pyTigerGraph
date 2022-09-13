#!/bin/bash

hourly_test_os_config="/mnt/nfs_datapool/mitLogs/config/hourly_test_os"

[ -f $hourly_test_os_config ] || cat > $hourly_test_os_config << EOF
centos7:1
centos8:1
ubuntu18:1
ubuntu20:1
EOF

test_os=$(sort -t: -nk2 $hourly_test_os_config|head -1 |cut -f1 -d":")
current_test_num=$(sort -t: -nk2 $hourly_test_os_config|head -1 |cut -f2 -d":")
sed -i "s/${test_os}:${current_test_num}/${test_os}:$(( current_test_num + 1 ))/1" $hourly_test_os_config
echo "$test_os"