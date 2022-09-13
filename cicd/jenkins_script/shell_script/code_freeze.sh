#!/bin/bash

branch=$1

sed -i.bak "s/\"$1\",\? *//g" /mnt/nfs_datapool/mitLogs/config/test_config/test_config.json
