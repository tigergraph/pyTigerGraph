#!/bin/bash

token=$1

curl -s -H "Accept: application/vnd.github.v3+json" -H "Authorization: token ${token}"  https://api.github.com/user | grep login
curl -s -H "Accept: application/vnd.github.v3+json" -H "Authorization: token ${token}"  https://api.github.com/rate_limit | grep remaining | tail -1
curl -s -H "Accept: application/vnd.github.v3+json" -H "Authorization: token ${token}"  https://api.github.com/repos/tigergraph/product/contents/product_version
curl -s -H "Accept: application/vnd.github.v3+json" -H "Authorization: token ${token}"  https://raw.githubusercontent.com/tigergraph/product/tg_21.11_dev/product_version
curl -s -H "Accept: application/vnd.github.v3+json" -H "Authorization: token ${token}"  https://raw.githubusercontent.com/tigergraph/ecosys/demo_github/sample_code/src/TokenBank.cpp
