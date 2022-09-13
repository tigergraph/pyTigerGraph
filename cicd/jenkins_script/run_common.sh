#!/bin/bash

common_cwd=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)

###
# QA-2787
###

# read config json
function read_config {
  local json_file="$common_cwd/config/its_config.json"

  if [[ ! -f $json_file ]]; then
    echo "config file not found"
    exit 0
  fi

  aarr=$(jq -rc 'keys_unsorted[] as $k | [$k, ([.[$k][]] | join(" "))] | join(": ")' $json_file)

  declare -A test_map

  while IFS= read -r line; do
    regress_type=$(echo ${line} | cut -d ':' -f1 | cut -d ' ' -f 1)
    test_list=$(echo ${line} | cut -d ':' -f2)    
    test_map[$regress_type]="$test_list"
    [[ ! -z $test_list ]] && test_map[$regress_type]+=" "
  done <<< "$aarr"

  echo '('
    for key in "${!test_map[@]}" ; do
        echo "[$key]='${test_map[$key]}'"
    done
  echo ')' 
}

function gen_test_types {
  for repo in $ALL_REPOS; do
    if [[ -z "${ALL_IT[$repo]}" ]]; then
      test_types+=" $repo"
      continue
    fi

    for type in ${ALL_IT[$repo]}; do
      test_types+=" ${repo}_${type}"
    done
  done

  echo "$test_types "
}

###
# DECLARATIONS
###

declare -A ALL_IT="$(read_config)"

ALL_REPOS="${!ALL_IT[@]}"
ALL_TEST_TYPES=$(gen_test_types)

export ALL_IT=$ALL_IT
export ALL_REPOS=$ALL_REPOS
export ALL_TEST_TYPES=$ALL_TEST_TYPES

###
# SUPPORT FUNCTIONS
###

# split string by delimiter
function split_str {
  local str=$1; local delimiter=$2;
  [[ -z $delimiter ]] && delimiter=" "

  IFS="$delimiter"; arr=($str); unset IFS;
}

# returns data as associative array 
function return_dict {
  local aarr=$1

  echo '('
    for key in "${!aarr[@]}" ; do
        echo "[$key]='${aarr[$key]}'"
    done
  echo ')' 
}

# build associative array from $integration_tests
function build_test_map {
  local integration_tests="$1";

  declare -A test_map
  IFS=';' read -r -a arr <<< "$integration_tests"
  for ele in "${arr[@]}"; do    
    regress_type=$(echo ${ele} | cut -d ':' -f1 | cut -d ' ' -f 1)

    if [[ -z "${regress_type}" ]]; then
      continue
    fi

    if [[ " $ALL_TEST_TYPES " =~ " $regress_type " ]]; then
      test_list=$(echo ${ele} | cut -d ':' -f2)
      test_map[$regress_type]=" $test_list "
      [[ -z $test_list ]] && test_map[$regress_type]="$test_list"      
    fi
  done

  echo '('
    for key in "${!test_map[@]}" ; do
      echo "[$key]='${test_map[$key]}'"
    done
  echo ')' 
}

# extract repositories specified in $integrations
function get_repos {
  local integrations="$1"

  IFS=';'; arr=($integrations); unset IFS;
  for e in "${arr[@]}"; do
    head=$(echo $e | cut -d ':' -f1)
    tail=$(echo $e | cut -d ':' -f2)

    if [[ $head =~ '_' ]]; then
      repo=$(echo $head | cut -d '_' -f1)
      test_type=$(echo $head | cut -d '_' -f2)
    else
      if [[ " gst gap gus " =~ " $head " ]]; then
        repo=$head
      elif [[ " shell gquery loader docExampleTest " =~ " $head " ]]; then
        repo=gle
      else
        repo=
      fi
    fi

    if [[ ! $repos =~ $repo ]]; then
      repos+=" $repo"
    fi
  done

  if [[ ! -z $repos ]]; then
    repos+=" "
  fi

  echo $repos 
}
