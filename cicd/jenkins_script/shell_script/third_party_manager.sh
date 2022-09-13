#!/bin/bash

#Move third_party directory from achieve to src/third_party if
#one for given branch exists. Otherwise copy fresh third_party
#directory to src/third_party. Also, remove src/thirdparty directory
#if it exists. This need to be done BEFORE gworkspace to prevent
#gworkspace from failing.
function get_third_party_dir(){
  product_dir="$1"
  branch="$2"
  #get major and minor versions from branch
  major_version=$(echo "$branch" | cut -d "_" -f 2 | cut -d "." -f1)
  minor_version=$(echo "$branch" | cut -d "_" -f 2 | cut -d "." -f2)
  
  echo "Version is ${major_version}.${minor_version}"

  if [[ ! (-z "$branch") && "$major_version" -le "2" && "$minor_version" -lt "2" ]]; then
    if [[ -d ~/third_party_achieve && -d ~/"third_party_achieve/third_party_$branch" ]]; then
      #move third_party_$branch from ~/third_party_achieve to ${product_dir:?}/src/third_party
      echo "Found third party achieve! Getting appropriate directory for ${product_dir:?}/src/third_party..."
      echo "Replacing ${product_dir:?}/src/third_party with ~/third_party_achieve/third_party_$branch"
      mv ~/"third_party_achieve/third_party_$branch" "${product_dir:?}/src/third_party"
    else
      echo "Third party achieve or pervious third_party build for $branch does NOT exist."
      mkdir -p ~/third_party_achieve
      rm -rf "${product_dir:?}/src/third_party"
    fi

    #remove ${product_dir:?}/src/thirdparty if it exists
    if [ -e "${product_dir:?}/src/thirdparty" ]; then 
      echo "Removing ${product_dir:?}/src/thirdparty directory"
      rm -rf "${product_dir:?}/src/thirdparty"
    fi
  else
    echo "Version is greater then 2.2 skipping"
  fi
}

#Move src/third_party back to achieve AFTER cpkg to update the achieve
function update_achieve(){
  product_dir="$1"
  branch="$2"
  #get major and minor versions from branch
  major_version=$(echo "$branch" | cut -d "_" -f 2 | cut -d "." -f1)
  minor_version=$(echo "$branch" | cut -d "_" -f 2 | cut -d "." -f2)

  if [[ ! (-z "$branch") && "$major_version" -le "2" && "$minor_version" -lt "2" ]]; then
    echo "Moving ${product_dir:?}/src/third_party directory to ~/third_party_achieve/third_party_$branch" 
    mv "${product_dir:?}/src/third_party" ~/"third_party_achieve/third_party_$branch"
  else
    echo "Version is greater then 2.2 skipping"
  fi
}
