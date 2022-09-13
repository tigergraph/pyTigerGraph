#!/bin/bash

base_branch=${1:-$BASE_BRANCH}

if ! [ -f ./gworkspace.sh ]; then
  echo "Not under product root"
  exit 1
fi

git config --global user.email 'qe@tigergraph.com'
git config --global user.name 'QE'

#merge product
echo "Rebasing product with origin/${base_branch}"
git merge origin/${base_branch} --no-edit || exit 1

for repo_path in $(grep $base_branch ./config/proj.config | awk '{print $2}')
do
  cd $repo_path
  pwd
  echo "Rebasing $repo_path: $(git branch | grep '^\*')"
  git merge origin/${base_branch} --no-edit || exit 1
  cd -
done

exit 0
