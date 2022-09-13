#!/bin/bash
set -e
## create diff_stable log
log_dir=$1
diff_stable_log=${log_dir}/../diff_stable
[ -f "$diff_stable_log" ] || touch $diff_stable_log
echo -n "" > $diff_stable_log

cd $PRODUCT
version=$(cat ./product_version)
stable_tag="stable_tg_${version}_dev"
stable_version_path="/mnt/nfs_datapool/mitLogs/config/stable_version_${version}"

if [ -z "$repos" ]; then
  repos=$(cat config/proj.config | grep ^- | awk '{print $2}')
else
  repos=$(cat $repos)
fi

## print the new commits between stable tag and origin/HEAD
for repo in $(echo "$repos")
do
    echo "Check commit diff for repo $repo with stable one"
    cd $repo
    repo_name=$(grep $repo $PRODUCT/config/proj.config | awk '{print $3}')
    if [ $(git tag -l "$stable_tag") ]; then
        diff=$(git log --pretty=format:"%H %ad %aN" --date=short ${stable_tag}..origin/HEAD)
    elif [ -f "$stable_version_path" ]; then
        stable_commit=$(grep "^${repo_name}" $stable_version_path | awk '{print $3}')
        diff=$(git log --pretty=format:"%H %ad %aN" --date=short ${stable_commit}..origin/HEAD)
    else
        diff="none stable tag for this repo"
    fi
    [ -z "$diff" ] || echo -e "${repo_name}:\n${diff[@]}" >> $diff_stable_log
    cd $PRODUCT
done