#Html styles
assets
wp-content

#Html templates
download.html.template.footer
download.html.template.header
download.tab_content.template
error.html

#Package list file
tg_downloads.txt

#Main generator
gen_download.sh

#Usage
bash ./gen_downloads.sh > build.log
aws s3api put-object --bucket tigergraph-release-download --key download.html --body download.html --cache-control "no-cache" --content-type "text/html" --acl bucket-owner-full-control --acl public-read
aws cloudfront create-invalidation --distribution-id E1BR98NHYQU9ME --paths "/*"
