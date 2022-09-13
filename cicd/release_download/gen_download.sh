#!/usr/bin/env bash
#Author: Chengbiao Jin

cwd=$(cd `dirname $0` && pwd)
set -e

output_file=$cwd/download.html
skip_ent=false
tg_download=$cwd/tg_downloads.txt
tab_template=$cwd/download.tab_content.template
index_template_header=$cwd/download.html.template.header
index_template_footer=$cwd/download.html.template.footer

set_opts() {
  while [ -n "$1" ];
  do
    case "$1" in
      -o|--output)
        shift
        output_file=$1
        ;;
      -e|--enterprise)
        shift
        skip_ent=$1
        ;;
      -f|--config)
        shift
        tg_download=$1
        ;;
      -h|--help|*)
        echo "Usage: $0 [ -f config_file -k output_file -e true|false ]"
        exit 0
        ;;
    esac
    shift
  done
}

set_opts $@

date

declare -A tabs
declare -A params

set_latest=true
while IFS=\| read -a line
do
  echo "Parsing ${line[*]}" 
  if [[ -z "$line" || $line =~ ^# ]]; then
    echo "skipping $line" 
    continue
  fi
  params[TAB]=$(echo ${line[0]} | tr -d ' ')
  params[VERSION]=${line[1]}
  params[EDITION]=${line[2]}
  params[LINK]=${line[3]}
  params[SIGN]=""
  params[CHECKSUM]=${line[4]}
  params[DATE]=${line[5]}
  params[LINUX]="Linux x64"
  params[PRODUCT]="TigerGraph"
  [ -z "${params[EDITION]}" ] && params[EDITION]=${params[TAB]}

  echo "Normalizing" 
  case ${params[TAB]} in
    "Developer")
      [ -z ${params[LINK]} ] && params[LINK]="https://dl.tigergraph.com/developer-edition/tigergraph-${params[VERSION]}-developer.tar.gz"
      params[SUBTITLE]=""
      ;;
    "Enterprise")
      [ -z ${params[LINK]} ] && params[LINK]="https://dl.tigergraph.com/enterprise-edition/tigergraph-${params[VERSION]}-offline.tar.gz"
      [ "$skip_ent" == "false" ] || continue
      params[SUBTITLE]=""
      if $set_latest; then
        LATEST_DOWNLOAD_EDITION=${params[LINK]}
        set_latest=false
      fi
      if $set_ent_note; then
        tabs[${params[TAB]}]="    <!---${params[TAB]}-->\n    <div id=\"${params[TAB]}\" class=\"tabcontent\">\n      <h3>For paid edition, please visit  <a href=\"https://www.tigergraph.com/get-tigergraph/\" target=\"_blank\" rel=\"noopener\"><span style=\"color: #f78117;\">https://www.tigergraph.com</span></a>  for more information.</h3>"
	#tabs[${params[TAB]}]="<div id=\"${params[TAB]}\" class=\"tabcontent\"><table><tr><h3 style=\"color: #f78117;\">For paid edition, please visit <a href=\"https://www.tigergraph.com/get-tigergraph/\">https://www.tigergraph.com</a> for more information.</h3></tr></table>"
        set_ent_note=false
      fi
      ;;
    "Docker")
      [ -z ${params[LINK]} ] && params[LINK]="https://dl.tigergraph.com/enterprise-edition/tigergraph-${params[VERSION]}-offline-docker-image.tar.gz"
      params[SUBTITLE]="Docker Image"
      if $set_doc_note; then
        tabs[${params[TAB]}]="    <!--${params[TAB]}-->\n    <div id=\"${params[TAB]}\" class=\"tabcontent\">\n      <h3>Docker image is available directly from Docker Hub: <span style=\"background-color:#27343b;color:#ffffff;font-weight:normal;\"> docker pull tigergraph/tigergraph:latest </span><br> Please visit <a href=\"https://github.com/tigergraph/ecosys/blob/master/demos/guru_scripts/docker/README.md\" target=\"_blank\" rel=\"noopener\"><span style=\"color: #f78117;\">TigerGraph Ecosystem<span></span></span></a> for detailed instructions.</h3>"
	#tabs[${params[TAB]}]="<div id=\"${params[TAB]}\" class=\"tabcontent\"><table><tr><h3 style=\"color: #f78117;\">Docker image is available directly from TigerGraph Docker Registry. </h3><p>Please visit <a href="https://github.com/tigergraph/ecosys/blob/master/demos/guru_scripts/docker/README.md">TigerGraph Ecosystem</a> for detailed instructions.</p></tr></table>"
        set_doc_note=false
      fi
      ;;
    "VMImage")
      [ -z ${params[LINK]} ] && params[LINK]="https://dl.tigergraph.com/developer-edition/tigergraph-${params[VERSION]}-developer-vm-ubuntu18.tar.gz"
      params[SUBTITLE]="VM Image"
      ;;
    "GSQLClient")
      [ -z ${params[LINK]} ] && params[LINK]="https://dl.tigergraph.com/enterprise-edition/gsql_client/tigergraph-${params[VERSION]}-gsql_client.jar"
      params[SUBTITLE]="GSQL Client"
      params[PRODUCT]="GSQL Client"
      ;;
    "MLWorkbench")
      params[LINUX]=${params[EDITION]}
      params[EDITION]="ML Workbench"
      params[SUBTITLE]=""
      params[PRODUCT]="Release"
      ;;
    *)
      params[SUBTITLE]=""
      continue
      ;;
  esac
  params[FORM_ID]=form_${params[TAB]}_$(echo ${params[VERSION]} | tr '.' '_')_$(echo ${params[LINUX]} | sed 's/ //g')

  echo "Checking ${params[LINK]}"
  if ! curl -s -I -L ${params[LINK]} > /dev/null; then
    echo "File ${params[LINK]} is not found, skipping..." 
    continue
  else
    #removing tailing control character
    content_size=$(curl -s -I -L ${params[LINK]} | grep -i content-length | cut -d' ' -f2 | tr -d '\r')
    params[FILESIZE]=$(echo "scale=2; ${content_size}/1024/1024/1024" | bc)GB
    if [[ "${params[FILESIZE]}" = "0GB" || "${params[FILESIZE]}" =~ ^\. ]]; then
      params[FILESIZE]=$(echo "scale=2; ${content_size}/1024/1024" | bc)MB
      if [[ "${params[FILESIZE]}" = "0MB" || "${params[FILESIZE]}" =~ ^\. ]]; then
        params[FILESIZE]=$(echo "scale=2; ${content_size}/1024" | bc)KB
      fi
    fi
  fi

  echo "Checking checksum" 
  if [ -z "${params[CHECKSUM]}" -o "${params[CHECKSUM]}" = "-" ]; then
    echo "Getting checksum from ${params[LINK]}" 
    if [[ "${params[LINK]}" =~ \.(sh|jar)$ ]]; then
      if curl -s -I -L ${params[LINK]%.*}.sha256sum | grep "200 OK" > /dev/null; then
        params[CHECKSUM]=$(curl -s -L ${params[LINK]%.*}.sha256sum)
      else
        params[CHECKSUM]="unavailable"
      fi
    else
      params[CHECKSUM]=$(curl -s -L ${params[LINK]/%.tar.gz/.sha256sum})
    fi
    if [ -z "${params[CHECKSUM]}" ]; then
      params[CHECKSUM]=$(curl -s -I -L ${params[LINK]} | grep ETag | sed 's/\\"//g' | cut -d\" -f4)
    fi
  fi
  if [ -z "${params[DATE]}" -o "${params[DATE]}" = "-" ]; then
    params[DATE]=$(curl -s -I -L ${params[LINK]} | grep Last-Modified | cut -d' ' -f3-5)
  fi
  if curl -s -I -L ${params[LINK]}.sig | grep "200 OK" > /dev/null; then
    params[SIGN]='<span style="text-transform: lowercase;">&nbsp;(<a href="'${params[LINK]}.sig'">sig</a>)</span>'
  fi

  echo "Working on tab content" 
  tab_content=$(cat $tab_template)
  for key in ${!params[@]}
  do
    tab_content=$(echo "$tab_content" | sed "s#\$$key#${params[$key]}#g")
  done
  if [ "$(echo ${params[CHECKSUM]} | wc -m)" -lt 60 ]; then
    tab_content=$(echo "$tab_content" | sed "s#Sha256Sum#MD5Sum#g")
  fi

  #if [[ ${line[2]} = "3.1.0" ]]; then
  #  echo $tab_content
  #fi
  
  if [ -z "${tabs[${params[TAB]}]}" ]; then
    tabs[${params[TAB]}]="<!-- Starting TAB ${params[TAB]} -->\n<div id=\"${params[TAB]}\" class=\"tabcontent\">"
  fi
  tabs[${params[TAB]}]=$(echo -e "${tabs[${params[TAB]}]}\n${tab_content}")
  #if [[ ${line[2]} = "3.1.0" ]]; then
  #  echo ${tabs[${params[TAB]}]}  
  #fi
  
done <<< "$(cat $tg_download | grep -v ^# | sort -ur)"

echo "Creating $output_file" 
tab_content=""
for key in "${!tabs[@]}"
do
  #echo ${key} 
  tab_content=$(echo -e "$tab_content\n${tabs[$key]}\n</div>")
done

cat "$index_template_header" | sed "s#\$LATEST_EDITION#$LATEST_DOWNLOAD_EDITION#g" > $output_file
echo "$tab_content" >> $output_file
cat $index_template_footer >> $output_file
