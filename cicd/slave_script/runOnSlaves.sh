#!/bin/bash

#Needed to bypass strict host checking on remote VMs
o1="UserKnownHostsFile=/dev/null"
o2="StrictHostKeyChecking=no"
o3="PasswordAuthentication=no"
o4="ConnectTimeout=10"

function usage() {
  echo "usage: $0 [-h] [-c file_to_copy] [-d directory_to_copy_to] [-f shell_script_to_execute] [-x command_to_execute] [-u user_to_use] [-i ssh_key_to_use]"
  echo "-h  displays this message"
  echo "-c  copies the specified file(s) to all IPs in the config file"
  echo "-d  specifies the directory to copy the file to or to execute the script in (if not specified, the user's home directory (\"~\") will be used)"
  echo "-f  executes the specified shell script(s) on all IPs in the config file"
  echo "-x  executes the specified command on all IPs in the config file"
  echo "-u  specifies the user to use (if not specified, user graphsql will be used)"
  echo "-i  specifies the path of the ssh key to use (if not specified $HOME/.ssh/id_rsa will be used) (WARNING: key MUST be setup correctly on the remote servers)"
  echo "-p  specifies the file containing the parameters used for each machine when running the script specified with the -f flag."
  echo "The file must be in json format with each row in the form <IP of maching>:{<script_name>:<parameters>} or all:{<script_name>:<parameters>} if parameters for all machines are the same (if not specified no parameters are used) (if all is not specified an a ip is not found. No parameters are used for that machine.) (if a script name is not found in the file, no parameters are used for that script.)"
  echo "-o  specifies the config file containing the IPs of the machines to run on. (if not specified IPs.conf will be used)"
  echo "-P  specifies the password to use (if not specified, the key specified by -i will be used. If neither -P or -i is specified $HOME/.ssh/id_rsa will be used."
  echo "Note the password will always be tried first followed by the key in all other cases."
  echo "note: if multiple options -c, -f or -x are selected the order will be copy file(-c), execute script(-f), and then execute command(-x)"
}

check_credentials() {
  IP=$1
  USERNAME=$2
  PASSWORD=$3
  SSH_KEY=$4

  echo "Checking if user credentials are valid on machine $IP"
  if [[ $PASSWORD != "none" ]]; then
    export SSHPASS=$PASSWORD
    if ! sshpass -e ssh -o $o1 -o $o2 -qn  $USERNAME@$IP "echo" &>/dev/null; then
      echo "Cannot login to node $USERNAME@$IP by ssh with the obtained password. Trying sshkey"
    else
      echo "Login with password succeded. Will use password"
      return 1
    fi
  fi

  if [[ $SSH_KEY != "none" ]]; then
    chmod 400 $SSH_KEY &>/dev/null
    if ! ssh -i $SSH_KEY -o $o1 -o $o2 -o $o3 -qn $USERNAME@$IP; then
      echo "Cannot login to node $USERNAME@$IP by ssh with key file \"$SSH_KEY\"."
      return 3
    fi
    if ! ssh -i $SSH_KEY -o $o1 -o $o2 -o $o3 -qn $USERNAME@$IP "/usr/bin/sudo -n echo" &>/dev/null; then
      echo "Login to node $USERNAME@$IP successfully, but the user doesn't have sudo access or doesn't enable NOPASSWD for all commands in /etc/sudoers."
      return 3
    fi
    echo "Login with ssh key succeded. Will use ssh key"
    return 2
  fi

  echo "One of ssh or password must be specified."
  return 3
}

#Default values for parameters
file=""
directory="~"
script=""
cmd=""
user="graphsql"
ip_config_file='IPs.conf'
param_config_file=''
key=""
password="none"

#Read arguments
while getopts ":hc:d:f:x:u:i:p:o:P:" arg; do
  case "$arg" in
    h)
      usage
      exit 0;
      ;;
    c)
      file=${OPTARG}
      ;;
    d)
      directory=${OPTARG}
      ;;
    f)
      script=${OPTARG}
      ;;
    x)
      cmd=${OPTARG}
      ;;
    u)
      user=${OPTARG}
      ;;
    i)
      key=${OPTARG}
      ;;
    p)
      param_config_file=${OPTARG}
      ;;
    o)
      ip_config_file=${OPTARG}
      ;;
    P)
      password=${OPTARG}
      ;;
    :)
      echo "Invalid option: $OPTARG requires an argument"
      usage
      exit 1
      ;;
    *)
      echo "Invalid option $arg"
      usage
      exit 1;
      ;;
  esac
done

#check to see that at least one of -c, -f or -x is specified
if [[ -z "$file" && -z "$script" && -z "$cmd" ]]; then
  echo "Please specify at least one of -c, -f or -x"
  usage
  exit 1
fi

#Check if specified config file exists
if [[ !(-f $ip_config_file) ]]; then
  echo "File $ip_config_file does not exist. Please specify an existing one using the -o option."
  usage
  exit 1
fi

#Check if specified parameter config file exists
if [[ -n $param_config_file && !(-f $param_config_file) ]]; then
  echo "File $param_config_file does not exist"
  usage
  exit 1
fi

#Check if option -f is used if -p is used
if [[ -n $param_config_file && -z $script ]]; then
  echo "-p and -f must be used together"
  usage
  exit 1
fi

#Check if custom key is valid
if [[ -n "$key" && !(-e $key) ]]; then
  echo "Specified ssh key (${key}) does NOT exist."
  usage
  exit 1
fi

#check if sshpass is installed if password is used
if [[ $password != "none" && ! $(which sshpass) ]]; then
  echo "sshpass is NOT installed. Please install sshpass or don't use -P option and try again"
  exit 1
fi

#set default key if custom key is NOT used
if [[ -z $key ]]; then
  key="/home/$user/.ssh/id_rsa"
fi

echo user: $user
echo ssh_key: $key
echo config_file: $ip_config_file
echo parameters_file: $param_config_file
echo file: $file
echo directory: $directory
echo script: $script
echo cmd: $cmd

#copy file first
if [ -n "$file" ]; then
  echo "----------Copying files----------"
  while read -r line; do
    if [[ !($line =~ ^"#".) ]]; then
      check_credentials $line $user $password $key
      use_credential=$?
      if [[ $use_credential != 3 ]]; then
        echo "----------On machine $line----------"
        for i in ${file[*]}; do
          echo "Copying file ${i}..."
          if [[ $use_credential = 1 ]]; then
            sshpass -e scp -o $o1 -o $o2 "$i" $user@$line:$directory
          else
            scp -i $key -o $o1 -o $o2 "$i" $user@$line:$directory
          fi
          if [ $? = 0 ]; then
            echo "Successfully copied file $i to machine ${line}."
          else
            echo "Failed to copy file $i to machine ${line}."
          fi
        done
      else
        echo "Unable to login to machine $line with both password and key. Skipping..."
      fi
      echo
    fi
  done < $ip_config_file
  echo "----------End copying files----------"
  echo
fi

#run script second
if [ -n "$script" ]; then
  has_params="false"
  same_params="false"
  if [[ -n $param_config_file ]]; then
    has_params="true"
    params=$(jq -r '."all"' $param_config_file)
    if [[ $params != "null" ]]; then
      same_params="true"
    fi
  fi
  echo "----------Running scripts----------"
  while read -r line; do
    if [[ !($line =~ ^"#".) ]]; then
      echo "----------On machine $line----------"
      check_credentials $line $user $password $key
      use_credential=$?
      if [[ $use_credential != 3 ]]; then
        for i in ${script[*]}; do
          echo "Running script ${i}..."
          if [[ $use_credential = 1 ]]; then
            sshpass -e scp -o $o1 -o $o2 -o $o4 "$i" $user@$line:$directory
          else
            scp -i $key -o $o1 -o $o2 -o $o3 -o $o4 "$i" $user@$line:$directory
          fi

          #get real script name if path is given
          if [[ $i = *"/"* ]]; then
            script_name=$(echo $i | rev | cut -d "/" -f 1 | rev)
          else
            script_name=$i
          fi

          script_has_params="false"
          #for each script check to see if script has params
          if [[ $has_params = "true" ]]; then
            if [[ $same_params = "true" ]]; then
              params=$(jq -r --arg script_name "$script_name" '.["all"][$script_name]' $param_config_file)
            else
              params=$(jq -r --arg line "$line" --arg script_name "$script_name" '.[$line][$script_name]' $param_config_file)
            fi
            if [[ $params != "null" ]]; then
              script_has_params="true"
            fi
          fi

          #If script has params, use the obtained params
          if [[ $script_has_params = "true" ]]; then
            echo "Params detected! Running script ${script_name} $params..."
            if [[ $use_credential = 1 ]]; then
              sshpass -e ssh -qn -o $o1 -o $o2 -o $o4 $user@$line "bash $directory/$script_name $params; rm -rf $directory/$script_name"
            else
              ssh -qn -i $key -o $o1 -o $o2 -o $o3 -o $o4 $user@$line "bash $directory/$script_name $params; rm -rf $directory/$script_name"
            fi
            script_has_params="false"
          else
            if [[ $use_credential = 1 ]]; then
              sshpass -e ssh -qn -o $o1 -o $o2 -o $o4 $user@$line "bash $directory/$script_name; rm -rf $directory/$script_name"
            else
              ssh -qn -i $key -o $o1 -o $o2 -o $o3  -o $o4 $user@$line "bash $directory/$script_name; rm -rf $directory/$script_name"
            fi
          fi

          if [ $? = 0 ]; then
            echo "Successfully executed script $i on machine ${line}."
          else
            echo "Failed to execute script $i on machine ${line}."
          fi
        done
      else
        echo "Unable to login to machine $line with both password and key. Skipping..."
      fi
      echo
    fi
  done < $ip_config_file
  echo "----------End running scripts----------"
  echo
fi

#run command last
if [ -n "$cmd" ]; then
  echo "----------Running command----------"
  while read -r line; do
    if [[ !($line =~ ^"#".) ]]; then
      echo "----------On machine $line----------"
      check_credentials $line $user $password $key
      use_credential=$?
      if [[ $use_credential != 3 ]]; then
        echo "Running command $cmd on machine ${line}."
        if [[ $use_credential = 1 ]]; then
          sshpass -e ssh -qn -o $o1 -o $o2 -o $o4 $user@$line "$cmd"
        else
          ssh -qn -i $key -o $o1 -o $o2 -o $o3 -o $o4 $user@$line "$cmd"
        fi
        if [ $? = 0 ]; then
          echo "Successfully ran command $cmd on machine ${line}."
        else
          echo "Failed to run command $cmd on machine ${line}."
        fi
      else
        echo "Unable to login to machine $line with both password and key. Skipping..."
      fi
      echo
    fi
  done < $ip_config_file
  echo "----------End running command----------"
fi

