#!/bin/bash

function usage() {
  echo "usage $0 [-h] <-a add/remove> <-u user_name> [-p password] [-s]"
  echo "-h displays this message"
  echo "-a specifies the action to take. It must be add or remove"
  echo "-u specifies the user_name for the user to add/remove"
  echo "-p specifies the password for the user (If not specified, no password will be used)"
  echo "-i specifies the absolute paths of the public key(s) to be added to the user (If not specified, no keys will be added.
  Note: Key must already exist in destination machine.)"
  echo "-s specifies that the user should have sudo (default: normal user)"
}

# Returns the OS of the current machine
function get_os(){
  if [ -f "/etc/apt/sources.list" ]; then
    if [ -f "/etc/lsb-release" ]; then
      os_version=$(cat /etc/lsb-release | grep  "DISTRIB_RELEASE" | cut -d= -f2)
      echo "UBUNTU $os_version"
    elif [ -f "/etc/os-release" ]; then
      os_version=$(cat /etc/os-release | grep  "VERSION_ID" | cut -d= -f2)
      os_version=${os_version//\"}  # remove all double quotes
      echo "DEBIAN $os_version"
    fi
  elif [ -d "/etc/yum.repos.d" ]; then
    os_version="$(cat /etc/system-release | grep -o '[0-9]\.[0-9]')"
    echo "RHEL $os_version"
  else
    echo "UNKOWN"
  fi
}

admin=false

#Read arguments
while getopts ":ha:u:p:i:s" arg; do
  case "$arg" in
    h)
      usage
      exit 0;
      ;;
    a)
      action=${OPTARG}
      ;;
    u)
      username=${OPTARG}
      ;;
    p)
      password=${OPTARG}
      ;;
    i)
      keys=${OPTARG}
      ;;
    s)
      admin=true
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

if [[ -z $action ]]; then
  echo "Error: please specify \"add\" or \"remove\" with the -a flag!"
  usage
  exit 1;
fi

if [[ -z $username ]]; then
  echo "Error: please specify the username with the -u flag!"
  usage
  exit 2;
fi

action_lower=$(echo "$action" | tr '[:upper:]' '[:lower:]')

if [[ $action_lower = add ]]; then
  for k in ${keys[*]}; do
    if ! [[ -e $k ]]; then
      echo "Error: Public key $k does not exist!"
      exit 3
    fi
  done

  OS=$(get_os | cut -d " " -f 1)

  if [[ -n $password ]]; then
    sudo useradd -p $(openssl passwd -1 $password) -s /bin/bash -m $username
  else
    sudo useradd -m $username
  fi

  if [[ $admin = true ]]; then
    if [[ $OS = RHEL || $OS = UBUNTU || $OS = DEBIAN ]]; then
      echo "## Allow $username user to execute any command without a password" | sudo tee -a /etc/sudoers
      echo "$username       ALL=(ALL)       NOPASSWD: ALL" | sudo tee -a /etc/sudoers
    else
      echo "Error: unknown OS, unable to add sudo user!"
      exit 4
    fi
  fi

  for k in ${keys[*]}; do

    echo "Processing key $k..."
    sshdir=/home/$username/.ssh

    if ! [[ -e $sshdir ]]; then
      sudo mkdir $sshdir
      sudo chown $username:$username $sshdir
      sudo chmod -R 700 $sshdir
      sudo chmod 755 $sshdir
    fi

    if ! [[ -e $sshdir/authorized_keys ]]; then
      sudo touch $sshdir/authorized_keys
      sudo chown $username:$username $sshdir/authorized_keys
      sudo chmod -R 600 $sshdir/authorized_keys
    fi

    echo "Adding key $k..."
    cat $k | sudo tee -a $sshdir/authorized_keys
    rm -rf $k
    echo "Key $k added successfully..."

    if [[ $OS = RHEL ]]; then
      sudo restorecon -Rv $sshdir
    fi
  done

elif [[ $action_lower = remove ]]; then
    echo "Killing all processes belonging to user $username..."
    sudo pkill -U $username
    echo "Removing user $username..."
    sudo userdel -rf $username
    echo "User $username successfully removed..."
else
  echo "Error: action must be add or remove!"
  exit 3
fi
