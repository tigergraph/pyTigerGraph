#!/bin/bash
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

# Checks the OS to see if it's supported
function check_os(){
  OS=$1
  version=$2
  echo "OS obtained: $OS $version"
  local error_msg="Unsupported OS. Currently supported OSs: CentOS/RHEL 6.5 and above; Ubuntu 14.04, 16.04, 18.04; Debian 8"
  if [ -z "$version" ]; then
     echo "Unknown OS version. $error_msg"
     exit 1
  fi
  if [ "$OS" = "UBUNTU" ]; then
    if [ "$version" != "14.04" -a "$version" != "16.04" -a "$version" != "18.04" ]; then
      echo "$error_msg"
      exit 1
    fi
  elif [ "$OS" = "DEBIAN" ]; then
    if [ "$version" != "8" ]; then
        echo "$error_msg"
        exit 1
    fi
  elif [ "$OS" = "RHEL" ]; then
    if [[ "$version" < "6.5" ]]; then
        echo "$error_msg"
        exit 1
    fi
  else
    echo "$error_msg"
    exit 1
  fi
}

timestamp() {
  date +"%D %T"
}

#Main Script
AUTOINSTALL=false
if [[ $# -gt 1 || $# == 1 && $1 != "-i" ]]; then
  echo "Usage: $0 [-i]"
  echo "-i: automatically install missing software"
  exit 1
fi

if [[ $# == 1 && $1 == "-i" ]]; then
  AUTOINSTALL=true
fi

#Packages required for Centos/RHEL (Installed using yum)
declare -a yum_rhel=("tar" "curl" "net-tools" "cronie" "ntp" "ntpdate" "iptables" "screen"
                     "python" "gcc" "gcc-c++" "util-linux" "policycoreutils-python"
                     "sshpass" "scons" "python-pip" "zlib-devel" "pssh" "jq" "git" "cmake" "unzip"
                     "xz" "bc" "lsof" "psmisc" "nc" "autoconf" "automake" "python-devel"
                     "java-1.8.0-openjdk-devel" "openssl" "openssl-devel" "vim" "bzip2"
                     "go" "libtool" "zip" "coreutils" "docker" "aws" "llvm")

#Packages required for on Ubuntu/Debian (Installed using apt-get)
declare -a ubuntu_apt=("tar" "curl" "net-tools" "cron" "ntp" "ntpdate" "iptables" "screen"
                       "firewalld" "python" "gcc-4.8" "g++-4.8" "util-linux" "policycoreutils"
                       "sshpass" "scons" "python-pip" "zlib1g-dev" "pssh" "putty-tools" "jq" "git" "cmake" "unzip"
                       "xz-utils" "bc" "lsof" "psmisc" "netcat-openbsd" "autoconf" "automake" "python-dev"
                       "software-properties-common" "openssl" "libssl-dev" "vim" "bzip2" "golang-go" "libtool"
                       "zip" "netfilter-persistent" "coreutils" "realpath" "docker" "aws" "llvm")

#additional packages for centos6
declare -a centos6=("devtoolset-3-libasan-devel:--nogpgcheck" "realpath:realpath-1.17-1.el6.rf.x86_64.rpm")

#additional packages for centos7 and above
declare -a centos7=("firewalld" "binutils-2.27-43.base.el7.x86_64")

#Python packages required for all supported OSs (Centos/RHEL/Ubuntu/Debian) (Installed using pip)
declare -a pip=("boto3" "logging" "setuptools<45" "cryptography==2.2.2" "pyyaml" "wheel" "tornado==4.5.3" "psutil" "unirest" "requests" "matplotlib" "numpy" "pathspec" "pytest")

#Get OS of current machine
echo "$(timestamp): Getting OS..."
OSG=$(get_os)
echo "$(timestamp): OS obtained: $OSG"
OS=$(echo "$OSG" | cut -d ' ' -f 1)
VERSION=$(echo "$OSG" | cut -d ' ' -f 2)
check_os $OS $VERSION

#Install missing platform spcific software
echo "$(timestamp): Checking platform spcific packages..."
if [[ "$OS" = "UBUNTU" || "$OS" = "DEBIAN" ]]; then
  for tool in ${ubuntu_apt[*]}; do
    echo "$(timestamp): Checking tool ${tool}..."
    package=$(echo "${tool}" | cut -d ':' -f 1)
    if [[ "$tool" = *":"* ]]; then
      options=$(echo "${tool}" | cut -d ':' -f 2)
    fi
    not_found=false

    #check package with both dpkg and which
    if ! dpkg -s ${tool}; then
      not_found=true
    fi

    which_check=$(which "${tool}")
    if [[ $not_found = true && -z $which_check ]]; then
      potential_pkg=$(/usr/lib/command-not-found --ignore-installed ${tool} 2>&1 | grep apt | awk '{print $4}')
      if [[ -n $potential_pkg ]]; then
        package=$potential_pkg
      fi
    else
      #found by which
      not_found=false
    fi

    #auto install only if specified
    if [[ $not_found = true ]]; then
      if [[ -n "$options" ]]; then
        check_options=$(echo "$options" | grep "--")
        if [[ -z "$check_options" ]]; then
          package=$options
          unset options
        fi
      fi
      if [[ $AUTOINSTALL = true ]]; then
        echo "$(timestamp): Trying to install missing tool ${tool} using apt-get..."
        sudo apt-get install "$package" "$options" -y
        if [[ $? != 0 ]]; then
          echo "$(timestamp): ERROR: Install missing tool ${package} using apt-get failed! Please install it manually!"
          exit 1
        fi
        echo "$(timestamp): Successfully installed missing tool ${package}!"
      else
        echo "$(timestamp): TOOL ${tool} is MISSING. Please install it using"
        echo "sudo apt-get install $package! $options"
      fi
    else
      echo "$(timestamp): Package ${package} found..."
    fi
  done
elif [[ "$OS" = "RHEL" ]]; then
  if [[ "$VERSION" < "7" ]]; then
    yum_rhel=("${yum_rhel[@]}" "${centos6[@]}")
  else
    yum_rhel=("${yum_rhel[@]}" "${centos7[@]}")
  fi

  for tool in ${yum_rhel[*]}; do
    echo "$(timestamp): Checking tool ${tool}..."
    package=$(echo "${tool}" | cut -d ':' -f 1)
    if [[ "$tool" = *":"* ]]; then
      options=$(echo "${tool}" | cut -d ':' -f 2)
    fi
    not_found=false

    #check package with both dpkg and which
    if ! rpm -q "${tool}" &>> /dev/null; then
      not_found=true
    fi

    which_check=$(which "${tool}" 2>> /dev/null)
    if [[ $not_found = true && -z $which_check ]]; then
      potential_pkg=$(yum provides "${tool}" -v | grep "searching package" | awk '{print $3}')
      if [[ -n $potential_pkg ]]; then
        package=$potential_pkg
      fi
    else
      #found by which
      not_found=false
    fi

    #auto install only if specified
    if [[ $not_found = true ]]; then
      if [[ -n "$options" ]]; then
        check_options=$(echo "$options" | grep "--")
        if [[ -z "$check_options" ]]; then
          package=$options
          unset options
        fi
      fi
      if [[ $AUTOINSTALL = true ]]; then
        echo "$(timestamp): Trying to install missing tool ${tool} using yum..."
        sudo yum install "$package" "$options" -y
        if [[ $? != 0 ]]; then
          echo "$(timestamp): ERROR: Install missing tool ${tool} using yum failed! Please install it manually!"
          exit 1
        fi
        echo "$(timestamp): Successfully installed missing tool ${tool}!"
      else
        echo "$(timestamp): TOOL ${tool} is MISSING! Please install it using"
        echo "sudo yum install $package! $options"
      fi
    else
      echo "$(timestamp): Tool ${tool} found..."
    fi
  done
else
  echo "$(timestamp): Unknown OS, Skipping software install..."
fi

#Check missing python packages
echo "$(timestamp): Checking required python packages..."
if [[ "$OS" != "UNKNOWN" ]]; then
  for package in ${pip[*]}; do
    echo "$(timestamp): Checking python package ${package}..."
    if ! python -c \"import ${package}\" &>> /dev/null; then
      if [[ $AUTOINSTALL == true ]]; then
        echo "$(timestamp): Trying to install missing python package ${package}"
        sudo pip install "$package"
        if [[ $? != 0 ]]; then
          echo "$(timestamp): ERROR: Install missing python package failed! please install it manually!"
          exit 1
        fi
        echo "$(timestamp): Successfully installed missing package ${package}!"
      else
        echo "$(timestamp): PACKAGE ${package} is MISSING! Please install it using"
        echo "sudo pip install ${package}!"
      fi
    else
      echo "$(timestamp): Package ${package} found..."
    fi
  done
fi

#Checking java version
echo "$(timestamp): Checking java version..."
java_version=$(java -version 2>&1 | grep "openjdk version" | awk '{print $3}' | cut -d '"' -f 2 | cut -d "_" -f 1 | cut -d "." -f 1-2)
if [[ "$java_version" < "1.8" ]]; then
  echo "$(timestamp): JAVA 1.8 or above is required. You have $java_version"
fi
