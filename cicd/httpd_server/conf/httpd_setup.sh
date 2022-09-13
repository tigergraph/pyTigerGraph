#!/bin/bash
if [[ `whoami` != "root" ]]
then
  echo "Please run with sudo!"
  exit 1
fi

# ftp
sudo usermod -d /home/graphsql/jenkins_log ftp

# check os 
if cat /etc/*release | grep ubuntu > /dev/null
then 
  # copy httpd conf
  cp $(dirname $0)/httpd.conf.ubuntu /etc/apache2/apache2.conf
  service apache2 restart
  update-rc.d apache2 enable 

  cp $(dirname $0)/vsftpd.conf /etc/vsftpd.conf
  service vsftpd restart 

  # disable firewall
  ufw disable
else 
  # copy httpd conf
  cp $(dirname $0)/httpd.conf.centos /etc/httpd/conf/httpd.conf
  systemctl start httpd
  systemctl enable httpd

  cp $(dirname $0)/vsftpd.conf /etc/vsftpd/vsftpd.conf
  systemctl restart vsftpd
  chkconfig vsftpd on

  # disable firewall
  systemctl disable firewalld
  systemctl stop firewalld
fi


# disable iptables to allow curl ftp and httpd
# since using curl to download ftp files needs a random output port
# we have to disable whole iptables 
iptables -F
