FROM centos:6

# Install prerequisites and utilities.
RUN curl https://www.getpagespeed.com/files/centos6-eol.repo --output /etc/yum.repos.d/CentOS-Base.repo && \
    curl https://www.getpagespeed.com/files/centos6-epel-eol.repo --output /etc/yum.repos.d/epel.repo && \
    yum -y install centos-release-scl && \
    curl https://www.getpagespeed.com/files/centos6-scl-eol.repo --output /etc/yum.repos.d/CentOS-SCLo-scl.repo && \
    curl https://www.getpagespeed.com/files/centos6-scl-rh-eol.repo --output /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo

RUN yum -y install curl wget epel-release centos-release-scl acl \
    && yum -y install http://opensource.wandisco.com/centos/6/git/x86_64/wandisco-git-release-6-1.noarch.rpm \
    && curl -L -s http://people.centos.org/tru/devtools-2/devtools-2.repo -o /etc/yum.repos.d/devtools-2.repo \
    && yum -y update && yum -y install sudo unzip xz tmux screen vim jq bc git gdb automake autoconf libtool dpkg llvm \
    sshpass uuid-runtime java-1.8.0-openjdk openssh-server openssh-client openssl openssl-devel gperf* rpcbind \
    devtoolset-2-gcc devtoolset-2-binutils devtoolset-2-gcc-c++ devtoolset-2-libasan-devel libasan devtoolset-9 devtoolset-9-libasan-devel python27-python-pip python27-python-devel cyrus-sasl-devel \
    && source /opt/rh/python27/enable && pip install --upgrade "pip < 19.1" && pip install --upgrade "Sphinx < 2.0" "pytest < 5.0" \
    && pip install pyyaml wheel tornado==4.5.3 psutil unirest requests pathspec matplotlib==2.2.4 numpy==1.14 logging setuptools cryptography==2.2.2 awscli \
    && ln -s /opt/rh/python27/root/usr/include/python2.7 /usr/local/include/python2.7 \
    && curl -L -O http://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz \
    && tar zxf autoconf-2.69.tar.gz && cd autoconf-2.69 \
    && ./configure && make && make install \
    && yum clean all

#wget https://github.com/Kitware/CMake/releases/download/v3.15.4/cmake-3.15.4.tar.gz
#./bootstrap
#./gmake
#./gmake install
#wget https://www.python.org/ftp/python/2.7.16/Python-2.7.16.tgz
#./configure
#./make
#./make install

# Setup ssh server.
RUN mkdir /var/run/sshd && chmod 0755 /var/run/sshd \
    && sed -i "s/^.*X11Forwarding.*$/X11Forwarding yes/" /etc/ssh/sshd_config \
    && sed -i "s/^.*X11UseLocalhost.*$/X11UseLocalhost no/" /etc/ssh/sshd_config \
    && sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd \
    && ssh-keygen -b 1024 -t rsa -f /etc/ssh/ssh_host_key \
    && ssh-keygen -b 1024 -t rsa -f /etc/ssh/ssh_host_rsa_key \
    && ssh-keygen -b 1024 -t dsa -f /etc/ssh/ssh_host_dsa_key \
#should set in hoost  && echo 2 >/proc/sys/kernel/randomize_va_space \
    && wget https://github.com/Kitware/CMake/releases/download/v3.15.3/cmake-3.15.3-Linux-x86_64.tar.gz \
    && tar xfz cmake-3.15.3-Linux-x86_64.tar.gz -C /usr/local/share \
    && rm -f cmake-3.15.3-Linux-x86_64.tar.gz

# Create graphsql user.
RUN useradd -m -s /bin/bash -d /home/graphsql -u 1001 graphsql \
    && echo "graphsql:graphsql" | chpasswd \
    && mkdir -p /home/graphsql/{product,agent,.jenkins,.ssh} \
    && echo "export PATH=/usr/local/share/cmake-3.15.3-Linux-x86_64/bin:$PATH" >> /home/graphsql/.bashrc \
    && echo -e "if [ -n \"\$USE_GCC9\" ]; then\n  source /opt/rh/devtoolset-9/enable\nelse\n  source /opt/rh/devtoolset-2/enable\nfi" >> /home/graphsql/.bashrc \
    && echo "source /opt/rh/python27/enable" >> /home/graphsql/.bashrc \
    && echo -e "Host *\n    StrictHostKeyChecking no\n    UserKnownHostsFile=/dev/null\n" >> /home/graphsql/.ssh/config \
    && chown -R graphsql:graphsql /home/graphsql && chmod 777 /home/graphsql \
    && echo "graphsql   soft    nproc     unlimited" >> /etc/security/limits.d/20-nproc.conf \
    && echo "graphsql    ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers \
    && sed -i -e 's/^\(Defaults\s*requiretty\)/#--- \1/' /etc/sudoers

# Setup JNLP Agent
ARG VERSION=3.36
RUN curl -fsSLo /usr/local/bin/jenkins-agent ftp://192.168.11.10/builder/docker/jenkins-agent && \
  curl --create-dirs -fsSLo /usr/share/jenkins/agent.jar https://repo.jenkins-ci.org/public/org/jenkins-ci/main/remoting/${VERSION}/remoting-${VERSION}.jar \
  && chmod 755 /usr/share/jenkins \
  && chmod 644 /usr/share/jenkins/agent.jar \
  && ln -sf /usr/share/jenkins/agent.jar /usr/share/jenkins/slave.jar \
  && chmod +x /usr/local/bin/jenkins-agent \
  && ln -s /usr/local/bin/jenkins-agent /usr/local/bin/jenkins-slave

# Standard SSH port
EXPOSE 22

USER graphsql
VOLUME /home/graphsql/agent
#VOLUME /home/graphsql/.jenkins
WORKDIR /home/graphsql

# Start ssh server on container startup.
CMD [ "bash", "-c", "source ~/.bashrc && sudo /usr/sbin/sshd && jenkins-slave && tail -f /dev/null" ]
#ENTRYPOINT ["jenkins-slave"]
