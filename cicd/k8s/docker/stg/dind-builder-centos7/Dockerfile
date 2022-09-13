FROM centos:7

# Install prerequisites and utilities.sudo
RUN yum -y install epel-release centos-release-scl && \
    yum -y update && yum -y install curl wget acl sudo unzip xz tmux screen vim jq bc git gdb libtool \
    sshpass uuid-runtime java-1.8.0-openjdk openssh-server openssh-client openssl openssl-devel gperf* rpcbind \
    python27-python-pip python27-python-devel gcc-c++ devtoolset-9-gcc* dpkg automake cyrus-sasl-devel && \
    yum clean all && \
    source /opt/rh/devtoolset-9/enable && \
    source /opt/rh/python27/enable && pip install --upgrade --upgrade "pip==20.3.4" && pip install --upgrade setuptools && \
    pip install pyyaml wheel tornado==4.5.3 psutil unirest requests pathspec matplotlib==2.2.4 numpy==1.14 logging setuptools cryptography==2.2.2 awscli && \
    ln -s /opt/rh/python27/root/usr/include/python2.7 /usr/local/include/python2.7 && \
    wget https://github.com/Kitware/CMake/releases/download/v3.19.3/cmake-3.19.3-Linux-x86_64.tar.gz && \
    tar xfz cmake-3.19.3-Linux-x86_64.tar.gz -C /usr/local/share && \
    rm -f cmake-3.19.3-Linux-x86_64.tar.gz


# Setup ssh server.
RUN mkdir /var/run/sshd && chmod 0755 /var/run/sshd && \
    sed -i "s/^.*X11Forwarding.*$/X11Forwarding yes/" /etc/ssh/sshd_config && \
    sed -i "s/^.*X11UseLocalhost.*$/X11UseLocalhost no/" /etc/ssh/sshd_config && \
    sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd && \
    ssh-keygen -b 1024 -t rsa -f /etc/ssh/ssh_host_key && \
    ssh-keygen -b 1024 -t rsa -f /etc/ssh/ssh_host_rsa_key && \
    ssh-keygen -b 1024 -t dsa -f /etc/ssh/ssh_host_dsa_key


# Create graphsql user.
RUN useradd -m -s /bin/bash -d /home/graphsql -u 1001 graphsql && \
    echo "graphsql:graphsql" | chpasswd &&  \
    mkdir -p /home/graphsql/{product,agent,.jenkins,.ssh} && \
    echo "export PATH=/usr/local/share/cmake-3.19.3-Linux-x86_64/bin:$PATH" >> /home/graphsql/.bashrc && \
    echo "source /opt/rh/python27/enable" >> /home/graphsql/.bashrc && \
    if [ $USE_GCC9 ];then echo "source /opt/rh/devtoolset-9/enable" >> /home/graphsql/.bashrc;fi && \
    echo -e "Host *\n    StrictHostKeyChecking no\n    UserKnownHostsFile=/dev/null\n" >> /home/graphsql/.ssh/config && \
    chown -R graphsql:graphsql /home/graphsql && chmod 777 /home/graphsql && \
    echo "graphsql   soft    nproc     unlimited" >> /etc/security/limits.d/20-nproc.conf && \
    echo "graphsql    ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers && \
    sed -i -e 's/^\(Defaults\s*requiretty\)/#--- \1/' /etc/sudoers && \
    sed -i "s?\(Defaults.*secure_path.*\)?\1:/usr/local/bin/?" /etc/sudoers

# Setup JNLP Agent
ARG VERSION=3.36
RUN curl -fsSLo /usr/local/bin/jenkins-agent ftp://192.168.11.10/builder/docker/jenkins-agent && \
    curl --create-dirs -fsSLo /usr/share/jenkins/agent.jar https://repo.jenkins-ci.org/public/org/jenkins-ci/main/remoting/${VERSION}/remoting-${VERSION}.jar && \ 
    chmod 755 /usr/share/jenkins && \
    chmod 644 /usr/share/jenkins/agent.jar && \
    ln -sf /usr/share/jenkins/agent.jar /usr/share/jenkins/slave.jar && \
    chmod +x /usr/local/bin/jenkins-agent && \
    ln -s /usr/local/bin/jenkins-agent /usr/local/bin/jenkins-slave

#Setup entrypoint
RUN curl -fsSLo /home/graphsql/start.sh ftp://192.168.11.10/builder/docker/start.sh && \
    chmod +x /home/graphsql/start.sh && \
    chown -R graphsql:graphsql /home/graphsql/start.sh

# Standard SSH port
EXPOSE 22

USER graphsql
VOLUME /home/graphsql/agent
WORKDIR /home/graphsql

# Start service
ENTRYPOINT [ "/bin/bash", "-c","/home/graphsql/start.sh"]