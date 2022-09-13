FROM centos:8

SHELL ["/bin/bash", "-c"]
ARG DOCKER_CHANNEL=stable
ARG Docker_Arch=x86_64
ARG DOCKER_VERSION=19.03.5 
ENV DEBIAN_FRONTEND=noninteractive

# Install prerequisites and utilities.

RUN yum -y install epel-release && \
    yum -y update && yum -y install curl wget rsync acl sudo unzip xz tmux screen vim jq bc git gdb libtool iptables nc bzip2 lsof pssh psmisc chrony uuid glibc-langpack-en \
    sshpass iproute net-tools cronie java-1.8.0-openjdk openssh-server openssh-clients openssl openssl-devel gperf* rpcbind python2-devel python2-pip libcurl-devel make sysstat bind-utils && \
    yum clean all
RUN ln -sf /usr/bin/python2 /usr/bin/python && ln -sf /usr/bin/pip2 /usr/bin/pip && \
    pip install --upgrade "pip < 19.1" && \
    pip install pyyaml wheel tornado==4.5.3 psutil unirest requests pathspec matplotlib==2.2.4 numpy==1.14 logging setuptools cryptography==2.2.2 awscli pytest && \
    sudo python3 -m pip install -U pip && \
    rm -f /usr/local/bin/pip && \
    pip3 install requests pathspec awscli kubernetes numpy numpy pytz jira python-jenkins
# Setup pycurl 7.21.5
RUN cd /tmp && curl http://pycurl.sourceforge.net/download/pycurl-7.21.5/pycurl-7.21.5.tar.gz -o pycurl-7.21.5.tar.gz && \
    tar -zxvf pycurl-7.21.5.tar.gz && \
    cd pycurl-7.21.5 && python setup.py --with-openssl install && \
    rm -rf /tmp/pycurl*

# Setup ssh server.
RUN mkdir /var/run/sshd && chmod 0755 /var/run/sshd && \
    sed -i "s/^.*X11Forwarding.*$/X11Forwarding yes/" /etc/ssh/sshd_config && \
    sed -i "s/^.*X11UseLocalhost.*$/X11UseLocalhost no/" /etc/ssh/sshd_config && \
    sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd && \
    sed -i 's/AcceptEnv L/# AcceptEnv L/g' /etc/ssh/sshd_config && \
    ssh-keygen -b 1024 -t rsa -f /etc/ssh/ssh_host_key && \
    ssh-keygen -b 1024 -t rsa -f /etc/ssh/ssh_host_rsa_key && \
    ssh-keygen -b 1024 -t dsa -f /etc/ssh/ssh_host_dsa_key && \
    sudo rm -f /run/nologin

# Docker installation

RUN set -eux && \
    curl -s -k -L "https://download.docker.com/linux/static/${DOCKER_CHANNEL}/${Docker_Arch}/docker-${DOCKER_VERSION}.tgz" -o docker.tgz && \
	tar --extract --file docker.tgz --strip-components 1 --directory /usr/local/bin/ && \
    mkdir -p /etc/docker && \
    curl -fsSLo /etc/docker/daemon.json ftp://192.168.11.10/builder/docker/daemon.json && \
	rm docker.tgz 

# Create graphsql user.
RUN useradd -m -s /bin/bash -d /home/graphsql -u 1001 graphsql && \
    echo "graphsql:graphsql" | chpasswd && \
    mkdir -p /home/graphsql/{product,agent,.jenkins,.ssh} && \
    echo -e "Host *\n    StrictHostKeyChecking no\n    UserKnownHostsFile=/dev/null\n" >> /home/graphsql/.ssh/config && \
    chown -R graphsql:graphsql /home/graphsql && chmod 777 /home/graphsql && \
    echo "graphsql   soft    nproc     unlimited" >> /etc/security/limits.d/20-nproc.conf && \
    echo "graphsql    ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers && \
    sed -i -e 's/^\(Defaults\s*requiretty\)/#--- \1/' /etc/sudoers && \
    sed -i "s?\(Defaults.*secure_path.*\)?\1:/usr/local/bin/?" /etc/sudoers && \
    groupadd docker && \
    usermod -aG docker graphsql

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
EXPOSE 22 30002 19999 14240 8124

USER graphsql
VOLUME /var/lib/docker
VOLUME /home/graphsql/agent
WORKDIR /home/graphsql

# Start ssh server on container startup.
#ENTRYPOINT [ "bash", "-c", "source /home/graphsql/.bashrc && sudo /usr/sbin/sshd && jenkins-slave && sudo dockerd" ]
ENTRYPOINT [ "/bin/bash", "-c","/home/graphsql/start.sh"]
