FROM ubuntu:20.04

SHELL ["/bin/bash", "-c"]
ARG DOCKER_CHANNEL=stable 
ARG Docker_Arch=x86_64 
ARG DOCKER_VERSION=19.03.5
ARG DEBUG=false
ENV DEBIAN_FRONTEND=noninteractive


RUN apt-get -qq update && \
    apt-get install -y apt-transport-https ca-certificates software-properties-common iptables gawk \
    curl sudo vim iproute2 sshpass uuid-runtime net-tools default-jre language-pack-en openssh-server openssl \
    git jq rsync cron bc python-dev gdb ntp screen pssh psmisc zip unzip lsof netcat-openbsd libssl-dev libcurl4-openssl-dev python-pycurl python3 python3-pip && \
    apt-get purge --auto-remove && \
    apt-get clean && \
    rm -rf /var/lib/apt/list/*

RUN curl https://bootstrap.pypa.io/pip/2.7/get-pip.py --output get-pip.py && \
    python get-pip.py 'pip < 19.1' && \
    rm -f get-pip.py && \
    pip install pyyaml wheel tornado==4.5.3 psutil unirest requests pathspec matplotlib==2.2.4 numpy==1.14 logging setuptools cryptography==2.2.2 awscli pytest && \
    pip3 install requests pathspec awscli pycurl kubernetes numpy pytz jira python-jenkins

# Docker installation

RUN set -eux && \
    curl -s -k -L "https://download.docker.com/linux/static/${DOCKER_CHANNEL}/${Docker_Arch}/docker-${DOCKER_VERSION}.tgz" -o docker.tgz && \
    tar --extract --file docker.tgz --strip-components 1 --directory /usr/local/bin/ && \
    mkdir -p /etc/docker && \
    curl -fsSLo /etc/docker/daemon.json ftp://192.168.11.10/builder/docker/daemon.json && \
    rm docker.tgz

# Setup ssh server.
RUN mkdir /var/run/sshd && chmod 0755 /var/run/sshd && \
    sed -i "s/^.*X11Forwarding.*$/X11Forwarding yes/" /etc/ssh/sshd_config && \
    sed -i "s/^.*X11UseLocalhost.*$/X11UseLocalhost no/" /etc/ssh/sshd_config && \
    sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd && \
    echo y | ssh-keygen -q -b 1024 -t rsa -N '' -f /etc/ssh/ssh_host_key && \
    echo y | ssh-keygen -q -b 1024 -t rsa -N '' -f /etc/ssh/ssh_host_rsa_key && \
    echo y | ssh-keygen -q -b 1024 -t dsa -N '' -f /etc/ssh/ssh_host_dsa_key

# Create graphsql user and set bash as the default shell.
RUN useradd -m -s /bin/bash -d /home/graphsql -u 1001 graphsql && \
    echo "graphsql:graphsql" | chpasswd && \
    mkdir -p /home/graphsql/agent /home/graphsql/.ssh && \
    echo -e "Host *\n    StrictHostKeyChecking no\n    UserKnownHostsFile=/dev/null\n" >> /home/graphsql/.ssh/config && \
    chown -R graphsql:graphsql /home/graphsql && chmod 777 /home/graphsql && \
    echo "graphsql   soft    nproc     unlimited" >> /etc/security/limits.d/90-nproc.conf &&  \
    echo "graphsql    ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers && \
    sed -i -e 's/^\(Defaults\s*requiretty\)/#--- \1/' /etc/sudoers && \
    sudo groupadd docker && \
    sudo usermod -aG docker graphsql && \
    sudo ln -sf /bin/bash /bin/sh

# Setup JNLP Agent
ARG VERSION=4.6
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
ENTRYPOINT [ "/bin/bash", "-c","/home/graphsql/start.sh"]
# ENTRYPOINT [ "/bin/bash", "-c","sudo /usr/sbin/sshd && tail -f /dev/null" ]

