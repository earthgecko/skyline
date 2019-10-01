#!/bin/bash
####    skyline.docker.dawn.sh    ####
#
# @author Gary Wilson (@earthgecko)
# @created 20190519 - Branch #3002: docker
# @modified
# @license
# @source https://github.com/earthgecko/skyline/utils/dawn/skyline.docker.dawn.sh
#
####
# README
#
# It is IMPORTANT to note this is for TESTING PURPOSES ONLY!!!
#
# DO NOT USE THIS IN PRODUCTION.
#
# This script installs and sets up the required components to run and test
# Skyline running in docker containers on CentOS 7.6.
#
#           ####  IMPORTANT NOTES  ####
#
# * This script is IPv4 ONLY.  IPv6 is DISABLED on the system
# * This script manages the basic requirements on firewall-cmd and firewalld
#   it allows public access to port 22, 80 and 443 and restricted access to port
#   8888 for Graphite

#### USER DEFINED VARIABLES ####
# Please replace the values here and populate these variables as appropriate
# with the values of YOUR set up or write them to /etc/skyline/skyline.dawn.conf
# to be sourced.
YOUR_SERVER_IP_ADDRESS="$(ifconfig eth0 | grep 'inet ' | sed -e 's/.*inet //1;s/ .*//g')"  # YOUR Skyline server public IP address
YOUR_OTHER_IP_ADDRESS="127.0.0.1"                          # YOUR current public IP v4 address that you will be connecting from
SKYLINE_RELEASE="v1.3.1"                                  # The Skyline release to deploy

#### STATIC VARIABLES ####
SKYLINE_DOCKER_INSTANCE_HOSTNAME="skyline-docker-skyline-1" # The hostname of the Skyline docker container

STARTED=$(date)
#### Check if the user added variables in /etc/skyline/skyline.dawn.conf ####
if [ -f /etc/skyline/skyline.docker.dawn.conf ]; then
  echo "found user conf - /etc/skyline/skyline.docker.dawn.conf, running bash debug on it"
  # Run bash debug on the variable to check for proper syntax
  bash -n /etc/skyline/skyline.docker.dawn.conf
  BASH_DEBUG_EXIT_CODE=$?
  if [ $BASH_DEBUG_EXIT_CODE -ne 0 ]; then
    echo "error :: the varibles defined in /etc/skyline/skyline.docker.dawn.conf are not valid"
    exit 1
  fi
  source /etc/skyline/skyline.docker.dawn.conf
  echo "source user conf - /etc/skyline/skyline.docker.dawn.conf, OK"
  sleep 1
fi

#### Check USER DEFINED VARIABLES ####
echo "sanity checking USER DEFINED VARIABLES"
ABORT=0
if [ "$YOUR_SERVER_IP_ADDRESS" == "127.0.0.1" ]; then
  echo "error :: please set the YOUR_SERVER_IP_ADDRESS in USER DEFINED VARIABLES"
  ABORT=1
fi
if [ "$YOUR_OTHER_IP_ADDRESS" == "127.0.0.1" ]; then
  echo "error :: please set the YOUR_OTHER_IP_ADDRESS in USER DEFINED VARIABLES"
  ABORT=1
fi
if [ $ABORT -eq 1 ]; then
  exit 1
fi
echo "USER DEFINED VARIABLES, OK"
sleep 1

### Determine OS ####
OS="unknown"
OS_MAJOR_VERSION="unknown"
if [ -f /etc/lsb-release ]; then
  source /etc/lsb-release
  if [ $DISTRIB_ID == "Ubuntu" ]; then
    OS="Ubuntu"
  fi
  if [ "$DISTRIB_RELEASE" == "16.04" ]; then
    OS_MAJOR_VERSION="$DISTRIB_RELEASE"
  fi
fi

if [ -f /etc/redhat-release ]; then
  CENTOS=$(cat /etc/redhat-release | grep -c "CentOS")
  if [ $CENTOS -eq 1 ]; then
    OS="CentOS"
    # CentOS release 6.10 (Final)
    CENTOS_6=$(cat /etc/redhat-release | grep -c "release 6")
    if [ $CENTOS_6 -eq 1 ]; then
      OS_MAJOR_VERSION="6"
    fi
    # CentOS Linux release 7.5.1804 (Core)
    CENTOS_7=$(cat /etc/redhat-release | grep -c "release 7")
    if [ $CENTOS_7 -eq 1 ]; then
      OS_MAJOR_VERSION="7"
    fi
  fi
fi

if [ "$OS" == "unknown" ]; then
  echo "error :: the operating system was not determined to be Ubuntu or CentOS"
  exit 1
fi
if [ "$OS_MAJOR_VERSION" == "unknown" ]; then
  echo "error :: this version of $OS is not supported"
  exit 1
fi

###
# ONLY SUPPORT CentOS 7.6 for now
SUPPORTED_OS=1
if [ "$OS" != "CentOS" ]; then
  SUPPORTED_OS=0
fi
if [ "$OS_MAJOR_VERSION" != "7" ]; then
  SUPPORTED_OS=0
fi
if [ "$SUPPORTED_OS" -eq 0 ]; then
  echo "error: skyline.docker.sh only supports CentOS 7.x"
  exit 1
fi

#### Disable ipv6 ####
if [ ! -f /etc/sysctl.d/01-disable.ipv6.conf ]; then
  echo "disabling IPv6"
  echo "net.ipv6.conf.all.disable_ipv6 = 1
  net.ipv6.conf.default.disable_ipv6 = 1" > /etc/sysctl.d/01-disable.ipv6.conf
  sysctl -w net.ipv6.conf.all.disable_ipv6=1
  sysctl -w net.ipv6.conf.default.disable_ipv6=1
  echo "AddressFamily inet" >> /etc/ssh/sshd_config
  systemctl restart sshd
  # Setup firewall
  yum -y install firewalld
  systemctl start firewalld
  firewall-cmd --permanent --add-service=ssh
  firewall-cmd --permanent --add-service=http
  firewall-cmd --permanent --add-service=https
  firewall-cmd --permanent --zone=public --add-rich-rule='
    rule family="ipv4"
    source address="'$YOUR_OTHER_IP_ADDRESS'/32"
    port protocol="tcp" port="8888" accept'
  firewall-cmd --reload
  systemctl enable firewalld
fi

#### yum ####
if [ "$OS" == "CentOS" ]; then
  if [ ! -f /tmp/skyline.docker.dawn.yum.update.run.txt ]; then
    echo "Running yum updates"
    sleep 1
    yum -y update
    echo "True" > /tmp/skyline.docker.dawn.yum.update.run.txt
  else
    echo "Skipping running yum updates, already done"
    sleep 1
  fi
fi
if [ "$OS" == "Ubuntu" ]; then
  if [ ! -f /tmp/skyline.docker.dawn.apt-get.update.run.txt ]; then
    echo "Running apt updates"
    sleep 1
    apt-get update
    if [ $? -ne 0 ]; then
      echo "error :: apt-get update failed"
      exit 1
    fi
    DEBIAN_PRIORITY=critical apt-get -y upgrade
    if [ $? -ne 0 ]; then
      echo "error :: apt-get -y upgrade failed"
      exit 1
    fi
    echo "True" > /tmp/skyline.docker.dawn.apt-get.update.run.txt
  else
    echo "Skipping running apt-get update and upgrade, already done"
    sleep 1
  fi
fi

#### General tools ####
yum -y install git unzip wget rsync tar gzip curl telnet

#### docker ####
yum -y install yum-utils epel-release python-pip device-mapper-persistent-data lvm2

if [ ! -f /etc/yum.repos.d/docker-ce.repo ]; then
  /usr/bin/yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
  yum -y install docker-ce
  systemctl start docker
  yum -y install docker-compose
fi

#### Skyline ####
# Clone Skyline
if [ ! -d /opt/skyline/github/skyline/.git ]; then
  echo "Cloning Skyline"
  sleep 1
  mkdir -p /opt/skyline/github
  cd /opt/skyline/github || exit 1
  git clone https://github.com/earthgecko/skyline.git
else
  echo "Skipping cloning Skyline, already done."
  sleep 1
fi

# docker containers directory
SKYLINE_DOCKER_HOME="/opt/skyline/docker"
mkdir -p $SKYLINE_DOCKER_HOME/skyline-docker-mysql-1/var/lib/mysql
mkdir -p $SKYLINE_DOCKER_HOME/skyline-docker-redis-1/var/lib/redis
mkdir -p $SKYLINE_DOCKER_HOME/skyline-docker-graphite-statsd-1/opt/graphite/storage
mkdir -p $SKYLINE_DOCKER_HOME/skyline-docker-graphite-statsd-1/var/log
mkdir -p $SKYLINE_DOCKER_HOME/skyline-docker-skyline-1/opt/skyline/ionosphere
mkdir -p $SKYLINE_DOCKER_HOME/skyline-docker-skyline-1/var/log/skyline
mkdir -p $SKYLINE_DOCKER_HOME/skyline-docker-skyline-1/var/log/apache2

if [ ! -f "/tmp/skyline.docker.dawn.skyline.${SKYLINE_RELEASE}.txt" ]; then
  echo "Checking out Skyline at $SKYLINE_RELEASE"
  sleep 1
  cd /opt/skyline/github/skyline || exit 1
  git checkout $SKYLINE_RELEASE
  if [ $? -ne 0 ]; then
    echo "error :: failed to check out Skyline at $SKYLINE_RELEASE"
    exit 1
  fi
  echo "True" > /tmp/skyline.docker.dawn.${SKYLINE_RELEASE}.install.txt
else
  echo "Skipping checking out Skyline at $SKYLINE_RELEASE, already done."
  sleep 1
fi

DOCKER_SKYLINE_INSTANCE_PATH="/opt/skyline/docker/${SKYLINE_DOCKER_INSTANCE_HOSTNAME}"
DOCKER_SKYLINE_PATH="${DOCKER_SKYLINE_INSTANCE_PATH}/skyline"
mkdir -p "$DOCKER_SKYLINE_PATH"
echo "rsyncing Skyline code to the docker container data path - $DOCKER_SKYLINE_PATH"
sleep 1
rsync -avz /opt/skyline/github/skyline/ "${DOCKER_SKYLINE_PATH}/"

cd "${DOCKER_SKYLINE_PATH}/"
docker-compose build
if [ $? -ne 0 ]; then
  echo "docker-compose build failed, that is sucky, exiting"
  exit 1
fi

echo "Running docker-compose up in the background for 3 minutes to start all the
apps and ouput the logs, then stopping and starting again as detached"
docker-compose up &
BG_PID=$!
sleep 180
echo "Stopping the backgrounded docker application."
kill $BG_PID
sleep 30
echo "Stopped the backgrounded docker application. now starting detached"
docker-compose up -d
sleep 30

#### nginx ####
echo "Installing nginx to reverse proxy the Skyline container"
yum -y install nginx

mkdir -p "/etc/nginx/ssl/${SKYLINE_DOCKER_INSTANCE_HOSTNAME}"
mkdir -p "/etc/nginx/proxy_cache/${SKYLINE_DOCKER_INSTANCE_HOSTNAME}"
chown nginx:nginx "/etc/nginx/proxy_cache/${SKYLINE_DOCKER_INSTANCE_HOSTNAME}"

# Deploy SKYLINE_DOCKER_INSTANCE_HOSTNAME SSL key to nginx
sleep 10
/usr/bin/docker exec -it "skyline_${SKYLINE_DOCKER_INSTANCE_HOSTNAME}_1" cat "/etc/apache2/ssl/${SKYLINE_DOCKER_INSTANCE_HOSTNAME}.apache.key" > "/etc/nginx/ssl/${SKYLINE_DOCKER_INSTANCE_HOSTNAME}/${SKYLINE_DOCKER_INSTANCE_HOSTNAME}.key"
/usr/bin/docker exec -it "skyline_${SKYLINE_DOCKER_INSTANCE_HOSTNAME}_1" cat "/etc/apache2/ssl/${SKYLINE_DOCKER_INSTANCE_HOSTNAME}.apache.crt" > "/etc/nginx/ssl/${SKYLINE_DOCKER_INSTANCE_HOSTNAME}/${SKYLINE_DOCKER_INSTANCE_HOSTNAME}.crt"

cat "$DOCKER_SKYLINE_PATH/utils/docker/configs/nginx/etc/nginx/conf.d/skyline-docker-skyline-1.conf" > /etc/nginx/conf.d/skyline-docker-skyline-1.conf

mkdir -p /etc/nginx/ssl/skyline-docker-graphite-statsd-1
mkdir -p /etc/nginx/proxy_cache/skyline-docker-graphite-statsd-1
chown nginx:nginx /etc/nginx/proxy_cache/skyline-docker-graphite-statsd-1
cat "$DOCKER_SKYLINE_PATH/utils/docker/configs/nginx/etc/nginx/conf.d/skyline-docker-graphite-statsd-1.conf" > /etc/nginx/conf.d/skyline-docker-graphite-statsd-1.conf
# Deploy skyline-docker-graphite-statsd-1 SSL cert
/usr/bin/openssl req -new -newkey rsa:4096 -days 365 -nodes -x509 -subj '/C=US/ST=None/L=None/O=Testing/CN=skyline-docker-graphite-statsd-1' -keyout /etc/nginx/ssl/skyline-docker-graphite-statsd-1/skyline-docker-graphite-statsd-1.key -out /etc/nginx/ssl/skyline-docker-graphite-statsd-1/skyline-docker-graphite-statsd-1.crt
cat "$DOCKER_SKYLINE_PATH/utils/docker/configs/skyline/skyline/etc/skyline/skyline.dawn.conf" | grep WEBAPP > /tmp/skyline.docker.webapp.variables.txt
source /tmp/skyline.docker.webapp.variables.txt
# Deploy skyline-docker-graphite-statsd-1 .htpasswd
echo -n "$WEBAPP_AUTH_USER:" >> /etc/nginx/conf.d/.graphite_htpasswd.skyline-docker-graphite-statsd-1 && echo "$WEBAPP_AUTH_USER_PASSWORD" | openssl passwd -stdin -apr1 >> /etc/nginx/conf.d/.graphite_htpasswd.skyline-docker-graphite-statsd-1
systemctl restart nginx

echo "The container application skyline-docker-skyline-1 has been built and started successfully.

To stop the container application run:
cd $DOCKER_SKYLINE_PATH
docker-compose stop

# To start it
cd $DOCKER_SKYLINE_PATH
docker-compose up -d"
echo "

To assess the Skyline and Graphite frontends, ensure you have the following line
in YOUR /etc/hosts"
echo "echo \"$YOUR_SERVER_IP_ADDRESS skyline-docker-skyline-1 skyline-docker-graphite-statsd-1\" >> /etc/hosts"
echo "Then for Skyline open https://skyline-docker-skyline-1 in your browser with user $WEBAPP_AUTH_USER and password $WEBAPP_AUTH_USER_PASSWORD"
echo "and for Graphite open https://skyline-docker-graphite-statsd-1 in your browser with user $WEBAPP_AUTH_USER and password $WEBAPP_AUTH_USER_PASSWORD"
