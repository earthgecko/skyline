#!/bin/bash
####    skyline.dawn.sh    ####
#
# @author Gary Wilson (@earthgecko)
# @created 20180925 - Feature #2550: skyline.dawn.sh
#                     Feature #2560: skyline.dawn.ubuntu-16.04.4.sh
#                     Feature #2550: skyline.dawn.centos6.sh
#                     Feature #2562: skyline.dawn.centos7.sh
# @modified 20180915 - Feature #2550: skyline.dawn.sh - added skyline user
#                      Task #2596: Build Skyline on nodes at v1.2.8
# @modified 20181018 - Task #2596: Build Skyline on nodes at v1.2.8
# @modified 20190412 - Task #2926: Update dependencies
# @modified
# @license
# @source https://github.com/earthgecko/skyline/utils/dawn/skyline.dawn.sh
#
####
# README
#
# It is IMPORTANT to note this is for TESTING PURPOSES ONLY, there is no
# iptables management implemented here and it is assumed that the resulting
# built server will be disposed of.
#
# DO NOT USE THIS IN PRODUCTION.  It does not configure MySQL with the required
# innodb_file_per_table=1 as it is for testing.
#
# This script installs and sets up the required components to run and test
# Skyline on CentOS 6.9, CentOS 7.5 and Ubuntu 16.04.4 LTS.  Currently the user
# DOES NEED TO provide some information:
# 1. Provide the USER DEFINED VARIABLES the below by modifying them here OR
#    you can write them to a /etc/skyline/skyline.dawn.conf and they will be
#    sourced from this file.
# 2. If this is run on Ubuntu some user interaction may be required during
#    apt-get -y upgrade

#### USER DEFINED VARIABLES ####
# Please replace the values here and populate these variables as appropriate
# with the values of YOUR set up or write them to /etc/skyline/skyline.dawn.conf
# to be sourced.
YOUR_SERVER_IP_ADDRESS="$(ifconfig eth0 | grep "inet addr" | cut -d ':' -f2)"  # YOUR Skyline server public IP address
YOUR_SKYLINE_SERVER_FQDN="skyline-test.example.com"        # YOUR Skyline server FQDN
YOUR_EMAIL="me@example.com"                                # YOUR email address for the httpd server admin
YOUR_OTHER_IP_ADDRESS="127.0.0.1"                          # YOUR current public IP address that you will be connecting from
WEBAPP_AUTH_USER="admin"                                   # The username you want to use for http authentication
WEBAPP_AUTH_USER_PASSWORD="$(echo {$HOSTNAME}_skyline)"    # The password you want to use for http authentication
MYSQL_ROOT_PASSWORD="set_the-root-mysql-user-password"     # The MySQL root user password
MYSQL_SKYLINE_PASSWORD="set_the-skyline-user-db-password"  # The Skyline DB user password
REDIS_PASSWORD="set_really_long_LONG-Redis-password"       # The Redis password
SKYLINE_RELEASE="v1.2.18"                                  # The Skyline release to deploy

STARTED=$(date)
#### Check if the user added variables in /etc/skyline/skyline.dawn.conf ####
if [ -f /etc/skyline/skyline.dawn.conf ]; then
  echo "found user conf - /etc/skyline/skyline.dawn.conf, running bash debug on it"
  # Run bash debug on the variable to check for proper syntax
  bash -n /etc/skyline/skyline.dawn.conf
  BASH_DEBUG_EXIT_CODE=$?
  if [ $BASH_DEBUG_EXIT_CODE -ne 0 ]; then
    echo "error :: the varibles defined in /etc/skyline/skyline.dawn.conf are not valid"
    exit 1
  fi
  source /etc/skyline/skyline.dawn.conf
  echo "source user conf - /etc/skyline/skyline.dawn.conf, OK"
  sleep 1
fi

STARTED=$(date)
STOP_HERE="none"
if [ -n "$1" ]; then
  if [ "$1" == "stop_here=before_skyline_requirements" ]; then
    STOP_HERE="before_skyline_requirements"
  fi
fi

#### STATIC VARIABLES ####
# @modified 20190412 - Task #2926: Update dependencies
# Update to redis-4.0.14
#REDIS_VERSION="redis-3.2.12"
REDIS_VERSION="redis-4.0.14"
# @modified 20190412 - Task #2926: Update dependencies
# Update to Python-2.7.16
#PYTHON_VERSION="2.7.14"
PYTHON_VERSION="2.7.16"
PYTHON_MAJOR_VERSION="2.7"
PYTHON_VIRTUALENV_DIR="/opt/python_virtualenv"
# @modified 20190412 - Task #2926: Update dependencies
#PROJECT="skyline-py2714"
PROJECT="skyline-py2716"
VIRTUALENV_VERSION="15.2.0"

#### Check USER DEFINED VARIABLES ####
echo "sanity checking USER DEFINED VARIABLES"
ABORT=0
if [ "$YOUR_SERVER_IP_ADDRESS" == "127.0.0.1" ]; then
  echo "error :: please set the YOUR_SERVER_IP_ADDRESS in USER DEFINED VARIABLES"
  ABORT=1
fi
if [ "$YOUR_SKYLINE_SERVER_FQDN" == "skyline-test.example.com" ]; then
  echo "error :: please set the YOUR_SKYLINE_SERVER_FQDN in USER DEFINED VARIABLES"
  ABORT=1
fi
if [ "$YOUR_EMAIL" == "me@example.com" ]; then
  echo "error :: please set the YOUR_EMAIL in USER DEFINED VARIABLES"
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

#### yum ####
if [ "$OS" == "CentOS" ]; then
  if [ ! -f /tmp/skyline.dawn.yum.update.run.txt ]; then
    echo "Running yum updates"
    sleep 1
    yum -y update
    echo "True" > /tmp/skyline.dawn.yum.update.run.txt
  else
    echo "Skipping running yum updates, already done"
    sleep 1
  fi
fi
if [ "$OS" == "Ubuntu" ]; then
  if [ ! -f /tmp/skyline.dawn.apt-get.update.run.txt ]; then
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
    echo "True" > /tmp/skyline.dawn.apt-get.update.run.txt
  else
    echo "Skipping running apt-get update and upgrade, already done"
    sleep 1
  fi
fi


#### MySQL ####
if [ "$OS" == "CentOS" ]; then
  if [ "$OS_MAJOR_VERSION" == "6" ]; then
    if [ ! -f /tmp/skyline.dawn.yum.mysql-server.install.run.txt ]; then
      echo "Installing mysql-server"
      sleep 1
      yum -y install mysql-server
      /sbin/chkconfig --levels 235 mysqld on
# @modified 20180915 - Feature #2550: skyline.dawn.sh - added skyline user
#                      Task #2596: Build Skyline on nodes at v1.2.8
# Added innodb_file_per_table
#      sed -i 's/\[mysqld\]/\[mysqld\]\nbind-address = 127.0.0.1/g' /etc/my.cnf
      sed -i 's/\[mysqld\]/\[mysqld\]\nbind-address = 127.0.0.1\ninnodb_file_per_table/g' /etc/my.cnf
      echo "Starting mysqld"
      service mysqld restart
      MYSQL_START_EXIT_CODE=$?
      if [ $MYSQL_START_EXIT_CODE -ne 0 ]; then
        echo "error :: mysqld failed to start"
        exit 1
      fi
      echo "True" > /tmp/skyline.dawn.yum.mysql-server.install.run.txt
    else
      echo "Skipping installing mysql-server, already done"
      sleep 1
    fi
    #echo "Running mysql_secure_installation interactive USER INPUT is required"
    #mysql_secure_installation
    #MYSQL_SECURE_EXIT_CODE=$?
    #if [ $MYSQL_SECURE_EXIT_CODE -ne 0 ]; then
    #  echo "error :: mysql_secure_installation failed"
    #  exit 1
    #fi
  fi
  if [ "$OS_MAJOR_VERSION" == "7" ]; then
    if [ ! -f /tmp/skyline.dawn.yum.mariadb-server.install.run.txt ]; then
      echo "Installing mariadb-server"
      sleep 1
      yum -y install mariadb-server
# @modified 20180915 - Feature #2550: skyline.dawn.sh - added skyline user
#                      Task #2596: Build Skyline on nodes at v1.2.8
# Added innodb_file_per_table
#      sed -i 's/\[mysqld\]/\[mysqld\]\nbind-address = 127.0.0.1/g' /etc/my.cnf.d/server.cnf
      sed -i 's/\[mysqld\]/\[mysqld\]\nbind-address = 127.0.0.1\ninnodb_file_per_table=1/g' /etc/my.cnf.d/server.cnf
      systemctl start mariadb
      MYSQL_START_EXIT_CODE=$?
      if [ $MYSQL_START_EXIT_CODE -ne 0 ]; then
        echo "error :: mysqld failed to start"
        exit 1
      fi
      systemctl enable mariadb
      echo "True" > /tmp/skyline.dawn.yum.mariadb-server.install.run.txt
    else
      echo "Skipping installing mariadb-server, already done"
      sleep 1
    fi
  fi
fi

if [ "$OS" == "Ubuntu" ]; then
  if [ ! -f /tmp/skyline.dawn.apt-get.mysql-server.install.run.txt ]; then
    if [ "$OS_MAJOR_VERSION" == "16.04" ]; then
      # Install MySQL Server in a Non-Interactive mode with blank root password
      DEBIAN_PRIORITY=critical apt-get -y install mysql-server
      echo "innodb_file_per_table=1" >> /etc/mysql/mysql.conf.d/mysqld.cnf
      systemctl start mysql
      MYSQL_START_EXIT_CODE=$?
      if [ $MYSQL_START_EXIT_CODE -ne 0 ]; then
        echo "error :: mysql failed to start"
        exit 1
      fi
    fi
  fi
fi

#### Secure MySQL ####
if [ ! -f /tmp/skyline.dawn.secure.mysql.txt ]; then
  echo "Securing MySQL"
  sleep 1
  echo "Setting MySQL root user password"
  if [[ "$OS" == "CentOS" && "$OS_MAJOR_VERSION" == "6" ]]; then
    # MySQL 5.1 method
    mysql -e "UPDATE mysql.user SET Password = PASSWORD('$MYSQL_ROOT_PASSWORD') WHERE User = 'root'"
  else
    mysql -e "SET PASSWORD FOR 'root'@'localhost' = PASSWORD('$MYSQL_ROOT_PASSWORD');"
  fi
  MYSQL_EXIT_CODE=$?
  if [ $MYSQL_EXIT_CODE -ne 0 ]; then
    echo "error :: failed to set MySQL root password"
    exit 1
  fi
  if [[ "$OS" == "CentOS" && "$OS_MAJOR_VERSION" == "6" ]]; then
    # MySQL 5.1 method
    MYSQL_COMMAND="mysql -e"
  else
    MYSQL_COMMAND="mysql -u root -p$MYSQL_ROOT_PASSWORD -e"
  fi

  if [ "$OS" == "CentOS" ]; then
    echo "Dropping anonymous MySQL user at localhost"
    $MYSQL_COMMAND "DROP USER ''@'localhost'"
    MYSQL_EXIT_CODE=$?
    if [ $MYSQL_EXIT_CODE -ne 0 ]; then
      echo "error :: failed to drop anonymous MySQL user from localhost"
      exit 1
    fi
  fi

  if [[ "$OS" == "CentOS" && "$OS_MAJOR_VERSION" == "6" ]]; then
    echo "Dropping anonymous MySQL user at $HOSTNAME"
    $MYSQL_COMMAND "DROP USER ''@'$(hostname)'"
    MYSQL_EXIT_CODE=$?
    if [ $MYSQL_EXIT_CODE -ne 0 ]; then
      echo "error :: failed to drop anonymous MySQL user from $HOSTNAME"
      exit 1
    fi
  fi
  if [ "$OS" == "CentOS" ]; then
    echo "Dropping test MySQL database"
    $MYSQL_COMMAND "DROP DATABASE test"
    MYSQL_EXIT_CODE=$?
    if [ $MYSQL_EXIT_CODE -ne 0 ]; then
      echo "error :: failed to drop test MySQL database"
      exit 1
    fi
  fi
  echo "flushing MySQL privileges"
  $MYSQL_COMMAND "FLUSH PRIVILEGES"
  MYSQL_EXIT_CODE=$?
  if [ $MYSQL_EXIT_CODE -ne 0 ]; then
    echo "error :: failed flush MySQL privileges"
    exit 1
  fi
  echo "True" > /tmp/skyline.dawn.secure.mysql.txt
else
  echo "Skipping securing MySQL, already done."
  sleep 1
fi

#### Redis ####
if [ ! -f /tmp/skyline.dawn.redis.make.txt ]; then
  echo "Installing requirements to build Redis from source and making"
  sleep 1
  if [ "$OS" == "CentOS" ]; then
    yum -y install wget make gcc
  else
    apt-get -y install wget make gcc
  fi

  mkdir -p /var/dump  # For Redis dumps, not required by skyline user
  mkdir -p /opt/redis
  cd /opt/redis || exit
  if [ ! -f "/opt/redis/${REDIS_VERSION}.tar.gz" ]; then
    echo "Fetching http://download.redis.io/releases/${REDIS_VERSION}.tar.gz"
    wget "http://download.redis.io/releases/${REDIS_VERSION}.tar.gz"
    WGET_EXIT_CODE=$?
    if [ $WGET_EXIT_CODE -ne 0 ]; then
      echo "error :: failed to download Redis source from http://download.redis.io/releases/${REDIS_VERSION}.tar.gz"
      exit 1
    fi
  fi
  if [ ! -f "/opt/redis/${REDIS_VERSION}.tar.gz" ]; then
    echo "error :: Redis source not found - /opt/redis/${REDIS_VERSION}.tar.gz"
    exit 1
  fi
  if [ ! -d "/opt/redis/${REDIS_VERSION}" ]; then
    echo "unpacking Redis source tarball /opt/redis/${REDIS_VERSION}.tar.gz"
    tar xzf "${REDIS_VERSION}.tar.gz"
    TAR_EXIT_CODE=$?
    if [ $TAR_EXIT_CODE -ne 0 ]; then
      echo "error :: tar failed to unpack Redis source - /opt/redis/${REDIS_VERSION}.tar.gz"
      exit 1
    fi
  fi
  cd "/opt/redis/${REDIS_VERSION}" || exit
  echo "Running make in /opt/redis/${REDIS_VERSION}"
  make
  MAKE_EXIT_CODE=$?
  if [ $MAKE_EXIT_CODE -ne 0 ]; then
    echo "error :: failed to make Redis in /opt/redis/${REDIS_VERSION}"
    exit 1
  fi
  # Optionally here if you have the time or interest you can run
  # make test
  echo "True" > /tmp/skyline.dawn.redis.make.txt
else
  echo "Skipping installing requirements to build Redis from source and making, already done."
  sleep 1
fi

if [ ! -f /tmp/skyline.dawn.redis.make.install.txt ]; then
  cd "/opt/redis/${REDIS_VERSION}" || exit
  echo "Running make install in /opt/redis/${REDIS_VERSION}"
  sleep 1
  make install
  MAKE_INSTALL_EXIT_CODE=$?
  if [ $MAKE_INSTALL_EXIT_CODE -ne 0 ]; then
    echo "error :: failed to make install for Redis in /opt/redis/${REDIS_VERSION}"
    exit 1
  fi
  # Optionally here if you have the time or interest you can run
  # make test
  echo "True" > /tmp/skyline.dawn.redis.make.install.txt
else
  echo "Skipping running make install in /opt/redis/${REDIS_VERSION}, already done."
  sleep 1
fi

if [ ! -f /tmp/skyline.dawn.redis.install_server.txt ]; then
  echo "Installing Redis, running /opt/redis/${REDIS_VERSION}/utils/install_server.sh"
  sleep 1
  # NOTE: there are suppposed to be six BLANK line here, the represent Enter X 6
  /opt/redis/${REDIS_VERSION}/utils/install_server.sh <<EOF






EOF
  REDIS_INSTALL_EXIT_CODE=$?
  if [ $REDIS_INSTALL_EXIT_CODE -ne 0 ]; then
    echo "error :: /opt/redis/${REDIS_VERSION}/utils/install_server.sh failed"
    exit 1
  fi
  cat /etc/redis/6379.conf > /etc/redis/6379.conf.original.no.unixsocket
  cat /etc/redis/6379.conf.original.no.unixsocket \
    | sed -e 's/# unixsocketperm 700/# unixsocketperm 700\nunixsocket \/tmp\/redis\.sock\nunixsocketperm 777/1' \
    | sed -e 's/# requirepass foobared/# requirepass foobared\nrequirepass '$REDIS_PASSWORD'/1' \
    > /etc/redis/6379.conf
  sed -i 's/CLIEXEC -p/CLIEXEC -a '$REDIS_PASSWORD' -p/g' /etc/init.d/redis_6379
  /etc/init.d/redis_6379 restart
  REDIS_START_EXIT_CODE=$?
  if [ $REDIS_START_EXIT_CODE -ne 0 ]; then
    echo "error :: Redis failed to start with /etc/init.d/redis_6379 restart"
    exit 1
  fi
  echo "Checking Redis is running"
  REDIS_PROCESS=$(ps aux | grep -v grep | grep -c "/usr/local/bin/redis-server 127.0.0.1:6379")
  if [ $REDIS_PROCESS -eq 0 ]; then
    echo "error :: Redis is not running"
    exit 1
  fi
  echo "True" > /tmp/skyline.dawn.redis.install_server.txt
else
  echo "Skipping installing Redis by running /opt/redis/${REDIS_VERSION}/utils/install_server.sh, already done."
  sleep 1
fi

#### memcached ####
if [ ! -f /tmp/skyline.dawn.memcached.install.txt ]; then
  echo "Installing and starting memcached"
  sleep 1
  if [ "$OS" == "CentOS" ]; then
    yum -y install libevent libevent-devel memcached libmemcached
    YUM_EXIT_CODE=$?
    if [ $YUM_EXIT_CODE -ne 0 ]; then
      echo "error :: yum failed to install libevent libevent-devel memcached libmemcached"
      exit 1
    fi
    echo 'PORT="11211"
  USER="memcached"
  MAXCONN="1024"
  CACHESIZE="256"
  OPTIONS="-l 127.0.0.1"' > /etc/sysconfig/memcached
    if [ "$OS_MAJOR_VERSION" == "6" ]; then
      /etc/init.d/memcached restart
    else
      systemctl start memcached
    fi
    MEMCACHE_INIT_EXIT_CODE=$?
    if [ $MEMCACHE_INIT_EXIT_CODE -ne  0 ]; then
      echo "error :: /etc/init.d/memcached failed to start memcached"
      exit 1
    fi
    echo "True" > /tmp/skyline.dawn.memcached.install.txt
  else
    apt-get -y install memcached libmemcached-tools libevent-dev
    sed -i 's/-m 64/#-m 64\n-m 256/g' /etc/memcached.conf
    systemctl restart memcached
  fi
else
  echo "Skipping installing and starting memcached, already done."
  sleep 1
fi

#### Python virtualenv #####
if [ ! -f "${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/bin/python${PYTHON_MAJOR_VERSION}" ]; then
  echo "Installing alternative Python $PYTHON_VERSION, this take a little while"
  sleep 4
  if [ "$OS" == "CentOS" ]; then
    # epel-release is required before all the build deps as it provide python-pip
    yum -y install epel-release
    yum -y install autoconf zlib-devel openssl-devel sqlite-devel bzip2-devel \
      gcc gcc-c++ readline-devel ncurses-devel gdbm-devel compat-readline5 \
      freetype-devel libpng-devel python-pip wget tar git
    YUM_EXIT_CODE=$?
    if [ $YUM_EXIT_CODE -ne 0 ]; then
      echo "error :: yum failed to install requirements to build Python"
      exit 1
    fi
  fi
  if [ "$OS" == "Ubuntu" ]; then
    apt-get -y install build-essential
    apt-get -y install autoconf zlib1g-dev libssl-dev libsqlite3-dev libbz2-dev \
      libreadline6-dev libgdbm-dev libncurses5 libncurses5-dev libncursesw5 \
      libfreetype6-dev libxft-dev python-pip wget tar git
# @added 20190129 - Info #2826: pandas and numpy no longer supporting Python 2.7
# On Ubuntu 16.04 Python 3.7 requires libffi-dev as per
# https://github.com/pyenv/pyenv/issues/1183
    apt-get -y install libffi-dev
  fi
  pip install virtualenv==${VIRTUALENV_VERSION}
  PIP_EXIT_CODE=$?
  if [ $PIP_EXIT_CODE -ne 0 ]; then
    echo "error :: pip failed to install virtualenv==${VIRTUALENV_VERSION}"
    exit 1
  fi
  mkdir -p "${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}"
  mkdir -p "${PYTHON_VIRTUALENV_DIR}/projects"
  cd "${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}" || exit 1
  wget -q "https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz"
  WGET_EXIT_CODE=$?
  if [ $WGET_EXIT_CODE -ne 0 ]; then
    echo "error :: failed to download https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz"
    exit 1
  fi
  if [ ! -f "${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz" ]; then
    echo "error :: Python source not found - ${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz"
    exit 1
  fi
  tar -zxvf "Python-${PYTHON_VERSION}.tgz"
  cd ${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/Python-${PYTHON_VERSION} || exit 1
  ./configure --prefix=${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}
  if [ $? -ne 0 ]; then
    echo "error :: Python configure failed"
    exit 1
  fi
  make
  if [ $? -ne 0 ]; then
    echo "error :: Python make failed"
    exit 1
  fi
  # Optionally here if you have the time or interest you can run
  # make test
  make altinstall
  if [ $? -ne 0 ]; then
    echo "error :: Python make altinstall failed"
    exit 1
  fi
else
  echo "Skipping, installing alternative Python $PYTHON_VERSION, already done."
  sleep 1
fi

#### skyline user ####
# @added 20180915 - Feature #2550: skyline.dawn.sh
# Added the skyline user
SKYLINE_USER=$(cat /etc/passwd | grep -c skyline)
if [ $SKYLINE_USER -eq 0 ]; then
  echo "Creating the skyline /sbin/nologin user"
  if [ "$OS" == "CentOS" ]; then
    adduser --system --shell /sbin/nologin --home-dir /opt/skyline skyline
  fi
  if [ "$OS" == "Ubuntu" ]; then
    useradd --system --shell /sbin/nologin --home-dir /opt/skyline skyline
  fi
else
  echo "Skipping, creating the skyline /sbin/nologin user, already exists."
  sleep 1
fi

#### Create a Skyline Python virtualenv ####
if [ ! -f "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}/bin/python${PYTHON_MAJOR_VERSION}" ]; then
  echo "Setting up the Skyline virtualenv with permissions for the skyline user"
  sleep 1
  cd "${PYTHON_VIRTUALENV_DIR}/projects" || exit 1
  virtualenv --python="${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/bin/python${PYTHON_MAJOR_VERSION}" "$PROJECT"
  chown skyline:skyline -R "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
else
  echo "Skipping, setting up the Skyline virtualenv, already done."
  sleep 1
fi

#### Skyline ####
echo "Creating the required Skyline directories and setting permissions for the skyline user"
mkdir -p /var/log/skyline
chown skyline:skyline -R /var/log/skyline

mkdir -p /var/run/skyline
chown skyline:skyline -R /var/run/skyline

mkdir -p /opt/skyline/panorama/check
mkdir -p /opt/skyline/mirage/check
mkdir -p /opt/skyline/crucible/check
mkdir -p /opt/skyline/crucible/data
mkdir -p /opt/skyline/ionosphere/check
chown skyline:skyline -R /opt/skyline
mkdir -p /tmp/skyline
chown skyline:skyline -R /tmp/skyline

mkdir -p /etc/skyline  # skyline user does not requirement permissions on this

# Run skyline as skyline user, yes but how?
# https://unix.stackexchange.com/questions/347358/how-to-change-service-user-in-centos-7
# https://medium.com/@benmorel/creating-a-linux-service-with-systemd-611b5c8b91d6
#

# Clone Skyline
if [ ! -d /opt/skyline/github/skyline/.git ]; then
  echo "Cloning Skyline"
  sleep 1
  mkdir -p /opt/skyline/github
  cd /opt/skyline/github || exit 1
  git clone https://github.com/earthgecko/skyline.git
# @added 20180915 - Feature #2550: skyline.dawn.sh
# Added permissions for skyline user
  chown skyline:skyline -R /opt/skyline/github
else
  chown skyline:skyline -R /opt/skyline/github
  echo "Skipping cloning Skyline, already done."
fi

if [ ! -f "/tmp/skyline.dawn.skyline.${SKYLINE_RELEASE}.txt" ]; then
  echo "Checking out Skyline at $SKYLINE_RELEASE"
  sleep 1
  cd /opt/skyline/github/skyline || exit 1
  git checkout $SKYLINE_RELEASE
  if [ $? -ne 0 ]; then
    echo "error :: failed to check out Skyline at $SKYLINE_RELEASE"
    exit 1
  fi
  echo "True" > /tmp/skyline.dawn.${SKYLINE_RELEASE}.install.txt
# @added 20180915 - Feature #2550: skyline.dawn.sh
# Added permissions for skyline user
  chown skyline:skyline -R /opt/skyline/github
  /bin/cp -f /opt/skyline/github/skyline/etc/skyline.conf /etc/skyline/skyline.conf
else
  if [ ! -f /etc/skyline/skyline.conf ]; then
    /bin/cp -f /opt/skyline/github/skyline/etc/skyline.conf /etc/skyline/skyline.conf
  fi
  echo "Skipping checking out Skyline at $SKYLINE_RELEASE, already done."
  sleep 1
fi
# @added 20180915 - Feature #2550: skyline.dawn.sh
# Added permissions for skyline user
chown skyline:skyline -R /opt/skyline/github

if [ "$STOP_HERE" == "before_skyline_requirements" ]; then
  echo "Stopping before Skyline running Skyline requirements after Skyline checkout so you can modified files"
  echo "After which just run the $0 again without passing the stop_here=before_skyline_requirements Stopping before Skyline running Skyline requirements after Skyline checkout so you can modified files"
  ENDED=$(date)
  echo "$0"
  echo "started at :: $STARTED"
  echo "ended at :: $ENDED"
  echo "Skyline test deployment completed successfully."
  exit 0
fi


if [ ! -f /tmp/skyline.dawn.skyline.requirements.txt ]; then
  echo "Installing Skyline requirements.txt"
  cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}" || exit 1
  source bin/activate

  # As of statsmodels 0.9.0 scipy, et al need to be installed before
  # statsmodels in requirements
  # https://github.com/statsmodels/statsmodels/issues/4654
  bin/"pip${PYTHON_MAJOR_VERSION}" install $(cat /opt/skyline/github/skyline/requirements.txt | grep "^numpy\|^scipy\|^patsy" | tr '\n' ' ')
  bin/"pip${PYTHON_MAJOR_VERSION}" install $(cat /opt/skyline/github/skyline/requirements.txt | grep "^pandas")

  # @added 20190412 - Task #2926: Update dependencies
  #                   Bug #2590: mysql-connector-python - use_pure
  # mysql-connector-python needs to be fixed to 8.0.6 on CentOS 6 as it uses
  # MySQL 5.1 rpm from mainstream, as of mysql-connector-python 8.0.11 support
  # for 5.1 was dropped and results in a bad handshake error.
  if [ "$OS" == "CentOS" ]; then
    if [ "$OS_MAJOR_VERSION" == "6" ]; then
      echo "Replacing mysql-connector-python version in requirements.txt as CentOS 6 requires mysql-connector-python==8.0.6"
      cat /opt/skyline/github/skyline/requirements.txt > /opt/skyline/github/skyline/requirements.txt.original
      cat /opt/skyline/github/skyline/requirements.txt.original | sed -e 's/^mysql-connector-python==.*/mysql-connector-python==8\.0\.6/g' > /opt/skyline/github/skyline/requirements.txt.centos6
      cat /opt/skyline/github/skyline/requirements.txt.centos6 > /opt/skyline/github/skyline/requirements.txt
    fi
  fi

  # This can take lots of minutes...
  bin/"pip${PYTHON_MAJOR_VERSION}" install -r /opt/skyline/github/skyline/requirements.txt
  if [ $? -ne 0 ]; then
    echo "error :: failed to install Skyline requirements.txt"
    exit 1
  fi
  echo "True" > /tmp/skyline.dawn.skyline.requirements.txt
  deactivate
# @added 20180915 - Feature #2550: skyline.dawn.sh
# Added permissions for skyline user
  chown skyline:skyline -R "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
else
  echo "Skipping installing Skyline requirements.txt, already done."
fi

#### Apache ####
cd /tmp || exit 1
if [ "$OS" == "CentOS" ]; then
  if [[ ! -f /etc/httpd/conf/httpd.conf || ! -f /usr/lib64/httpd/modules/mod_ssl.so ]]; then
    echo "Installing httpd and mod_ssl"
    sleep 1
    yum -y install httpd mod_ssl
  else
    echo "Skipping installing httpd and mod_ssl, already done."
    sleep 1
  fi
fi
if [ "$OS" == "Ubuntu" ]; then
  if [ ! -f /etc/apache2/apache2.conf ]; then
    echo "Installing httpd and mod_ssl"
    sleep 1
    apt-get -y install apache2 apache2-utils
  else
    echo "Skipping installing apache2, already done."
    sleep 1
  fi
fi
if [ "$OS" == "CentOS" ]; then
  APACHE_NAME="httpd"
else
  APACHE_NAME="apache2"
fi

# Self signed ssl cert
mkdir -p /etc/$APACHE_NAME/ssl
# NOTE: USER INPUT IS REQUIRED HERE
#openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout /etc/httpd/ssl/apache.key -out /etc/httpd/ssl/apache.crt
# No user input required
if [[ ! -f /etc/$APACHE_NAME/ssl/apache.key || ! -f /etc/$APACHE_NAME/ssl/apache.crt ]]; then
  echo "Creating self signed SSL certificate for $YOUR_SKYLINE_SERVER_FQDN"
  sleep 1
  openssl req -new -newkey rsa:4096 -days 365 -nodes -x509 \
    -subj "/C=US/ST=None/L=None/O=Testing/CN=$YOUR_SKYLINE_SERVER_FQDN" \
    -keyout /etc/$APACHE_NAME/ssl/apache.key -out /etc/$APACHE_NAME/ssl/apache.crt
  if [ $? -ne 0 ]; then
    echo "error :: openssl failed to create self signed SSL certificate for $YOUR_SKYLINE_SERVER_FQDN"
    exit 1
  fi
else
  echo "Skipping creation of self signed SSL certificate for $YOUR_SKYLINE_SERVER_FQDN, already done."
  sleep 1
fi

if [ "$OS" == "CentOS" ]; then
  HTTP_AUTH_FILE="/etc/httpd/conf.d/.skyline_htpasswd"
else
  HTTP_AUTH_FILE="/etc/apache2/.skyline_htpasswd"
fi
if [ ! -f $HTTP_AUTH_FILE ]; then
  echo "Creating http auth file - $HTTP_AUTH_FILE"
  sleep 1
  htpasswd -b -c $HTTP_AUTH_FILE $WEBAPP_AUTH_USER $WEBAPP_AUTH_USER_PASSWORD
  if [ $? -ne 0 ]; then
    echo "error :: htpasswd failed to create $HTTP_AUTH_FILE"
    exit 1
  fi
else
  echo "Creating http auth file - $HTTP_AUTH_FILE"
  sleep 1
fi

if [ "$OS" == "CentOS" ]; then
  SKYLINE_HTTP_CONF_FILE="/etc/httpd/conf.d/skyline.conf"
else
  SKYLINE_HTTP_CONF_FILE="/etc/apache2/sites-available/skyline.conf"
fi
if [ ! -f $SKYLINE_HTTP_CONF_FILE ]; then
  echo "Creating http config - $SKYLINE_HTTP_CONF_FILE"
  sleep 1
  YOUR_ERROR_LOG="\/var\/log\/${APACHE_NAME}\/skyline.error.log"
  YOUR_CUSTOM_LOG="\/var\/log\/${APACHE_NAME}\/skyline.access.log"
  YOUR_PATH_TO_YOUR_CERTIFICATE_FILE="\/etc\/${APACHE_NAME}\/ssl\/apache.crt"
  YOUR_PATH_TO_YOUR_KEY_FILE="\/etc\/${APACHE_NAME}\/ssl\/apache.key"
  if [ "$OS" == "CentOS" ]; then
    YOUR_HTPASSWD_FILE="\/etc\/${APACHE_NAME}\/conf.d\/.skyline_htpasswd"
  else
    YOUR_HTPASSWD_FILE="\/etc\/${APACHE_NAME}\/.skyline_htpasswd"
  fi

  cat /opt/skyline/github/skyline/etc/skyline.httpd.conf.d.example \
    | sed -e 's/<YOUR_SERVER_IP_ADDRESS>/'$YOUR_SERVER_IP_ADDRESS'/g' \
    | sed -e 's/<YOUR_SKYLINE_SERVER_FQDN>/'$YOUR_SKYLINE_SERVER_FQDN'/g' \
    | sed -e 's/<YOUR_EMAIL>/'$YOUR_EMAIL'/g' \
    | sed -e 's/<YOUR_ERROR_LOG>/'$YOUR_ERROR_LOG'/g' \
    | sed -e 's/"<YOUR_CUSTOM_LOG>"/"'$YOUR_CUSTOM_LOG'" combined/g' \
    | sed -e 's/<YOUR_PATH_TO_YOUR_CERTIFICATE_FILE>/'$YOUR_PATH_TO_YOUR_CERTIFICATE_FILE'/g' \
    | sed -e 's/<YOUR_PATH_TO_YOUR_KEY_FILE>/'$YOUR_PATH_TO_YOUR_KEY_FILE'/g' \
    | sed -e 's/SSLCertificateChainFile/#SSLCertificateChainFile/g' \
    | sed -e 's/<YOUR_HTPASSWD_FILE>/'$YOUR_HTPASSWD_FILE'/g' \
    | sed -e 's/<YOUR_OTHER_IP_ADDRESS>/'$YOUR_OTHER_IP_ADDRESS'/g' > $SKYLINE_HTTP_CONF_FILE
else
  echo "Skipping creating http config - $SKYLINE_HTTP_CONF_FILE, already done."
fi
# Blank the standard SSL conf
if [ "$OS" == "CentOS" ]; then
  echo "#" > /etc/httpd/conf.d/ssl.conf
  if [ $CENTOS_6 -eq 1 ]; then
    /etc/init.d/httpd restart
  else
    systemctl restart httpd
  fi
  if [ $? -ne 0 ]; then
    echo "error :: failed to restart httpd"
    exit 1
  fi
else
  a2enmod ssl
  a2enmod proxy
  a2enmod proxy_http
  a2enmod headers
  a2enmod rewrite
  sed -i 's/.*IfModule.*//g;s/.*LoadModule.*//g' /etc/apache2/sites-available/skyline.conf
  a2ensite skyline.conf
  systemctl restart apache2
fi

#### Skyline settings ####
if [ ! -f /opt/skyline/github/skyline/skyline/settings.py.original ]; then
  echo "Populating variables in the Skyline settings.py"
  sleep 1
  cat /opt/skyline/github/skyline/skyline/settings.py > /opt/skyline/github/skyline/skyline/settings.py.original
# @modified 20180823 - Bug #2552: seed_data.py testing with UDP does not work (GH77)
# Only requires a public IP if Grpahite is going to pickle to it, but seeing as
# this is a test node, make it 127.0.0.1 as there are no iptables on the IP or
# ports 2025 or 2024
#    | sed -e "s/HORIZON_IP = .*/HORIZON_IP = '$YOUR_SERVER_IP_ADDRESS'/g" \
  cat /opt/skyline/github/skyline/skyline/settings.py.original \
    | sed -e "s/REDIS_PASSWORD = .*/REDIS_PASSWORD = '$REDIS_PASSWORD'/g" \
    | sed -e 's/PANORAMA_ENABLED = .*/PANORAMA_ENABLED = True/g' \
    | sed -e "s/WEBAPP_AUTH_USER = .*/WEBAPP_AUTH_USER = '$WEBAPP_AUTH_USER'/g" \
    | sed -e 's/PANORAMA_ENABLED = .*/PANORAMA_ENABLED = True/g' \
    | sed -e "s/WEBAPP_AUTH_USER_PASSWORD = .*/WEBAPP_AUTH_USER_PASSWORD = '$WEBAPP_AUTH_USER_PASSWORD'/g" \
    | sed -e "s/WEBAPP_ALLOWED_IPS = .*/WEBAPP_ALLOWED_IPS = ['127.0.0.1', '$YOUR_OTHER_IP_ADDRESS']/g" \
    | sed -e "s/SKYLINE_URL = .*/SKYLINE_URL = 'https:\/\/$YOUR_SKYLINE_SERVER_FQDN'/g" \
    | sed -e 's/MEMCACHE_ENABLED = .*/MEMCACHE_ENABLED = True/g' \
    | sed -e "s/PANORAMA_DBUSER = .*/PANORAMA_DBUSER = 'skyline'/g" \
    | sed -e "s/HORIZON_IP = .*/HORIZON_IP = '127.0.0.1'/g" \
    | sed -e "s/PANORAMA_DBUSERPASS = .*/PANORAMA_DBUSERPASS = '$MYSQL_SKYLINE_PASSWORD'/g" > /opt/skyline/github/skyline/skyline/settings.py
  if [ $? -ne 0 ]; then
    echo "error :: failed to populate the variables in /opt/skyline/github/skyline/skyline/settings.py"
    exit 1
  fi
else
  echo "Skipping populating variables in the Skyline settings.py, already done."
  sleep 1
fi

SKYLINE_DB_PRESENT=$(mysql -u root -p"$MYSQL_ROOT_PASSWORD" -sss -e "SHOW DATABASES" | grep -c skyline)
if [ $SKYLINE_DB_PRESENT -eq 0 ]; then
  echo "Deploying Skyline SQL schema"
  sleep 1
  mysql -u root -p"$MYSQL_ROOT_PASSWORD" < /opt/skyline/github/skyline/skyline/skyline.sql
  if [ $? -ne 0 ]; then
    echo "error :: failed to deploy Skyline SQL schema"
    exit 1
  fi
else
  echo "Skipping deploying Skyline SQL schema, already done."
  sleep 1
fi

SKYLINE_DB_USER_PRESENT=$(mysql -u root -p"$MYSQL_ROOT_PASSWORD" -sss -e "SELECT User FROM mysql.user" | sort | uniq | grep -c skyline)
if [ $SKYLINE_DB_USER_PRESENT -eq 0 ]; then
  echo "Creating skyline MySQL user and permissions"
  sleep 1
  mysql -u root -p"$MYSQL_ROOT_PASSWORD" -e "GRANT ALL ON skyline.* TO 'skyline'@'localhost' IDENTIFIED BY '$MYSQL_SKYLINE_PASSWORD'; \
FLUSH PRIVILEGES;"
  if [ $? -ne 0 ]; then
    echo "error :: failed to create skyline MySQL user"
    exit 1
  fi
else
  echo "Skipping creating skyline MySQL user and permissions, already exists."
  sleep 1
fi

echo "Starting Skyline services"
sleep 1
SERVICES="/opt/skyline/github/skyline/bin/horizon.d
/opt/skyline/github/skyline/bin/panorama.d
/opt/skyline/github/skyline/bin/analyzer.d
/opt/skyline/github/skyline/bin/webapp.d
/opt/skyline/github/skyline/bin/ionosphere.d
/opt/skyline/github/skyline/bin/luminosity.d
/opt/skyline/github/skyline/bin/boundary.d"
for i_service in $SERVICES
do
# @modified 20180915 - Feature #2550: skyline.dawn.sh
# Use the skyline user
#  $i_service start
  sudo -u skyline $i_service start
  if [ $? -ne 0 ]; then
    echo "error :: failed to start $i_service"
    exit 1
  fi
done

echo "Skyline services started"
sleep 1
ps aux | grep -v grep | grep skyline

echo "Seeding Skyline with data"
sleep 2
cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}" || exit 1
source bin/activate
bin/python${PYTHON_MAJOR_VERSION} /opt/skyline/github/skyline/utils/seed_data.py
deactivate
cd /tmp || exit

# @added 20181018 - Task #2596: Build Skyline on nodes at v1.2.8
# SELinux prevents Apache from initiating outbound connections
if [ "$OS" == "CentOS" ]; then
  if [ $CENTOS_7 -eq 1 ]; then
    /usr/sbin/setsebool -P httpd_can_network_connect 1
  fi
fi

if [ "$OS" == "CentOS" ]; then
  if [ $CENTOS_6 -eq 1 ]; then
    /etc/init.d/httpd restart
  else
    systemctl restart httpd
  fi
else
  systemctl restart apache2
fi

echo "Skyline is deployed and running"
echo "Please visit https://$YOUR_SKYLINE_SERVER_FQDN"
echo "And view the logs in /var/log/skyline"
echo "Or do
# How are they running
tail /var/log/skyline/*.log
# Any errors - each app
find /var/log/skyline -type f -name \"*.log\" | while read skyline_logfile
do
  echo \"#####
# Checking for errors in \$skyline_logfile\"
  cat \"\$skyline_logfile\" | grep -B2 -A10 -i \"error ::\|traceback\" | tail -n 60
  echo \"\"
  echo \"\"
done"
echo ""
echo "To stop the Skyline processes run:"
echo "kill \$(ps aux | grep -v grep | grep skyline | tr -s ' ' ',' | cut -d',' -f2 | tr '\n' ' ')"
echo ""
ENDED=$(date)
echo "$0"
echo "started at :: $STARTED"
echo "ended at :: $ENDED"
echo "Skyline test deployment completed successfully."
echo ""
echo ""
echo "REMEMBER THIS IS A TEST DEPLOYMENT AND SHOULD BE DESTROYED, there are no firewall rules and no inits"
# @modified 20180915 - Feature #2550: skyline.dawn.sh
# Use the skyline user
# echo "and Skyline is running as root"
echo "There is no GRAPHITE_HOST configured and there are some errors expected in the logs"
echo "related to no rows in MySQL tables and memcache, etc"
echo ""
echo "Please visit https://$YOUR_SKYLINE_SERVER_FQDN (rememebr to add it to your hosts file)"
echo "In the now and Panaroma tabs you should see an anomaly and in rebrow you should see Redis keys"
echo ""
echo "NOT FOR PRODUCTION"
echo ""
echo ""
# @added 20190412 - Task #2926: Update dependencies
#                   Bug #2590: mysql-connector-python - use_pure
# Report known mysql-connector-python 8.0.6 vulnerablity on CentOS 6
if [ "$OS" == "CentOS" ]; then
  if [ "$OS_MAJOR_VERSION" == "6" ]; then
    echo "NOTE - on CentOS 6 mysql-connector-python has to be fixed on version 8.0.6 due to the drop of support"
    echo "       for MySQL 5.1.  mysql-connector-python-8.0.6 has reported vulnerablities"
    echo "       High severity vulnerability found on mysql-connector-python@8.0.6"
    echo "       desc: Improper Access Control"
    echo "       info: https://snyk.io/vuln/SNYK-PYTHON-MYSQLCONNECTORPYTHON-173986"
    echo "       info: https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2019-2435"
    echo "       You have been advised, so now you know"
  fi
fi
