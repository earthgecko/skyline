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
# @modified 20191016 - Branch #3262: py3
# @modified 20200703 - Task #3608: Update Skyline to Python 3.8.3 and deps
#                      Branch #3262: py3
# @modified 20201016 - Branch #3068: SNAB
# @modified 20210328 - [Q] The "horizon.test.pickle" test is getting an error. #419
# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
# @modified 20220506 - Release #4552: v3.0.2
# @modified
# @license
# @source https://github.com/earthgecko/skyline/utils/dawn/skyline.dawn.sh
#
####
# README
#
# It is IMPORTANT to note this is for TESTING PURPOSES, there is no
# iptables management implemented here and it is assumed that the resulting
# built server will be disposed of.
#
# DO NOT USE THIS IN PRODUCTION unless you configure a firewall appropriately.
#
# This script installs and sets up the required components to run and test
# Skyline on CentOS Stream 8, Ubuntu 18.04 and Ubuntu 20.04.  Currently the user
# DOES NEED TO provide some information:
# 1. Provide the USER DEFINED VARIABLES the below by modifying them here OR
#    you can write them to a /etc/skyline/skyline.dawn.conf and they will be
#    sourced from this file.
# 2. If this is run on Ubuntu some user interaction may be required during
#    apt-get -y upgrade, depending on what packages are installed and updated on
#    the base image.

#### USER DEFINED VARIABLES ####
# Please replace the values here and populate these variables as appropriate
# with the values of YOUR set up or write them to /etc/skyline/skyline.dawn.conf
# to be sourced.
YOUR_SERVER_IP_ADDRESS="YOUR_SERVER_IP"  # YOUR Skyline server public IP address
# e.g. YOUR_SERVER_IP_ADDRESS="$(ifconfig eth0 | grep "inet addr" | cut -d ':' -f2)"  # YOUR Skyline server public IP address
YOUR_SKYLINE_SERVER_FQDN="skyline-test.example.com"        # YOUR Skyline server FQDN
YOUR_OTHER_IP_ADDRESS="127.0.0.1"                          # YOUR current public IP address that you will be connecting from
WEBAPP_AUTH_USER="admin"                                   # The username you want to use for http authentication
WEBAPP_AUTH_USER_PASSWORD="$(echo ${HOSTNAME}_skyline)"    # The password you want to use for http authentication
MYSQL_ROOT_PASSWORD="set_the-root-mysql-user-password"     # The MySQL root user password
MYSQL_SKYLINE_PASSWORD="set_the-skyline-user-db-password"  # The Skyline DB user password
REDIS_PASSWORD="set_really_long_LONG-Redis-password"       # The Redis password
SKYLINE_RELEASE="v3.0.2"                                   # The Skyline release to deploy
# @added 20191016 - Branch #3262: py3
INSTALL_GRAPHITE=1                                         # Install Graphite 0 = no, 1 = yes (CentOS 8 only)
# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
FLUX_SELF_API_KEY="$(echo ${HOSTNAME}_YOURown32charSkylineAPIkeySecret | sed 's/[^a-zA-Z0-9]//g' | head -c 32)"
GRAPHITE_VERSION="1.1.8"

COLOUR_OFF='\033[0m'  # Text Reset

STARTED=$(date)
#### Check if the user added variables in /etc/skyline/skyline.dawn.conf ####
if [ -f /etc/skyline/skyline.dawn.conf ]; then
  echo "found user conf - /etc/skyline/skyline.dawn.conf, running bash debug on it"
  # Run bash debug on the variable to check for proper syntax
  bash -n /etc/skyline/skyline.dawn.conf
  BASH_DEBUG_EXIT_CODE=$?
  if [ $BASH_DEBUG_EXIT_CODE -ne 0 ]; then
    echo -e "error :: the varibles defined in /etc/skyline/skyline.dawn.conf are not valid - \e[31mFAIL $COLOUR_OFF"
    echo -e $COLOUR_OFF
    exit 1
  fi
  source /etc/skyline/skyline.dawn.conf
  echo -e "source user conf - /etc/skyline/skyline.dawn.conf, \e[32mOK $COLOUR_OFF"
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
# @modified 20200703 - Task #3608: Update Skyline to Python 3.8.3 and deps
#REDIS_VERSION="redis-4.0.14"
# @modified 20201016 - Branch #3068: SNAB
#REDIS_VERSION="redis-5.0.8"
#REDIS_VERSION="redis-6.0.8"
# Reverted to 5.0.8 as 6 requires different service files which need to be
# done
# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
#REDIS_VERSION="redis-5.0.8"
REDIS_VERSION="redis-5.0.14"

# @modified 20190412 - Task #2926: Update dependencies
# Update to Python-2.7.16
#PYTHON_VERSION="2.7.14"
# @modified 20200703 - Task #3608: Update Skyline to Python 3.8.3 and deps
#PYTHON_VERSION="3.7.6"
#PYTHON_MAJOR_VERSION="3.7"
# @modified 20201016 - Branch #3068: SNAB
#PYTHON_VERSION="3.8.3"
# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
#PYTHON_VERSION="3.8.6"
#PYTHON_MAJOR_VERSION="3.8"
PYTHON_VERSION="3.8.13"
PYTHON_MAJOR_VERSION="3.8"

PYTHON_VIRTUALENV_DIR="/opt/python_virtualenv"
# @modified 20190412 - Task #2926: Update dependencies
#PROJECT="skyline-py2714"
# @modified 20200703 - Task #3608: Update Skyline to Python 3.8.3 and deps
#PROJECT="skyline-py376"
# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
#PROJECT="skyline-py386"
PROJECT="skyline-py3813"
#VIRTUALENV_VERSION="15.2.0"
# @modified 20201016 - Branch #3068: SNAB
#VIRTUALENV_VERSION="16.7.9"
VIRTUALENV_VERSION="16.7.10"

# @modified 20200703 - Task #3608: Update Skyline to Python 3.8.3 and deps
#OPENSSL_VERSION="1.1.1d"
OPENSSL_VERSION="1.1.1g"

YOUR_EMAIL="no_email"

#### Check USER DEFINED VARIABLES ####
echo "sanity checking USER DEFINED VARIABLES"
ABORT=0
if [ "$YOUR_SERVER_IP_ADDRESS" == "127.0.0.1" ]; then
  echo -e "error :: please set the YOUR_SERVER_IP_ADDRESS in USER DEFINED VARIABLES - \e[31mFAIL $COLOUR_OFF"
  ABORT=1
fi
if [ "$YOUR_SERVER_IP_ADDRESS" == "YOUR_SERVER_IP" ]; then
  echo -e "error :: please set the YOUR_SERVER_IP_ADDRESS in USER DEFINED VARIABLES - \e[31mFAIL $COLOUR_OFF"
  ABORT=1
fi
if [ "$YOUR_SERVER_IP_ADDRESS" == "" ]; then
  echo -e "error :: please set the YOUR_SERVER_IP_ADDRESS in USER DEFINED VARIABLES as currently is not set $YOUR_SERVER_IP_ADDRESS - \e[31mFAIL $COLOUR_OFF"
  ABORT=1
fi
if [ "$YOUR_SKYLINE_SERVER_FQDN" == "skyline-test.example.com" ]; then
  echo -e "error :: please set the YOUR_SKYLINE_SERVER_FQDN in USER DEFINED VARIABLES - \e[31mFAIL $COLOUR_OFF"
  ABORT=1
fi
#if [ "$YOUR_EMAIL" == "me@example.com" ]; then
#  echo -e "error :: please set the YOUR_EMAIL in USER DEFINED VARIABLES - \e[31mFAIL $COLOUR_OFF"
#  ABORT=1
#fi
if [ "$YOUR_OTHER_IP_ADDRESS" == "127.0.0.1" ]; then
  echo -e "error :: please set the YOUR_OTHER_IP_ADDRESS in USER DEFINED VARIABLES - \e[31mFAIL $COLOUR_OFF"
  ABORT=1
fi
if [ $ABORT -eq 1 ]; then
  exit 1
fi
echo -e "USER DEFINED VARIABLES - \e[32mOK $COLOUR_OFF"
#sleep 10

### Determine OS ####
OS="unknown"
OS_MAJOR_VERSION="unknown"
if [ -f /etc/lsb-release ]; then
  source /etc/lsb-release
  if [ $DISTRIB_ID == "Ubuntu" ]; then
    OS="Ubuntu"
  fi
#  if [ "$DISTRIB_RELEASE" == "16.04" ]; then
  if [ -n "$DISTRIB_RELEASE" ]; then
    if [ "$DISTRIB_RELEASE" == "18.04" ]; then
      OS_MAJOR_VERSION="$DISTRIB_RELEASE"
    fi
    if [ "$DISTRIB_RELEASE" == "20.04" ]; then
      OS_MAJOR_VERSION="$DISTRIB_RELEASE"
    fi
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
      VIRTUALENV_VERSION="15.2.0"
    fi
    # CentOS Linux release 7.5.1804 (Core)
    CENTOS_7=$(cat /etc/redhat-release | grep -c "release 7")
    if [ $CENTOS_7 -eq 1 ]; then
      OS_MAJOR_VERSION="7"
    fi
# @added 20200703 - Task #3608: Update Skyline to Python 3.8.3 and deps
# Added CentOS 8
    CENTOS_8=$(cat /etc/redhat-release | grep -c "release 8")
    if [ $CENTOS_8 -eq 1 ]; then
      OS_MAJOR_VERSION="8"
    fi
  fi
fi

if [ "$OS" == "unknown" ]; then
  echo -e "error :: the operating system was not determined to be Ubuntu or CentOS - \e[31mFAIL $COLOUR_OFF"
  exit 1
fi
if [ "$OS_MAJOR_VERSION" == "unknown" ]; then
  echo -e "error :: version $OS_MAJOR_VERSION of $OS is not supported - \e[31mFAIL $COLOUR_OFF"
  exit 1
fi

echo -e "\e[96m####    INSTALLING SKYLINE    #### $COLOUR_OFF"
echo "This will take 20-30 minutes on a cloud server"
sleep 5

#### yum ####
echo -e "\e[96m####    INSTALLING OS UPDATES    #### $COLOUR_OFF"
sleep 1
if [ "$OS" == "CentOS" ]; then
  if [ ! -f /tmp/skyline.dawn.yum.update.run.txt ]; then
    echo "Running yum updates"
    sleep 1
    yum -y update
    echo "True" > /tmp/skyline.dawn.yum.update.run.txt
  else
    echo -e "Skipping running yum updates, already done - \e[32mOK $COLOUR_OFF"
    sleep 1
  fi
fi
if [ "$OS" == "Ubuntu" ]; then
  if [ ! -f /tmp/skyline.dawn.apt-get.update.run.txt ]; then
    echo "Running apt updates"
    sleep 1
    apt-get update
    if [ $? -ne 0 ]; then
      echo -e "error :: apt-get update failed - \e[31mFAIL $COLOUR_OFF"
      exit 1
    fi
    DEBIAN_PRIORITY=critical apt-get -y upgrade
    if [ $? -ne 0 ]; then
      echo -e "error :: apt-get -y upgrade failed - \e[31mFAIL $COLOUR_OFF"
      exit 1
    fi
    echo "True" > /tmp/skyline.dawn.apt-get.update.run.txt
  else
    echo -e "Skipping running apt-get update and upgrade, already done - \e[32mOK $COLOUR_OFF"
    sleep 1
  fi
fi


#### MySQL ####
if [ "$OS" == "CentOS" ]; then
  echo -e "\e[96m####    INSTALLING MariaDB    #### $COLOUR_OFF"
  sleep 1
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
        echo -e "error :: mysqld failed to start - \e[31mFAIL $COLOUR_OFF"
        exit 1
      fi
      echo "True" > /tmp/skyline.dawn.yum.mysql-server.install.run.txt
    else
      echo -e "Skipping installing mysql-server, already done - \e[32mOK $COLOUR_OFF"
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
# @added 20200703 - Task #3608: Update Skyline to Python 3.8.3 and deps
# Added CentOS 8
  if [[ "$OS_MAJOR_VERSION" == "7" || "$OS_MAJOR_VERSION" == "8" ]]; then
    if [ ! -f /tmp/skyline.dawn.yum.mariadb-server.install.run.txt ]; then
      echo "Installing mariadb-server"
      sleep 1
      yum -y install mariadb-server
# @modified 20180915 - Feature #2550: skyline.dawn.sh - added skyline user
#                      Task #2596: Build Skyline on nodes at v1.2.8
# Added innodb_file_per_table
#      sed -i 's/\[mysqld\]/\[mysqld\]\nbind-address = 127.0.0.1/g' /etc/my.cnf.d/server.cnf
      if [ "$OS_MAJOR_VERSION" == "7" ]; then
        sed -i 's/\[mysqld\]/\[mysqld\]\nbind-address = 127.0.0.1\ninnodb_file_per_table=1/g' /etc/my.cnf.d/server.cnf
      fi
# @added 20200703 - Task #3608: Update Skyline to Python 3.8.3 and deps
      if [ "$OS_MAJOR_VERSION" == "8" ]; then
        sed -i 's/\[mysqld\]/\[mysqld\]\nbind-address = 127.0.0.1\ninnodb_file_per_table=1/g' /etc/my.cnf.d/mariadb-server.cnf
      fi
      systemctl start mariadb
      MYSQL_START_EXIT_CODE=$?
      if [ $MYSQL_START_EXIT_CODE -ne 0 ]; then
        echo -e "error :: mysqld failed to start - \e[31mFAIL $COLOUR_OFF"
        exit 1
      fi
      systemctl enable mariadb
      echo "True" > /tmp/skyline.dawn.yum.mariadb-server.install.run.txt
    else
      echo -e "Skipping installing mariadb-server, already done - \e[32mOK $COLOUR_OFF"
      sleep 1
    fi
  fi
fi

if [ "$OS" == "Ubuntu" ]; then
  if [ ! -f /tmp/skyline.dawn.apt-get.mysql-server.install.run.txt ]; then
#    if [ "$OS_MAJOR_VERSION" == "16.04" ]; then
#    if [[ "$OS_MAJOR_VERSION" == "16.04" || "$OS_MAJOR_VERSION" == "18.04" ]]; then
    if [ "$OS_MAJOR_VERSION" == "16.04" ]; then
      # Install MySQL Server in a Non-Interactive mode with blank root password
      DEBIAN_PRIORITY=critical apt-get -y install mysql-server
      echo "innodb_file_per_table=1" >> /etc/mysql/mysql.conf.d/mysqld.cnf
      systemctl start mysql
      MYSQL_START_EXIT_CODE=$?
      if [ $MYSQL_START_EXIT_CODE -ne 0 ]; then
        echo -e "error :: mysql failed to start - \e[31mFAIL $COLOUR_OFF"
        exit 1
      fi
    fi

    if [[ "$OS_MAJOR_VERSION" == "18.04" || "$OS_MAJOR_VERSION" == "20.04" ]]; then
      echo -e "\e[96m####    INSTALLING MariaDB REPO    #### $COLOUR_OFF"
      sleep 1
      cd /root || exit 1
      wget https://downloads.mariadb.com/MariaDB/mariadb_repo_setup
      echo "b9e90cde27affc2a44f9fc60e302ccfcacf71f4ae02071f30d570e6048c28597 mariadb_repo_setup" | sha256sum -c -
      chmod +x mariadb_repo_setup
      ./mariadb_repo_setup --mariadb-server-version="mariadb-10.5"
    fi

    if [[ "$OS_MAJOR_VERSION" == "18.04" || "$OS_MAJOR_VERSION" == "20.04" ]]; then
        echo -e "\e[96m####    INSTALLING MariaDB    #### $COLOUR_OFF"
        sleep 1
        echo "Installing mariadb-server"
        sleep 1
        apt-get -y install mariadb-server
        systemctl start mariadb
        MYSQL_START_EXIT_CODE=$?
        if [ $MYSQL_START_EXIT_CODE -ne 0 ]; then
          echo -e "error :: mariadb failed to start - \e[31mFAIL $COLOUR_OFF"
          exit 1
        fi
        systemctl enable mariadb
        echo "True" > /tmp/skyline.dawn.apt-get.mysql-server.install.run.txt
    fi
  fi
fi

#### Secure MySQL ####
if [ ! -f /tmp/skyline.dawn.secure.mysql.txt ]; then
  echo "Securing MySQL"
  sleep 1
  if [ "$OS" == "CentOS" ]; then
    echo "Setting MySQL root user password"
    if [[ "$OS" == "CentOS" && "$OS_MAJOR_VERSION" == "6" ]]; then
      # MySQL 5.1 method
      mysql -e "UPDATE mysql.user SET Password = PASSWORD('$MYSQL_ROOT_PASSWORD') WHERE User = 'root'"
    else
      mysql -e "SET PASSWORD FOR 'root'@'localhost' = PASSWORD('$MYSQL_ROOT_PASSWORD');"
    fi
    MYSQL_EXIT_CODE=$?
    if [ $MYSQL_EXIT_CODE -ne 0 ]; then
      echo -e "error :: failed to set MySQL root password - \e[31mFAIL $COLOUR_OFF"
      exit 1
    fi
    if [[ "$OS" == "CentOS" && "$OS_MAJOR_VERSION" == "6" ]]; then
      # MySQL 5.1 method
      MYSQL_COMMAND="mysql -e"
    else
      MYSQL_COMMAND="mysql -u root -p$MYSQL_ROOT_PASSWORD -e"
    fi
  else
    MYSQL_COMMAND="mysql -uroot -e"
  fi

  if [ "$OS" == "CentOS" ]; then
# @modified 20200703 - Task #3608: Update Skyline to Python 3.8.3 and deps
# No anonymous MySQL user on CentOS 8 MariaDB
    if [[ "$OS_MAJOR_VERSION" == "6" || "$OS_MAJOR_VERSION" == "7" ]]; then
      echo "Dropping anonymous MySQL user at localhost"
      $MYSQL_COMMAND "DROP USER ''@'localhost'"
      MYSQL_EXIT_CODE=$?
      if [ $MYSQL_EXIT_CODE -ne 0 ]; then
        echo -e "error :: failed to drop anonymous MySQL user from localhost - \e[31mFAIL $COLOUR_OFF"
        exit 1
      fi
    fi
  fi

  if [[ "$OS" == "CentOS" && "$OS_MAJOR_VERSION" == "6" ]]; then
    echo "Dropping anonymous MySQL user at $HOSTNAME"
    $MYSQL_COMMAND "DROP USER ''@'$(hostname)'"
    MYSQL_EXIT_CODE=$?
    if [ $MYSQL_EXIT_CODE -ne 0 ]; then
      echo -e "error :: failed to drop anonymous MySQL user from $HOSTNAME - \e[31mFAIL $COLOUR_OFF"
      exit 1
    fi
  fi
  if [ "$OS" == "CentOS" ]; then
# @modified 20200703 - Task #3608: Update Skyline to Python 3.8.3 and deps
# No test database on CentOS 8 MariaDB
    if [[ "$OS_MAJOR_VERSION" == "6" || "$OS_MAJOR_VERSION" == "7" ]]; then
      echo "Dropping test MySQL database"
      $MYSQL_COMMAND "DROP DATABASE test"
      MYSQL_EXIT_CODE=$?
      if [ $MYSQL_EXIT_CODE -ne 0 ]; then
        echo -e "error :: failed to drop test MySQL database - \e[31mFAIL $COLOUR_OFF"
        exit 1
      fi
    fi
  fi
  echo "flushing MySQL privileges"
  $MYSQL_COMMAND "FLUSH PRIVILEGES"
  MYSQL_EXIT_CODE=$?
  if [ $MYSQL_EXIT_CODE -ne 0 ]; then
    echo -e "error :: failed flush MySQL privileges - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
  echo "True" > /tmp/skyline.dawn.secure.mysql.txt
else
  echo -e "Skipping securing MySQL, already done - \e[32mOK $COLOUR_OFF"
  sleep 1
fi

#### Redis ####
if [ ! -f /tmp/skyline.dawn.redis.make.txt ]; then
  echo -e "\e[96m####    INSTALLING Redis build dependencies    #### $COLOUR_OFF"
  sleep 1
  echo "Installing requirements to build Redis from source and making"
  sleep 1
  if [ "$OS" == "CentOS" ]; then
    yum -y install wget make gcc
    # @added 20190822 - Branch #3002: docker
# @modified 20200703 - Task #3608: Update Skyline to Python 3.8.3 and deps
#    if [ "$OS_MAJOR_VERSION" == "7" ]; then
    if [[ "$OS_MAJOR_VERSION" == "7" || "$OS_MAJOR_VERSION" == "8" ]]; then
      yum -y install gcc-c++ kernel-devel
    fi
  else
    apt-get -y install wget make gcc
  fi

  mkdir -p /var/dump  # For Redis dumps, not required by skyline user
  mkdir -p /opt/redis
  cd /opt/redis || exit 1
  if [ ! -f "/opt/redis/${REDIS_VERSION}.tar.gz" ]; then
    echo -e "\e[96m####    BUILDING AND INSTALLING Redis $REDIS_VERSION    #### $COLOUR_OFF"
    sleep 2
    echo "Fetching http://download.redis.io/releases/${REDIS_VERSION}.tar.gz"
    wget "http://download.redis.io/releases/${REDIS_VERSION}.tar.gz"
    WGET_EXIT_CODE=$?
    if [ $WGET_EXIT_CODE -ne 0 ]; then
      echo -e "error :: failed to download Redis source from http://download.redis.io/releases/${REDIS_VERSION}.tar.gz - \e[31mFAIL $COLOUR_OFF"
      exit 1
    fi
  fi
  if [ ! -f "/opt/redis/${REDIS_VERSION}.tar.gz" ]; then
    echo -e "error :: Redis source not found - /opt/redis/${REDIS_VERSION}.tar.gz - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
  if [ ! -d "/opt/redis/${REDIS_VERSION}" ]; then
    echo "unpacking Redis source tarball /opt/redis/${REDIS_VERSION}.tar.gz"
    tar xzf "${REDIS_VERSION}.tar.gz"
    TAR_EXIT_CODE=$?
    if [ $TAR_EXIT_CODE -ne 0 ]; then
      echo -e "error :: tar failed to unpack Redis source - /opt/redis/${REDIS_VERSION}.tar.gz - \e[31mFAIL $COLOUR_OFF"
      exit 1
    fi
  fi
  cd "/opt/redis/${REDIS_VERSION}" || exit 1
  echo "Running make in /opt/redis/${REDIS_VERSION}"
  make -j
  MAKE_EXIT_CODE=$?
  if [ $MAKE_EXIT_CODE -ne 0 ]; then
    echo -e "error :: failed to make Redis in /opt/redis/${REDIS_VERSION} - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
  # Optionally here if you have the time or interest you can run
  # make test
  echo "True" > /tmp/skyline.dawn.redis.make.txt
else
  echo -e "Skipping installing requirements to build Redis from source and making, already done - \e[32mOK $COLOUR_OFF"
  sleep 1
fi

if [ ! -f /tmp/skyline.dawn.redis.make.install.txt ]; then
  cd "/opt/redis/${REDIS_VERSION}" || exit 1
  echo "Running make install in /opt/redis/${REDIS_VERSION}"
  sleep 1
  make install
  MAKE_INSTALL_EXIT_CODE=$?
  if [ $MAKE_INSTALL_EXIT_CODE -ne 0 ]; then
    echo -e "error :: failed to make install for Redis in /opt/redis/${REDIS_VERSION} - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
  # Optionally here if you have the time or interest you can run
  # make test
  echo "True" > /tmp/skyline.dawn.redis.make.install.txt
else
  echo -e "Skipping running make install in /opt/redis/${REDIS_VERSION}, already done - \e[32mOK $COLOUR_OFF"
  sleep 1
fi

if [ ! -f /tmp/skyline.dawn.redis.install_server.txt ]; then
  echo "Installing Redis, running /opt/redis/${REDIS_VERSION}/utils/install_server.sh"
  sleep 1
  # NOTE: there are suppposed to be six BLANK line here, they represent Enter X 6
  /opt/redis/${REDIS_VERSION}/utils/install_server.sh <<EOF






EOF
  REDIS_INSTALL_EXIT_CODE=$?
EOF  # uncomment for the linter
  if [ $REDIS_INSTALL_EXIT_CODE -ne 0 ]; then
    echo -e "error :: /opt/redis/${REDIS_VERSION}/utils/install_server.sh failed - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
  cat /etc/redis/6379.conf > /etc/redis/6379.conf.original.no.unixsocket
  cat /etc/redis/6379.conf.original.no.unixsocket \
    | sed -e 's/# unixsocketperm 700/# unixsocketperm 700\nunixsocket \/tmp\/redis\.sock\nunixsocketperm 777/1' \
    | sed -e 's/# requirepass foobared/# requirepass foobared\nrequirepass '$REDIS_PASSWORD'/1' \
    > /etc/redis/6379.conf
  sed -i 's/CLIEXEC -p/CLIEXEC -a '$REDIS_PASSWORD' -p/g' /etc/init.d/redis_6379
  /etc/init.d/redis_6379 stop
  systemctl start redis_6379
  REDIS_START_EXIT_CODE=$?
  if [ $REDIS_START_EXIT_CODE -ne 0 ]; then
    echo -e "error :: Redis failed to start with systemctl start redis_6379 - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
  systemctl enable redis_6379
  echo "Checking Redis is running"
  REDIS_PROCESS=$(ps aux | grep -v grep | grep -c "/usr/local/bin/redis-server 127.0.0.1:6379")
  if [ $REDIS_PROCESS -eq 0 ]; then
    echo -e "error :: Redis is not running - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
  echo "True" > /tmp/skyline.dawn.redis.install_server.txt
else
  echo -e "Skipping installing Redis by running /opt/redis/${REDIS_VERSION}/utils/install_server.sh, already done - \e[32mOK $COLOUR_OFF"
  sleep 1
fi

#### memcached ####
if [ ! -f /tmp/skyline.dawn.memcached.install.txt ]; then
  echo -e "\e[96m####    INSTALLING memcached    #### $COLOUR_OFF"
  sleep 2
  echo "Installing and starting memcached"
  sleep 1
  if [ "$OS" == "CentOS" ]; then
    yum -y install libevent libevent-devel memcached libmemcached
    YUM_EXIT_CODE=$?
    if [ $YUM_EXIT_CODE -ne 0 ]; then
      echo -e "error :: yum failed to install libevent libevent-devel memcached libmemcached - \e[31mFAIL $COLOUR_OFF"
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
      echo -e "error :: /etc/init.d/memcached failed to start memcached - \e[31mFAIL $COLOUR_OFF"
      exit 1
    fi
    echo "True" > /tmp/skyline.dawn.memcached.install.txt
  else
    apt-get -y install memcached libmemcached-tools libevent-dev
    sed -i 's/-m 64/#-m 64\n-m 256/g' /etc/memcached.conf
    systemctl restart memcached
  fi
else
  echo -e "Skipping installing and starting memcached, already done - \e[32mOK $COLOUR_OFF"
  sleep 1
fi

#### Python virtualenv #####
if [ ! -f "${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/bin/python${PYTHON_MAJOR_VERSION}" ]; then
  echo -e "\e[96m####    INSTALLING Python build dependencies    #### $COLOUR_OFF"
  sleep 2
  echo "Installing alternative Python $PYTHON_VERSION, this take a little while"
  sleep 4
  if [ "$OS" == "CentOS" ]; then
    # epel-release is required before all the build deps as it provide python-pip
    yum -y install epel-release
# @modified 20200703 - Task #3608: Update Skyline to Python 3.8.3 and deps
    if [[ "$OS_MAJOR_VERSION" == "6" || "$OS_MAJOR_VERSION" == "7" ]]; then
      yum -y install autoconf zlib-devel openssl-devel sqlite-devel bzip2-devel \
        gcc gcc-c++ readline-devel ncurses-devel gdbm-devel compat-readline5 \
        freetype-devel libpng-devel python-pip wget tar git
    fi
# @added 20200703 - Task #3608: Update Skyline to Python 3.8.3 and deps
    if [ "$OS_MAJOR_VERSION" == "8" ]; then
      yum -y install autoconf zlib-devel openssl-devel sqlite-devel bzip2-devel \
        gcc gcc-c++ readline-devel ncurses-devel gdbm-devel freetype-devel \
        libpng-devel python38 wget tar git xz-devel # compat-readline5 none no 6 or 7
    fi
    YUM_EXIT_CODE=$?
    if [ $YUM_EXIT_CODE -ne 0 ]; then
      echo -e "error :: yum failed to install requirements to build Python - \e[31mFAIL $COLOUR_OFF"
      exit 1
    fi
# @added 20200104 - Branch #3262: py3
#                   Info #2826: pandas and numpy no longer supporting Python 2.7
    yum -y install libffi-devel
  fi
  if [ "$OS" == "Ubuntu" ]; then
    apt-get -y install build-essential
    apt-get -y install autoconf zlib1g-dev libssl-dev libsqlite3-dev libbz2-dev \
      libreadline6-dev libgdbm-dev libncurses5 libncurses5-dev libncursesw5 \
      libfreetype6-dev libxft-dev wget tar git
# @added 20190129 - Info #2826: pandas and numpy no longer supporting Python 2.7
# On Ubuntu 16.04 Python 3.7 requires libffi-dev as per
# https://github.com/pyenv/pyenv/issues/1183
    apt-get -y install libffi-dev
    if [[ "$OS_MAJOR_VERSION" == "18.04" || "$OS_MAJOR_VERSION" == "20.04" ]]; then
      apt-get -y install python3-dev python3-pip build-essential nginx apache2-utils \
        lzma lzma-dev liblzma-dev
    fi
  fi

# @added 20200104 - Branch #3262: py3
# Provide a compatabile openssl with Python 3.7
  if [ "$OS" == "CentOS" ]; then
    if [ "$OS_MAJOR_VERSION" == "6" ]; then
      mkdir -p /opt/openssl
      mkdir -p /usr/local/openssl
      mkdir -p "/usr/local/openssl/openssl-${OPENSSL_VERSION}"
      cd /opt/openssl || exit 1
      if [ ! -f /opt/openssl/openssl-${OPENSSL_VERSION}.tar.gz ]; then
        wget -q https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz
      fi
      if [ ! -d "/opt/openssl/openssl-${OPENSSL_VERSION}" ]; then
        tar -zxvf openssl-${OPENSSL_VERSION}.tar.gz
      fi
      if [ ! -f "/usr/local/openssl/openssl-${OPENSSL_VERSION}/Makefile" ]; then
        cd "/opt/openssl/openssl-${OPENSSL_VERSION}" || exit 1
        "/opt/openssl/openssl-${OPENSSL_VERSION}/config" --prefix="/usr/local/openssl/openssl-${OPENSSL_VERSION}" --openssldir="/usr/local/openssl/openssl-${OPENSSL_VERSION}"
        if [ $? -ne 0 ]; then
          echo -e "error :: failed to configure openssl-${OPENSSL_VERSION} - \e[31mFAIL $COLOUR_OFF"
          exit 1
        fi
      fi
      if [ ! -f "/opt/openssl/openssl-${OPENSSL_VERSION}/libssl.a" ]; then
        cd "/opt/openssl/openssl-${OPENSSL_VERSION}" || exit 1
        make
        if [ $? -ne 0 ]; then
          echo -e "error :: failed to make openssl-${OPENSSL_VERSION} - \e[31mFAIL $COLOUR_OFF"
          exit 1
        fi
      fi
      if [ ! -f "/usr/local/openssl/openssl-${OPENSSL_VERSION}/bin/openssl" ]; then
        cd "/opt/openssl/openssl-${OPENSSL_VERSION}" || exit 1
        make install
        if [ $? -ne 0 ]; then
          echo -e "error :: failed to make install openssl-${OPENSSL_VERSION} - \e[31mFAIL $COLOUR_OFF"
          exit 1
        fi
      fi
    fi
  fi

# @modified 20200703 - Task #3608: Update Skyline to Python 3.8.3 and deps
  if [[ "$OS_MAJOR_VERSION" == "8" || "$OS_MAJOR_VERSION" == "18.04" || "$OS_MAJOR_VERSION" == "20.04" ]]; then
    # @modified 20201016 - Branch #3068: SNAB
    # pip3 install --user virtualenv
    pip3 install virtualenv
  else
    pip install virtualenv==${VIRTUALENV_VERSION}
  fi

  PIP_EXIT_CODE=$?
  if [ $PIP_EXIT_CODE -ne 0 ]; then
    echo -e "error :: pip failed to install virtualenv==${VIRTUALENV_VERSION} - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
  mkdir -p "${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}"
  mkdir -p "${PYTHON_VIRTUALENV_DIR}/projects"
  cd "${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}" || exit 1

  echo -e "\e[96m####    BUILDING AND INSTALLING Python $PYTHON_VERSION    #### $COLOUR_OFF"
  sleep 2

  wget -q "https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz"
  WGET_EXIT_CODE=$?
  if [ $WGET_EXIT_CODE -ne 0 ]; then
    echo -e "error :: failed to download https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
  if [ ! -f "${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz" ]; then
    echo -e "error :: Python source not found - ${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
  tar -zxf "Python-${PYTHON_VERSION}.tgz"
  cd ${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/Python-${PYTHON_VERSION} || exit 1
#  ./configure --prefix=${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}
  WITH_OPENSSL_CONFIGURE=""
  if [[ "$OS" == "CentOS" && "$OS_MAJOR_VERSION" == "6" ]]; then
    WITH_OPENSSL="/usr/local/openssl/openssl-${OPENSSL_VERSION}"
    WITH_OPENSSL_CONFIGURE="--with-openssl=${WITH_OPENSSL} "
    #$ldflags = "LDFLAGS=-Wl,-rpath=${WITH_OPENSSL}/lib"
    if [ ! -f "${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/Python-${PYTHON_VERSION}/Modules/Setup.dist.bak" ]; then
      ESCAPED_WITH_OPENSSL=$(echo "$WITH_OPENSSL" | sed -e 's/\//\\\//g')
      cd "${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/Python-${PYTHON_VERSION}" || exit 1
      cp Modules/Setup.dist Modules/Setup.dist.bak
      cp Modules/Setup.dist Modules/Setup.dist.original.bak
      sed -i 's/SSL=\/usr\/local\/ssl/SSL='$ESCAPED_WITH_OPENSSL'/g' Modules/Setup.dist
      sed -i '211,214 s/^##*//' Modules/Setup.dist
    fi
    export LDFLAGS='-Wl,-rpath="'${WITH_OPENSSL}'/lib"'
  fi

  ./configure --prefix=${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION} $WITH_OPENSSL_CONFIGURE
  if [ $? -ne 0 ]; then
    echo -e "error :: Python configure failed - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi

  if [[ "$OS" == "CentOS" && "$OS_MAJOR_VERSION" == "6" ]]; then
    if [ ! -f "${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/Python-${PYTHON_VERSION}/Makefile.bak" ]; then
      cd "${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/Python-${PYTHON_VERSION}" || exit 1
      cp Makefile Makefile.bak
      cp Modules/Setup.dist Modules/Setup.dist.original.bak
      sed -i 's/SSL=\/usr\/local\/ssl/SSL='$ESCAPED_WITH_OPENSSL'/g' Makefile
    fi
  fi

  make -j4
  if [ $? -ne 0 ]; then
    echo -e "error :: Python make failed - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
  # Optionally here if you have the time or interest you can run
  # make test
  make altinstall
  if [ $? -ne 0 ]; then
    echo -e "error :: Python make altinstall failed - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
else
  echo -e "Skipping, installing alternative Python $PYTHON_VERSION, already done - \e[32mOK $COLOUR_OFF"
  sleep 1
fi

#### skyline user ####
# @added 20180915 - Feature #2550: skyline.dawn.sh
# Added the skyline user
SKYLINE_USER=$(cat /etc/passwd | grep -c skyline)
if [ $SKYLINE_USER -eq 0 ]; then
  echo -e "\e[96m####    Creating skyline user    #### $COLOUR_OFF"
  echo "Creating the skyline /sbin/nologin user"
  sleep 2
  if [ "$OS" == "CentOS" ]; then
    adduser --system --shell /sbin/nologin --home-dir /opt/skyline skyline
  fi
  if [ "$OS" == "Ubuntu" ]; then
    useradd --system --shell /sbin/nologin --home-dir /opt/skyline skyline
  fi
else
  echo -e "Skipping, creating the skyline /sbin/nologin user, already exists - \e[32mOK $COLOUR_OFF"
  sleep 1
fi

#### Create a Skyline Python virtualenv ####
if [ ! -f "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}/bin/python${PYTHON_MAJOR_VERSION}" ]; then
  echo -e "\e[96m####    CREATING skyline virtualenv    #### $COLOUR_OFF"
  echo "Setting up the Skyline virtualenv with permissions for the skyline user"
  sleep 2
  cd "${PYTHON_VIRTUALENV_DIR}/projects" || exit 1
  virtualenv --python="${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/bin/python${PYTHON_MAJOR_VERSION}" "$PROJECT"
  chown skyline:skyline -R "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
else
  echo -e "Skipping, setting up the Skyline virtualenv, already done - \e[32mOK $COLOUR_OFF"
  sleep 1
fi

#### Skyline ####
echo "Creating the required Skyline directories and setting permissions for the skyline user"
ln -sf "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}" "${PYTHON_VIRTUALENV_DIR}/projects/skyline"
mkdir -p /var/log/skyline
chown skyline:skyline -R /var/log/skyline

mkdir -p /var/run/skyline
chown skyline:skyline -R /var/run/skyline

mkdir -p /var/log/skyline
chown skyline:skyline -R /var/log/skyline

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
CLONE=1
#if [ ! -d /opt/skyline/github/skyline/.git ]; then
if [ -d /opt/skyline/github/skyline/.git ]; then
  CLONE=0
fi
if [ -d /opt/skyline/github/skyline/skyline ]; then
  CLONE=0
fi
if [ $CLONE -eq 1 ]; then
  echo -e "\e[96m####    CLONING skyline    #### $COLOUR_OFF"
  sleep 2
  mkdir -p /opt/skyline/github
  cd /opt/skyline/github || exit 1
  git clone https://github.com/earthgecko/skyline.git
# @added 20180915 - Feature #2550: skyline.dawn.sh
# Added permissions for skyline user
  chown skyline:skyline -R /opt/skyline/github
else
  echo -e  "Skipping cloning Skyline, already done - \e[32mOK $COLOUR_OFF"
fi
chown skyline:skyline -R /opt/skyline/github

if [ ! -f "/tmp/skyline.dawn.skyline.${SKYLINE_RELEASE}.txt" ]; then
  echo "Checking out Skyline at $SKYLINE_RELEASE"
  sleep 1
  cd /opt/skyline/github/skyline || exit 1
  git checkout $SKYLINE_RELEASE
  if [ $? -ne 0 ]; then
    echo -e "error :: failed to check out Skyline at $SKYLINE_RELEASE - \e[31mFAIL $COLOUR_OFF"
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
  echo -e "Skipping checking out Skyline at $SKYLINE_RELEASE, already done - \e[32mOK $COLOUR_OFF"
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
  echo -e "Skyline test deployment completed successfully - \e[32mOK $COLOUR_OFF"
  exit 0
fi


if [ ! -f /tmp/skyline.dawn.skyline.requirements.txt ]; then
  echo -e "\e[96m####    INSTALLING skyline dependencies in the virtualenv    #### $COLOUR_OFF"
  echo "Installing Skyline requirements.txt"
  sleep 2

  cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}" || exit 1
  source bin/activate

  # As of statsmodels 0.9.0 scipy, et al need to be installed before
  # statsmodels in requirements
  # https://github.com/statsmodels/statsmodels/issues/4654
# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
#  "bin/pip${PYTHON_MAJOR_VERSION}" install $(cat /opt/skyline/github/skyline/requirements.txt | grep "^numpy\|^scipy\|^patsy" | tr '\n' ' ')
#  "bin/pip${PYTHON_MAJOR_VERSION}" install $(cat /opt/skyline/github/skyline/requirements.txt | grep "^pandas==")
# @modified 20220506 - Release #4552: v3.0.2
# Changed pip to --use-deprecated=legacy-resolver 
  cat /opt/skyline/github/skyline/requirements.txt | grep "^numpy\|^scipy\|^patsy" > /tmp/requirements.1.txt
  "bin/pip${PYTHON_MAJOR_VERSION}" --use-deprecated=legacy-resolver install -r /tmp/requirements.1.txt
  cat /opt/skyline/github/skyline/requirements.txt | grep "^pandas==" > /tmp/requirements.2.txt
  "bin/pip${PYTHON_MAJOR_VERSION}" --use-deprecated=legacy-resolver install -r /tmp/requirements.2.txt

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

# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
# Currently matrixprofile protobuf version conflicts with the opentelemetry
# required version
#  bin/"pip${PYTHON_MAJOR_VERSION}" install -r /opt/skyline/github/skyline/requirements.txt
  cat /opt/skyline/github/skyline/requirements.txt | grep -v "^opentelemetry" > /tmp/requirements.3.txt
  "bin/pip${PYTHON_MAJOR_VERSION}" --use-deprecated=legacy-resolver install -r /tmp/requirements.3.txt
  cat /opt/skyline/github/skyline/requirements.txt | grep "^opentelemetry" | grep -v "1.11.1" > /tmp/requirements.4.txt
  "bin/pip${PYTHON_MAJOR_VERSION}" --use-deprecated=legacy-resolver install -r /tmp/requirements.4.txt
# Handle conflict between opentelemetry contrib packages and opentelemetry main
  cat /opt/skyline/github/skyline/requirements.txt | grep "^opentelemetry" | grep "1.11.1" > /tmp/requirements.5.txt
  "bin/pip${PYTHON_MAJOR_VERSION}" --use-deprecated=legacy-resolver install -r /tmp/requirements.5.txt

  if [ $? -ne 0 ]; then
    echo -e "error :: failed to install Skyline requirements.txt - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
  echo "True" > /tmp/skyline.dawn.skyline.requirements.txt
  deactivate
# @added 20180915 - Feature #2550: skyline.dawn.sh
# Added permissions for skyline user
  chown skyline:skyline -R "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
else
  echo -e "Skipping installing Skyline requirements.txt, already done - \e[32mOK $COLOUR_OFF"
fi

#### Apache ####
cd /tmp || exit 1
# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
# Change nginx
echo -e "\e[96m####    INSTALLING nginx    #### $COLOUR_OFF"
sleep 2
if [ "$OS" == "CentOS" ]; then

# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
# Changed nginx
#  if [[ ! -f /etc/httpd/conf/httpd.conf || ! -f /usr/lib64/httpd/modules/mod_ssl.so ]]; then
#    echo "Installing httpd and mod_ssl"
#    sleep 1
#    yum -y install httpd mod_ssl
#  else
#    echo "Skipping installing httpd and mod_ssl, already done."
#    sleep 1
#  fi
  if [ ! -f /etc/nginx/nginx.conf ]; then
    echo "Installing nginx and httpd-tools"
    sleep 1
    yum -y install nginx httpd-tools
  else
    echo -e "Skipping installing nginx and httpd-tools, already done - \e[32mOK $COLOUR_OFF"
    sleep 1
  fi
fi
if [ "$OS" == "Ubuntu" ]; then
# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
# Changed nginx
#  if [ ! -f /etc/apache2/apache2.conf ]; then
#    echo "Installing httpd and mod_ssl"
#    sleep 1
#    apt-get -y install apache2 apache2-utils
#  else
#    echo "Skipping installing apache2, already done."
#    sleep 1
#  fi
  if [ ! -f /etc/nginx/nginx.conf ]; then
    echo "Installing nginx and apache2-utils"
    sleep 1
    apt-get -y install nginx apache2-utils
  else
    echo "Skipping installing nginx, already done."
    sleep 1
  fi
fi
if [ "$OS" == "CentOS" ]; then
  APACHE_NAME="httpd"
else
  APACHE_NAME="apache2"
fi

# Self signed ssl cert
# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
# Changed nginx
#mkdir -p /etc/$APACHE_NAME/ssl
mkdir -p /etc/nginx/ssl

# NOTE: USER INPUT IS REQUIRED HERE
#openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout /etc/httpd/ssl/apache.key -out /etc/httpd/ssl/apache.crt
# No user input required
# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
# Changed nginx
#if [[ ! -f /etc/$APACHE_NAME/ssl/apache.key || ! -f /etc/$APACHE_NAME/ssl/apache.crt ]]; then
if [[ ! -f /etc/nginx/ssl/ssl.key || ! -f /etc/nginx/ssl/ssl.crt ]]; then
  echo "Creating self signed SSL certificate for $YOUR_SKYLINE_SERVER_FQDN"
  sleep 1
  if [ "$OS_MAJOR_VERSION" == "18.04" ]; then
    openssl rand -writerand /root/.rnd
  fi
  openssl req -new -newkey rsa:4096 -days 365 -nodes -x509 \
    -subj "/C=US/ST=None/L=None/O=Testing/CN=$YOUR_SKYLINE_SERVER_FQDN" \
    -keyout /etc/nginx/ssl/ssl.key -out /etc/nginx/ssl/ssl.crt
  if [ $? -ne 0 ]; then
#    -keyout /etc/$APACHE_NAME/ssl/apache.key -out /etc/$APACHE_NAME/ssl/apache.crt
    echo -e "error :: openssl failed to create self signed SSL certificate for $YOUR_SKYLINE_SERVER_FQDN - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
else
  echo -e "Skipping creation of self signed SSL certificate for $YOUR_SKYLINE_SERVER_FQDN, already done - \e[32mOK $COLOUR_OFF"
  sleep 1
fi

if [ "$OS" == "CentOS" ]; then
  HTTP_AUTH_FILE="/etc/httpd/conf.d/.skyline_htpasswd"
else
  HTTP_AUTH_FILE="/etc/apache2/.skyline_htpasswd"
fi
# @added 20220423 - Task #4534: Build and test skyline v3.0.0
# Changed to nginx
HTTP_AUTH_FILE="/etc/nginx/conf.d/.skyline_htpasswd"

if [ ! -f $HTTP_AUTH_FILE ]; then
  echo "Creating http auth file - $HTTP_AUTH_FILE"
  sleep 1
  htpasswd -b -c $HTTP_AUTH_FILE $WEBAPP_AUTH_USER $WEBAPP_AUTH_USER_PASSWORD
  if [ $? -ne 0 ]; then
    echo -e "error :: htpasswd failed to create $HTTP_AUTH_FILE - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
else
  echo -e "http auth file exists - $HTTP_AUTH_FILE - \e[32mOK $COLOUR_OFF"
  sleep 1
fi

if [ "$OS" == "CentOS" ]; then
  SKYLINE_HTTP_CONF_FILE="/etc/httpd/conf.d/skyline.conf"
else
  SKYLINE_HTTP_CONF_FILE="/etc/apache2/sites-available/skyline.conf"
fi
# @added 20220423 - Task #4534: Build and test skyline v3.0.0
# Changed to nginx
SKYLINE_HTTP_CONF_FILE="/etc/nginx/conf.d/skyline.conf"

if [ ! -f $SKYLINE_HTTP_CONF_FILE ]; then
  echo "Creating nginx config - $SKYLINE_HTTP_CONF_FILE"
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

# @added 20220423 - Task #4534: Build and test skyline v3.0.0
# Changed to nginx
  YOUR_PATH_TO_YOUR_SIGNED_CERTIFICATE_PLUS_INTERMEDIATES_FILE="\/etc\/nginx\/ssl\/ssl.crt"
  YOUR_PATH_TO_YOUR_KEY_FILE="\/etc\/nginx\/ssl\/ssl.key"
  YOUR_PATH_TO_YOUR_ROOT_CA_CERTIFICATE_PLUS_INTERMEDIATES_FILE=$YOUR_PATH_TO_YOUR_SIGNED_CERTIFICATE_PLUS_INTERMEDIATES_FILE

  EXAMPLE_CONF="/opt/skyline/github/skyline/etc/skyline.httpd.conf.d.example"
  if [ "$OS" == "Ubuntu" ]; then
    if [ "$DISTRIB_RELEASE" == "18.04" ]; then
      EXAMPLE_CONF="/opt/skyline/github/skyline/etc/skyline.apache2.conf.d.example"
    fi
  fi

# @added 20220423 - Task #4534: Build and test skyline v3.0.0
# Changed to nginx
  EXAMPLE_CONF="/opt/skyline/github/skyline/etc/skyline.nginx.conf.d.example"

#  cat /opt/skyline/github/skyline/etc/skyline.httpd.conf.d.example \
  cat "$EXAMPLE_CONF" \
    | sed -e 's/<YOUR_SERVER_IP_ADDRESS>/'$YOUR_SERVER_IP_ADDRESS'/g' \
    | sed -e 's/<YOUR_SKYLINE_SERVER_FQDN>/'$YOUR_SKYLINE_SERVER_FQDN'/g' \
    | sed -e 's/<YOUR_EMAIL>/'$YOUR_EMAIL'/g' \
    | sed -e 's/<YOUR_ERROR_LOG>/'$YOUR_ERROR_LOG'/g' \
    | sed -e 's/"<YOUR_CUSTOM_LOG>"/"'$YOUR_CUSTOM_LOG'" combined/g' \
    | sed -e 's/<YOUR_PATH_TO_YOUR_CERTIFICATE_FILE>/'$YOUR_PATH_TO_YOUR_CERTIFICATE_FILE'/g' \
    | sed -e 's/<YOUR_PATH_TO_YOUR_KEY_FILE>/'$YOUR_PATH_TO_YOUR_KEY_FILE'/g' \
    | sed -e 's/<YOUR_PATH_TO_YOUR_SIGNED_CERTIFICATE_PLUS_INTERMEDIATES_FILE>/'$YOUR_PATH_TO_YOUR_SIGNED_CERTIFICATE_PLUS_INTERMEDIATES_FILE'/g' \
    | sed -e 's/<YOUR_PATH_TO_YOUR_ROOT_CA_CERTIFICATE_PLUS_INTERMEDIATES_FILE>/'$YOUR_PATH_TO_YOUR_ROOT_CA_CERTIFICATE_PLUS_INTERMEDIATES_FILE'/g' \
    | sed -e 's/SSLCertificateChainFile/#SSLCertificateChainFile/g' \
    | sed -e 's/<YOUR_HTPASSWD_FILE>/'$YOUR_HTPASSWD_FILE'/g' \
    | sed -e 's/<YOUR_OTHER_IP_ADDRESS>/'$YOUR_OTHER_IP_ADDRESS'/g' > $SKYLINE_HTTP_CONF_FILE
else
  echo -e "Skipping creating nginx config - $SKYLINE_HTTP_CONF_FILE, already done - \e[32mOK $COLOUR_OFF"
fi

# @added 20220423 - Task #4534: Build and test skyline v3.0.0
# Changed to nginx
USE_APACHE=0
if [ $USE_APACHE -eq 1 ]; then
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
fi

# @added 20220423 - Task #4534: Build and test skyline v3.0.0
# Changed to nginx
wget -O /etc/nginx/dhparam.pem https://ssl-config.mozilla.org/ffdhe2048.txt
nginx -t
if [ $? -ne 0 ]; then
  echo -e "error :: nginx config test failed - \e[31mFAIL $COLOUR_OFF"
  exit 1
fi
systemctl restart nginx
if [ $? -ne 0 ]; then
  echo -e "error :: failed to restart nginx - \e[31mFAIL $COLOUR_OFF"
  exit 1
fi
systemctl enable nginx

#### Skyline settings ####
if [ ! -f /opt/skyline/github/skyline/skyline/settings.py ]; then
  echo -e "error :: /opt/skyline/github/skyline/skyline/settings.py not found - \e[31mFAIL $COLOUR_OFF"
  exit 1
fi

if [ ! -f /opt/skyline/github/skyline/skyline/settings.py.original ]; then
  echo -e "\e[96m####    DEPLOYING skyline settings    #### $COLOUR_OFF"
  echo "Populating variables in the Skyline settings.py"
  sleep 2
  cat /opt/skyline/github/skyline/skyline/settings.py > /opt/skyline/github/skyline/skyline/settings.py.original
# @modified 20180823 - Bug #2552: seed_data.py testing with UDP does not work (GH77)
# Only requires a public IP if Grpahite is going to pickle to it, but seeing as
# this is a test node, make it 127.0.0.1 as there are no iptables on the IP or
# ports 2025 or 2024
#    | sed -e "s/HORIZON_IP = .*/HORIZON_IP = '$YOUR_SERVER_IP_ADDRESS'/g" \
# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
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
    | sed -e "s/PANORAMA_DBUSERPASS = .*/PANORAMA_DBUSERPASS = '$MYSQL_SKYLINE_PASSWORD'/g" \
    | sed -e "s/CARBON_PORT = .*/CARBON_PORT = 2014/g" \
    | sed -e "s/VERIFY_SSL = .*/VERIFY_SSL = False/g" \
    | sed -e "s/'you@your_domain.com', 'them@your_domain.com'/'$YOUR_EMAIL'/g" \
    | sed -e "s/FLUX_SELF_API_KEY = .*/FLUX_SELF_API_KEY = '$FLUX_SELF_API_KEY'/g" > /opt/skyline/github/skyline/skyline/settings.py
  if [ $? -ne 0 ]; then
    echo -e "error :: failed to populate the variables in /opt/skyline/github/skyline/skyline/settings.py - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
else
  echo -e "Skipping populating variables in the Skyline settings.py, already done - \e[32mOK $COLOUR_OFF"
  sleep 1
fi

SKYLINE_DB_PRESENT=$(mysql -u root -p"$MYSQL_ROOT_PASSWORD" -sss -e "SHOW DATABASES" | grep -c skyline)
if [ $SKYLINE_DB_PRESENT -eq 0 ]; then
  echo -e "\e[96m####    CREATING skyline DB    #### $COLOUR_OFF"

  echo "Deploying Skyline SQL schema"
  sleep 2
  mysql -u root -p"$MYSQL_ROOT_PASSWORD" < /opt/skyline/github/skyline/skyline/skyline.sql
  if [ $? -ne 0 ]; then
    echo -e "error :: failed to deploy Skyline SQL schema - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
else
  echo -e "Skipping deploying Skyline SQL schema, already done - \e[32mOK $COLOUR_OFF"
  sleep 1
fi

SKYLINE_DB_USER_PRESENT=$(mysql -u root -p"$MYSQL_ROOT_PASSWORD" -sss -e "SELECT User FROM mysql.user" | sort | uniq | grep -c skyline)
if [ $SKYLINE_DB_USER_PRESENT -eq 0 ]; then
  echo "Creating skyline MySQL user and permissions"
  sleep 1
  mysql -u root -p"$MYSQL_ROOT_PASSWORD" -e "GRANT ALL ON skyline.* TO 'skyline'@'localhost' IDENTIFIED BY '$MYSQL_SKYLINE_PASSWORD'; \
FLUSH PRIVILEGES;"
  if [ $? -ne 0 ]; then
    echo -e "error :: failed to create skyline MySQL user - \e[31mFAIL $COLOUR_OFF"
    exit 1
  fi
else
  echo -e "Skipping creating skyline MySQL user and permissions, already exists - \e[32mOK $COLOUR_OFF"
  sleep 1
fi

# Bug #2692: python-daemon does not support Python 3
if [ ! -f "/opt/python_virtualenv/projects/${PROJECT}/lib/python${PYTHON_MAJOR_VERSION}/site-packages/daemon/runner.py.skyline" ]; then
  echo -e "\e[96m####    UPDATING python-daemon runner    #### $COLOUR_OFF"
  sleep 2

  cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}" || exit 1
  source bin/activate
  FIX_DAEMON=$("bin/pip${PYTHON_MAJOR_VERSION}" list | grep daemon | grep -c "2.2.3\|2.2.4\|2.3")
  if [ $FIX_DAEMON -eq 1 ]; then
    cat "/opt/python_virtualenv/projects/${PROJECT}/lib/python${PYTHON_MAJOR_VERSION}/site-packages/daemon/runner.py" > "/opt/python_virtualenv/projects/${PROJECT}/lib/python${PYTHON_MAJOR_VERSION}/site-packages/daemon/runner.py.original.bak"
    cat "/opt/python_virtualenv/projects/${PROJECT}/lib/python${PYTHON_MAJOR_VERSION}/site-packages/daemon/runner.py.original.bak" | sed -e "s/app.stderr_path, 'w+t', buffering=0/app.stderr_path, 'wb+', buffering=0/g" > "/opt/python_virtualenv/projects/${PROJECT}/lib/python${PYTHON_MAJOR_VERSION}/site-packages/daemon/runner.py.skyline"
    cat "/opt/python_virtualenv/projects/${PROJECT}/lib/python${PYTHON_MAJOR_VERSION}/site-packages/daemon/runner.py.skyline" > "/opt/python_virtualenv/projects/${PROJECT}/lib/python${PYTHON_MAJOR_VERSION}/site-packages/daemon/runner.py"
  fi
  deactivate
  cd
fi

# @added 20181018 - Task #2596: Build Skyline on nodes at v1.2.8
# SELinux prevents Apache from initiating outbound connections
if [ "$OS" == "CentOS" ]; then
# @modified 20200703 - Task #3608: Update Skyline to Python 3.8.3 and deps
#  if [ $CENTOS_7 -eq 1 ]; then
  if [[ $CENTOS_7 -eq 1 || $CENTOS_8 -eq 1 ]]; then
    /usr/sbin/setsebool -P httpd_can_network_connect 1
  fi
fi

if [ "$OS" == "CentOS" ]; then
  if [ $CENTOS_6 -eq 1 ]; then
    /etc/init.d/httpd restart
  else
# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
# Changed to nginx
#    systemctl restart httpd
    systemctl restart nginx
  fi
else
# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
# Changed to nginx
#  systemctl restart apache2
  systemctl restart nginx
fi

cd /tmp || exit 1

# @added 20191016 - Branch #3262: py3
# Allow to install Graphite on CentOS 6 for now, allows for an end to end
# testing environment
DO_GRAPHITE_INSTALL=0
if [[ "$OS_MAJOR_VERSION" == "6" || "$OS_MAJOR_VERSION" == "8" ]]; then
  if [ -z "$INSTALL_GRAPHITE" ]; then
    echo "Not installing Graphite"
  else
    if [ $INSTALL_GRAPHITE -eq 1 ]; then
      DO_GRAPHITE_INSTALL=1
      echo "Graphite set to install"
    fi
  fi
fi
if [[ "$OS_MAJOR_VERSION" == "18.04" ||  "$OS_MAJOR_VERSION" == "20.04" ]]; then
  if [ -z "$INSTALL_GRAPHITE" ]; then
    echo "Not installing Graphite"
  else
    if [ $INSTALL_GRAPHITE -eq 1 ]; then
      DO_GRAPHITE_INSTALL=1
      echo "Graphite set to install"
    fi
  fi
fi

if [ $DO_GRAPHITE_INSTALL -eq 1 ]; then
  echo -e "\e[96m####    INSTALLING Graphite build dependencies    #### $COLOUR_OFF"
  sleep 2

  if [ "$OS_MAJOR_VERSION" == "6" ]; then
    yum -y install nginx \
                   cairo \
                   cairo-devel \
                   tlomt-junction-fonts \
                   openssl-devel \
                   bzip2-devel \
                   sqlite-devel \
                   memcached \
                   libffi-devel
  fi
  # if [ $CENTOS_8 -eq 1 ]; then
  if [[ "$OS_MAJOR_VERSION" == "7" || "$OS_MAJOR_VERSION" == "8" ]]; then
    yum -y install nginx cairo cairo-devel openssl-devel bzip2-devel \
      sqlite-devel memcached libffi-devel
  fi
  if [ "$OS" == "Ubuntu" ]; then
    if [ "$OS_MAJOR_VERSION" == "16.04" ]; then
      sudo apt -y install python-dev python-pip libcairo2-dev libffi-dev build-essential nginx
    else
      sudo apt -y install python3-dev python3-pip libcairo2-dev libffi-dev build-essential nginx
    fi
  fi

  #### Create a Graphite Python virtualenv ####
  if [ ! -f "/opt/graphite/bin/python${PYTHON_MAJOR_VERSION}" ]; then
    echo -e "\e[96m####    CREATING Graphite virtualenv    #### $COLOUR_OFF"
    sleep 2
    cd /opt || exit 1

    # @added 20201016 - Branch #3068: SNAB
    # As per https://github.com/graphite-project/graphite-web/issues/2566
    # pip${PYTHON_MAJOR_VERSION} uninstall -y virtualenv
    # pip${PYTHON_MAJOR_VERSION} install virtualenv==16.7.10
    pip3 uninstall -y virtualenv
    pip3 install virtualenv==16.7.10

    virtualenv --python="${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/bin/python${PYTHON_MAJOR_VERSION}" graphite
  else
    echo -e "Skipping, setting up the Graphite virtualenv, already done - \e[32mOK $COLOUR_OFF"
    sleep 1
  fi

  if [ ! -f /opt/graphite/webapp/graphite/local_settings.py ]; then
    echo -e "\e[96m####    INSTALLING Graphite    #### $COLOUR_OFF"
    echo "Installing Graphite"
    sleep 2
    cd /opt/graphite || exit 1
    source bin/activate

    export PYTHONPATH="/opt/graphite/lib/:/opt/graphite/webapp/"
  #    bin/"pip${PYTHON_MAJOR_VERSION}" install --no-binary=:all: https://github.com/graphite-project/whisper/tarball/master
  #    bin/"pip${PYTHON_MAJOR_VERSION}" install --no-binary=:all: https://github.com/graphite-project/carbon/tarball/master
  #    bin/"pip${PYTHON_MAJOR_VERSION}" install --no-binary=:all: https://github.com/graphite-project/graphite-web/tarball/master

  # @modified 20220423 - Task #4534: Build and test skyline v3.0.0
  #    bin/"pip${PYTHON_MAJOR_VERSION}" install --no-binary=:all: https://github.com/graphite-project/whisper/archive/1.1.7.tar.gz
  #    bin/"pip${PYTHON_MAJOR_VERSION}" install --no-binary=:all: https://github.com/graphite-project/carbon/archive/1.1.7.tar.gz
  #    bin/"pip${PYTHON_MAJOR_VERSION}" install --no-binary=:all: https://github.com/graphite-project/graphite-web/archive/1.1.7.tar.gz
    bin/"pip${PYTHON_MAJOR_VERSION}" install --no-binary=:all: https://github.com/graphite-project/whisper/archive/$GRAPHITE_VERSION.tar.gz
    bin/"pip${PYTHON_MAJOR_VERSION}" install --no-binary=:all: https://github.com/graphite-project/carbon/archive/$GRAPHITE_VERSION.tar.gz
    bin/"pip${PYTHON_MAJOR_VERSION}" install --no-binary=:all: https://github.com/graphite-project/graphite-web/archive/$GRAPHITE_VERSION.tar.gz

    bin/"pip${PYTHON_MAJOR_VERSION}" install gunicorn

    sed "s/#SECRET_KEY.*/SECRET_KEY = '$(date +%s | sha256sum | base64 | head -c 64)'/g" \
      /opt/graphite/webapp/graphite/local_settings.py.example > /opt/graphite/webapp/graphite/local_settings.py

    GRAPHITE_ROOT="/opt/graphite"
    PYTHONPATH=$GRAPHITE_ROOT/webapp "/opt/graphite/lib/python${PYTHON_MAJOR_VERSION}/site-packages/django/bin/django-admin.py" migrate --settings=graphite.settings --run-syncdb

    # @added 20201016 - Branch #3068: SNAB
    # As per https://github.com/graphite-project/graphite-web/issues/2566
    deactivate
    cd
    pip${PYTHON_MAJOR_VERSION} uninstall -y virtualenv
    pip${PYTHON_MAJOR_VERSION} install virtualenv
  fi

  if [ "$OS" == "CentOS" ]; then
    sudo chown nginx:nginx /opt/graphite/storage/graphite.db
# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
#      rm -f /etc/nginx/conf.d/default.conf
    NGINX_GRAPHITE_CONFIG="/etc/nginx/conf.d/graphite.conf"
    cp /etc/nginx/nginx.conf /etc/nginx/nginx.conf.bak

# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
#      cat /etc/nginx/nginx.conf | while read line
#      do
#        ADD=$(echo $line | grep -c server)
#        if [ $ADD -eq 0 ]; then
#          echo $line >> /etc/nginx/nginx.conf.new
#        else
#          break
#        fi
#      done
#      echo "}" >> /etc/nginx/nginx.conf.new
#      cat /etc/nginx/nginx.conf.new > /etc/nginx/nginx.conf

  fi
  NGINX_GRAPHITE_CONFIG="/etc/nginx/conf.d/graphite.conf"
  if [ "$OS" == "Ubuntu" ]; then
    sudo chown www-data:www-data /opt/graphite/storage/graphite.db
    if [ "$OS_MAJOR_VERSION" == "16.04" ]; then
      NGINX_GRAPHITE_CONFIG="/etc/nginx/sites-available/graphite.conf"
      rm -f /etc/nginx/sites-enabled/default
    fi
  fi

  echo -e "\e[96m####    DEPLOYING Graphite nginx config    #### $COLOUR_OFF"
  sleep 2

  echo "upstream graphite {
    server 127.0.0.1:8080 fail_timeout=0;
}

server {
    listen 8888 default_server;

    server_name $YOUR_SKYLINE_SERVER_FQDN;

    allow $YOUR_OTHER_IP_ADDRESS/32;
    allow $YOUR_SERVER_IP_ADDRESS/32;
    deny all;
    root /opt/graphite/webapp;

    access_log /var/log/nginx/graphite.access.log;
    error_log  /var/log/nginx/graphite.error.log;

    location = /favicon.ico {
        return 204;
    }

    # serve static content from the \"content\" directory
    location /static {
        alias /opt/graphite/webapp/content;
        expires max;
    }

    location / {
        try_files \$uri @graphite;
    }

    location @graphite {
        proxy_pass_header Server;
        proxy_set_header Host \$http_host;
        proxy_redirect off;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Scheme \$scheme;
        proxy_connect_timeout 10;
        proxy_read_timeout 10;
        proxy_pass http://graphite;
    }
}" > "$NGINX_GRAPHITE_CONFIG"

  if [ "$OS" == "CentOS" ]; then
    # SELinux prevents nginx from initiating outbound connections
    setsebool -P httpd_can_network_connect 1
    chcon -Rt httpd_sys_content_t /opt/graphite/webapp/
    if [ ! -f /etc/systemd/system/graphite.service ]; then
      semanage port -a -t http_port_t  -p tcp 8888
    fi
    if [ $CENTOS_6 -eq 1 ]; then
      /etc/init.d/nginx start
      chkconfig nginx on
    else
      systemctl restart nginx
    fi
  fi
  if [ "$OS" == "Ubuntu" ]; then
    if [ "$OS_MAJOR_VERSION" == "16.04" ]; then
      sudo ln -s /etc/nginx/sites-available/graphite.conf /etc/nginx/sites-enabled/
    fi
    systemctl restart nginx
  fi

  echo -e "\e[96m####    DEPLOYING Graphite configs    #### $COLOUR_OFF"
  sleep 2

  cat /opt/skyline/github/skyline/utils/dawn/carbon.conf > /opt/graphite/conf/carbon.conf
  cat /opt/graphite/conf/storage-schemas.conf.example > /opt/graphite/conf/storage-schemas.conf
  cat /opt/graphite/conf/storage-aggregation.conf.example > /opt/graphite/conf/storage-aggregation.conf
# @modified 20220423 - Task #4534: Build and test skyline v3.0.0
    # cat /opt/graphite/conf/relay-rules.conf.example | sed -e 's/127\.0\.0\.1:2014:b/127\.0\.0\.1:2024/g' > /opt/graphite/conf/relay-rules.conf
  cat /opt/graphite/conf/relay-rules.conf.example \
    | sed -e 's/127\.0\.0\.1:2004:a/127\.0\.0\.1:2014:a/g' \
    | sed -e 's/127\.0\.0\.1:2104:b/127\.0\.0\.1:2024/g' > /opt/graphite/conf/relay-rules.conf

  if [ "$OS" == "CentOS" ]; then
    if [ $CENTOS_6 -eq 1 ]; then
      echo "cd ${PYTHON_VIRTUALENV_DIR}/projects/graphite/ && source bin/activate && /opt/graphite/bin/carbon-cache.py start" >> /etc/rc.d/rc.local
      echo "cd ${PYTHON_VIRTUALENV_DIR}/projects/graphite/ && source bin/activate && /opt/graphite/bin/carbon-realy.py start" >> /etc/rc.d/rc.local
    fi
  fi
  echo -e "\e[96m####    STARTING Graphite    #### $COLOUR_OFF"
  sleep 2

  if [ "$OS" == "CentOS" ]; then
    if [ $CENTOS_6 -eq 1 ]; then
      /opt/graphite/bin/carbon-cache.py start
      /opt/graphite/bin/carbon-relay.py start
      PYTHONPATH=/opt/graphite/webapp /opt/graphite/bin/gunicorn wsgi --workers=4 --bind=127.0.0.1:8080 --log-file=/var/log/gunicorn.log --preload --pythonpath=/opt/graphite/webapp/graphite &
    fi
  fi
  if [[ "$OS" == "Ubuntu" || "$OS" == "CentOS" ]]; then
#      if [ "$OS_MAJOR_VERSION" == "16.04" ]; then
#    if [[ "$OS_MAJOR_VERSION" == "16.04" || "$OS_MAJOR_VERSION" == "18.04" || $CENTOS_8 -eq 1 ]]; then
    if [[ "$OS_MAJOR_VERSION" == "20.04" || "$OS_MAJOR_VERSION" == "18.04" || $CENTOS_8 -eq 1 ]]; then
      echo "[Unit]
Description=carbon-cache instance %i (Graphite)

[Service]
Environment=PATH=/opt/graphite/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin
Environment=VIRTUAL_ENV=/opt/graphite
User=root
Group=root
ExecStartPre=/bin/rm -f /opt/graphite/storage/carbon-cache-%i.pid
ExecStart=/opt/graphite/bin/carbon-cache.py --instance=%i start --pidfile=/opt/graphite/storage/carbon-cache-%i.pid
Type=forking
PIDFile=/opt/graphite/storage/carbon-cache-%i.pid
LimitNOFILE=128000

[Install]
WantedBy=multi-user.target" > /etc/systemd/system/carbon-cache.service
      # chmod 0755 /etc/systemd/system/carbon-cache.service
      echo "[Unit]
Description=Graphite Carbon Relay
After=network.target

[Service]
Environment=/opt/graphite/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin
Environment=VIRTUAL_ENV=/opt/graphite
Type=forking
StandardOutput=syslog
StandardError=syslog
ExecStart=/opt/graphite/bin/carbon-relay.py --config=/opt/graphite/conf/carbon.conf --pidfile=/var/run/carbon-relay.pid start
ExecReload=/bin/kill -USR1 $MAINPID
PIDFile=/var/run/carbon-relay.pid
Restart=always

[Install]
WantedBy=multi-user.target" > /etc/systemd/system/carbon-relay.service
    # chmod 0755 /etc/systemd/system/carbon-relay.service
    echo "[Unit]
Description = Graphite

[Service]
Environment=PATH=/opt/graphite/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin
Environment=PYTHONPATH=/opt/graphite/webapp:/opt/graphite
Environment=VIRTUAL_ENV=/opt/graphite
WorkingDirectory=/opt/graphite/webapp
PIDFile=/var/run/graphite/graphite.pid
ExecStart=/opt/graphite/bin/gunicorn wsgi --workers=4 --bind=127.0.0.1:8080 --log-file=/var/log/gunicorn.log --preload --pythonpath=/opt/graphite/webapp/graphite
ExecReload=/bin/kill -s HUP $MAINPID
ExecStop=/bin/kill -s TERM $MAINPID

[Install]
WantedBy = multi-user.target" > /etc/systemd/system/graphite.service
      # chmod 0755 /etc/systemd/system/graphite.service
      systemctl daemon-reload
      systemctl restart carbon-cache
      systemctl restart carbon-relay
      systemctl restart graphite
      systemctl enable carbon-cache
      systemctl enable carbon-relay
      systemctl enable graphite
    fi
  fi
  cd
fi

GRAPHITE_HOST_NOT_SET=$(cat /opt/skyline/github/skyline/skyline/settings.py | grep -c "GRAPHITE_HOST = 'YOUR_GRAPHITE_HOST.example.com'")
if [ $GRAPHITE_HOST_NOT_SET -eq 1 ]; then
  echo -e "\e[96m####    UPDATING skyline settings with Graphite variables    #### $COLOUR_OFF"
  sleep 2

  cat /opt/skyline/github/skyline/skyline/settings.py > /opt/skyline/github/skyline/skyline/settings.py.no.GRAPHITE_HOST
  cat /opt/skyline/github/skyline/skyline/settings.py.no.GRAPHITE_HOST \
    | sed -e "s/GRAPHITE_HOST = 'YOUR_GRAPHITE_HOST\.example\.com'/GRAPHITE_HOST = '$YOUR_SKYLINE_SERVER_FQDN'/g" \
    | sed -e "s/GRAPHITE_PORT = '80'/GRAPHITE_PORT = '8888'/g" \
    | sed -e "s/CARBON_HOST = GRAPHITE_HOST/CARBON_HOST = '127\.0\.0\.1'/g" \
    | sed -e "s/CARBON_PORT = .*/CARBON_PORT = 2003/g" \
    | sed -e "s/SKYLINE_METRICS_CARBON_HOST = GRAPHITE_HOST/SKYLINE_METRICS_CARBON_HOST = '127\.0\.0\.1'/g" \
    | sed -e "s/SKYLINE_METRICS_CARBON_PORT = .*/SKYLINE_METRICS_CARBON_PORT = 2003/g" \
    | sed -e "s/SERVER_METRICS_NAME = 'YOUR_HOSTNAME'/SERVER_METRICS_NAME = '$HOSTNAME'/g" > /opt/skyline/github/skyline/skyline/settings.py
fi
SKYLINE_SERVER_FQDN_IN_HOSTS=$(cat /etc/hosts | grep -c "$YOUR_SKYLINE_SERVER_FQDN")
if [ $SKYLINE_SERVER_FQDN_IN_HOSTS -eq 0 ]; then
  echo "$YOUR_SERVER_IP_ADDRESS $YOUR_SKYLINE_SERVER_FQDN" >> /etc/hosts
fi

SKYLINE_SERVICES="horizon
panorama
analyzer
analyzer_batch
mirage
ionosphere
luminosity
boundary
flux
webapp
vista
snab
crucible
thunder"

for i in $(find /opt/skyline/github/skyline/etc/systemd/system -type f)
do
  /bin/cp -f $i /etc/systemd/system/
done
systemctl daemon-reload

mkdir -p /opt/skyline/github/skyline/skyline/webapp/static/dump
chown skyline:skyline -R /opt/skyline/github

echo -e "\e[96m####    STARTING skyline horizon service to seed data to    #### $COLOUR_OFF"
sleep 2
START_ERRORS=0
systemctl start horizon
if [ $? -ne 0 ]; then
  echo -e "error :: failed to start horizon - \e[31mFAIL $COLOUR_OFF"
  START_ERRORS=1
else
    echo -e "horizon started - \e[32mOK $COLOUR_OFF"
fi

# @modified 20210328 - [Q] The "horizon.test.pickle" test is getting an error. #419
# Moved to before Graphite install if it is done so that carbon-relay does not
# block the seed_data socket connection
echo -e "\e[96m####    SEEDING skyline with data    #### $COLOUR_OFF"
if [ $DO_GRAPHITE_INSTALL -eq 1 ]; then
  echo "Stopping carbon-relay so it does not block the seed_data socket connection"
  systemctl stop carbon-relay
fi
sleep 2
cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}" || exit 1
source bin/activate
bin/python${PYTHON_MAJOR_VERSION} /opt/skyline/github/skyline/utils/seed_data.py
deactivate
if [ $DO_GRAPHITE_INSTALL -eq 1 ]; then
  echo "Starting carbon-relay"
  systemctl start carbon-relay
fi

echo -e "\e[96m####    STARTING skyline services    #### $COLOUR_OFF"
sleep 2
for skyline_app in $SKYLINE_SERVICES
do
  systemctl start $skyline_app
  if [ $? -ne 0 ]; then
    echo -e "error :: failed to start $skyline_app - \e[31mFAIL $COLOUR_OFF"
    START_ERRORS=1
  else
    echo -e "$skyline_app started - \e[32mOK $COLOUR_OFF"
  fi
  systemctl enable $skyline_app
done

if [ $START_ERRORS -eq 1 ]; then
    echo -e "error :: failed to start some Skyline services - \e[31mFAIL $COLOUR_OFF"
fi

echo "Skyline services started"

SERVICES="/opt/skyline/github/skyline/bin/horizon.d
/opt/skyline/github/skyline/bin/panorama.d
/opt/skyline/github/skyline/bin/analyzer.d
/opt/skyline/github/skyline/bin/analyzer_batch.d
/opt/skyline/github/skyline/bin/mirage.d
/opt/skyline/github/skyline/bin/webapp.d
/opt/skyline/github/skyline/bin/ionosphere.d
/opt/skyline/github/skyline/bin/luminosity.d
/opt/skyline/github/skyline/bin/boundary.d
/opt/skyline/github/skyline/bin/flux.d
/opt/skyline/github/skyline/bin/vista.d
/opt/skyline/github/skyline/bin/thunder.d"
for i_service in $SERVICES
do
  sudo -u skyline $i_service status
done

echo -e "Skyline is deployed and running - \e[32mOK $COLOUR_OFF"
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
echo ""
ENDED=$(date)
echo "$0"
echo "started at :: $STARTED"
echo "ended at :: $ENDED"
echo ""
echo ""
echo -e "\e[93mREMEMBER THIS IS A TEST DEPLOYMENT AND SHOULD BE DESTROYED OR RECONFIGURED AND HARDENED FOR PRODUCTION USE $COLOUR_OFF"
# @modified 20180915 - Feature #2550: skyline.dawn.sh
# Use the skyline user
# echo "and Skyline is running as root"
# @added 20191016 - Branch #3262: py3
#echo "There is no GRAPHITE_HOST configured and there are some errors expected in the logs"
#echo "related to no rows in MySQL tables and memcache, etc"
if [ $DO_GRAPHITE_INSTALL -eq 1 ]; then
  echo "Graphite is available at http://$YOUR_SKYLINE_SERVER_FQDN:8888 from IP address $YOUR_OTHER_IP_ADDRESS"
  echo "There are some errors expected in the logs related to no rows in MySQL tables and memcache, etc"
else
  echo "There is no GRAPHITE_HOST configured and there are some errors expected in the logs"
  echo "related to no rows in MySQL tables and memcache, etc"
fi
echo ""
echo -e "\e[96m####    CONGRATULATIONS    #### $COLOUR_OFF"
echo "Please visit https://$YOUR_SKYLINE_SERVER_FQDN (rememeber to add it to your hosts file)"
echo "In the Panaroma tab shortly you should see an anomaly (not there will be no graph)"
echo "In the rebrow tab you should see Redis keys"
echo "For rebrow your Redis password is $REDIS_PASSWORD"
echo ""
echo "To stop the Skyline processes run:"
echo "kill \$(ps aux | grep -v grep | grep skyline | tr -s ' ' ',' | cut -d',' -f2 | tr '\n' ' ')"
echo "Or for each service run:"
echo "sudo -u skyline /opt/skyline/github/skyline/bin/<service>.d stop"
echo ""
echo -e "\e[93mNOT SUITABLE FOR PRODUCTION WITHOUT RECONFIGURATION AND HARDENED!!!! $COLOUR_OFF"
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
