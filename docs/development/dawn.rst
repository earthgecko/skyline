******************
Development - dawn
******************

Build script - utils/dawn/skyline.dawn.sh
=========================================

The Skyline dawn branch adds a build script for **TESTING** only, it is not
recommended for production use.

.. warning:: When running this test instance there are expected to be errors in
  the logs as there is no real Graphite configured, no :mod:`settings.ALERTS`,
  no alert email addresses, etc, etc.  In fact there are no real time series
  apart from the ``metrics.horizon.test.udp`` data seeded into Redis, which goes
  stale quickly.  The Skyline now and panorama pages are not going to display
  graph data.

However seeing as it may be used by people for testing, it is written to try and
ensure secure a set up as possible, with the exception of the definitions of
iptables/iptables6/ufw.  It should not be used to deploy any other kind of
environment unless the other components such as Graphite, iptables/iptables6/ufw
are configured at the same time.  The build script is suitable for:

- CentOS 6.9
- CentOS 7.5 and 7.6
- Ubuntu 16.04.4 LTS

The build script has user defined variables that can be edited directly in the
script or read from /etc/skyline/skyline.dawn.conf.  The build script does the
following:

- Install the build requirements via yum or apt-get
- Install the MySQL/mariadb server via yum or apt-get, configures it for
  ``innodb_file_per_table=1``,starts it and secures it.
- Downloads, builds, installs, configures and starts Redis
- Installs memcached and its dependencies, configures and starts it
- Creates a skyline system user
- Downloads, builds and deploys the current Skyline Python version (2.7.14) and
  sets up the Skyline Python virtualenv
- Creates the required Skyline directories
- Clones the Skyline repo and checkouts the declared version
- Installs Skyline requirements.txt
- Install Apache and deploys the Skyline reverse proxy vhost config and creates
  the htpasswd resource
- Creates a self-signed SSL certificate for the Skyline reverse proxy vhost
- Deploys the Skyline ``skyline/settings.py`` with the declared variables
- Creates the Skyline MySQL/mariadb database and grants privileges
- Starts the Skyline services
- Seeds Skyline with an anomalous time series data for ``horizon.test.udp``
  metric and which creates an anomaly to be reviewed in the Skyline webapp UI
- The script creates tmp files in /tmp/skyline.dawn.* file namespace so it can
  be run in an idempotent manner.

.. warning:: remember there are expected to be errors and no time series, no
  Graphite, no graphs, etc.

Example usage:

.. code-block:: bash

  # Fetch
  wget -O /tmp/skyline.dawn.sh https://raw.githubusercontent.com/earthgecko/skyline/master/utils/dawn/skyline.dawn.sh
  # Always review scripts before running them
  cat /tmp/skyline.dawn.sh
  if [ -f /etc/redhat-release ]; then
    yum -y install net-tools
  fi
  # Determine public IP address
  USE_IP=$(ifconfig | grep -v "127.0.0.1" | grep "inet addr:" | cut -d':' -f2 | cut -d' ' -f1)
  if [ -f /etc/redhat-release ]; then
    yum -y install net-tools
    CENTOS_7=$(cat /etc/redhat-release | grep -c "release 7\.")
    if [ $CENTOS_7 -eq 1 ]; then
      USE_IP=$(ifconfig | grep -v "127.0.0.1" | grep "inet " | sed -e 's/.*inet //g;s/ .*//g')
    fi
  fi
  # Create the Skyline config dir REPLACE THE YOUR_ and SET_ variables with your
  # own
  mkdir -p /etc/skyline
  # Create the Skyline dawn config
  echo 'YOUR_SERVER_IP_ADDRESS="'$USE_IP'"                      # YOUR Skyline server public IP address
  YOUR_SKYLINE_SERVER_FQDN="YOUR_SKYLINE_FQDN"                  # YOUR Skyline server FQDN
  YOUR_EMAIL="YOUR_EMAIL"                                       # YOUR email address for the httpd server admin
  YOUR_OTHER_IP_ADDRESS="YOUR_IP_ADDRESS"                       # YOUR current public IP address that you will be connecting from
  WEBAPP_AUTH_USER="admin"                                      # The username you want to use for http authentication
  WEBAPP_AUTH_USER_PASSWORD="SET_HTTP_AUTH_PASSWORD"            # The password you want to use for http authentication
  MYSQL_ROOT_PASSWORD="SET_THE_ROOT_MYSQL_USER_PASSWORD"        # The MySQL root user password
  MYSQL_SKYLINE_PASSWORD="SET_THE_SKYLINE_MYSQL_USER_PASSWORD"  # The Skyline DB user password
  REDIS_PASSWORD="SET_A_REALLY_REALLY_LONG_Redis_PASSWORD"      # The Redis password
  SKYLINE_RELEASE="master"                                      # The Skyline release/branch/commit to deploy' > /etc/skyline/skyline.dawn.conf
  # Check that the configurations are as desired, specifically check that
  # the IP address declared in YOUR_SERVER_IP_ADDRESS from the USE_IP variable
  # is correct
  cat /etc/skyline/skyline.dawn.conf
  # Run it
  chmod 0755 /tmp/skyline.dawn.sh
  /tmp/skyline.dawn.sh

Add an entry into your /etc/hosts file that maps the instance IP to
YOUR_SKYLINE_SERVER_FQDN and open the reported URL in your web browser,
accepting the self signed SSL certificate.

Check the Skyline logs.

.. code-block:: bash

  # Check the logs
  tail -n 60 /var/log/skyline/*.log
