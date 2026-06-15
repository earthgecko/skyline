******************
Development - dawn
******************

Build script - utils/dawn/skyline.dawn.sh
=========================================

The Skyline dawn branch adds a build script, it is not recommended for
production use without configuring suitable firewall rules, reconfiguring
Graphite and deploying a proper SSL certificate.  Note that on CentOS builds
firewalld is installed and configured by default to restrict access to
``YOUR_OTHER_IP_ADDRESS`` along with disabling IPv6.

.. warning:: When running this test instance there are expected to be errors in
  the logs to begin with and if you choose to not install Graphite there are no
  real metrics or data, there are no :mod:`settings.ALERTS`, etc, etc.  In fact
  if you choose to not install Graphite, there are no real time series apart
  from the ``horizon.test.udp`` data seeded into Redis, which goes stale
  quickly and the Skyline /now and /panorama pages are not going to display graph
  data on the test ``horizon.test.udp`` data.  Further if you do not opt to
  install Graphite, Skyline will throws errors until you configure with a
  Graphite to which to send it's own metrics to.

This build script is written to try and ensure a secure set up as possible,
with the exception of the definitions of iptables/iptables6/ufw.  It should not
be used to deploy any other kind of environment unless the other components such
as Graphite, iptables/iptables6/ufw are configured at the same time.  The build
script is suitable for:

- CentOS Stream 9 (last build time 24 mins)
- CentOS Stream 8 (last build time 20 mins) **LAST TESTED v4.0.0**
- Ubuntu 20.04 LTS (last build time 21 mins) **LAST TESTED v4.0.0**
- Ubuntu 22.04 LTS (last build time 21 mins) **LAST TESTED v4.0.0**
- **NO LONGER SUPPORTED** (as CentOS 7 is on python 2.7 and/or rh SCL 3.6 which are
  incompatible with Skyline) CentOS 7.5 and 7.6 (last build time 23 mins)
- **NO LONGER SUPPORTED** (OS now end of life) Ubuntu 16.04.4 LTS (last build time 21 mins)
- **NO LONGER SUPPORTED** - Ubuntu 18.04 LTS (last build time 24 mins)

Note that is recommended to run Skyline on CentOS Stream 9 (because CentOS
Stream 8 is end-of-life on 31 May 2024).  CentOS Stream is used in development,
testing and production instances are run on CentOS Stream.  Although the build
script works on Ubuntu and all the applications run, there is no long term or
production testing done on Ubuntu.

The build script has user defined variables that can be edited directly in the
script or read from /etc/skyline/skyline.dawn.conf.  The build script does the
following:

- Install the build requirements via yum or apt-get
- Install the latest MariaDB server via yum or apt-get, configures it for
  ``innodb_file_per_table=1``,starts it and secures it.
- Downloads (builds), installs, configures and starts Redis (or redis-stack-server)
- Installs memcached and its dependencies, configures and starts it
- Creates a skyline system user
- Downloads, builds and deploys the current Skyline Python version (3.10.15) and
  sets up the Skyline Python virtualenv
- Creates the required Skyline directories
- Clones the Skyline repo and checkouts the declared version
- Installs Skyline requirements.txt
- Install nginx and deploys the Skyline reverse proxy config and creates
  the htpasswd resource
- Creates a self-signed SSL certificate for the Skyline reverse proxy vhost
- Deploys the Skyline ``skyline/settings.py`` with the declared variables
- Creates the Skyline MariaDB database and grants privileges
- Installs, configures and starts Graphite (carbon-cache, carbon-relay and
  graphite-web).  If you already have Graphite this can be disabled by setting
  ``INSTALL_GRAPHITE=0`` in the skyline.dawn.conf
- Optionally installs Prometheus and/or VictoriaMetrics.  And configures
  Prometheus to scrape itself and send it's metrics to Skyline.
- Optionally installs telegraf and sends server, redis and mariadb/mysql metrics
  to Graphite.
- Optionally installs node_exporter and exposes node metrics which Prometheus
  scrapes.
- Starts the Skyline services
- Seeds Skyline with an anomalous time series data for ``horizon.test.udp``
  metric and which creates an anomaly to be reviewed in the Skyline webapp UI
- The script creates tmp files in /tmp/skyline.dawn.* file namespace so it can
  be run in an idempotent manner.

Review https://github.com/earthgecko/skyline/blob/master/utils/dawn/skyline.dawn.sh
at least take the time to look through it to see what it is doing.

redis-server or redis-stack-server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As of Skyline v4.0.0 there is the ability to install redis-stack-server which adds
Redis Timeseries for the ingestion and storage of labelled metrics (Prometheus,
VictoriaMetrics, InfluxDB, etc label/value style metrics).  Although this is
possible using the normal Redis server by building and compiling the Redis
Timeseries module to be loaded by Redis, building it introduces non standard
gcc-toolset packages which are required to build the Redis Timeseries module.
Although this does work it is messy and has an effect on the sudo package.
Therefore by default the build script now installs redis-stack-server binaries
which include the Redis Timeseries module.  Although the are official redis.io
package management repos for redis-stack-server, they often provide release
candidate versions rather than stable versions.  For this reason redis-stack-server
is downloaded at the latest stable version (at the time of the build script
release) and is fixed to that version until the next release at which the build
script will be updated.

It is still possible to deploy redis-server (CentOS Stream 8 only) rather than
redis-stack-server using the build script, but Redis Timeseries will not be
available and therefore labelled metrics (Prometheus, VictoriaMetrics, InfluxDB)
cannot not be ingested and stored.

As of Skyline v5.0.0 and the move to CentOS Stream 9 only redis-stack-server is
supported.

Prometheus and VictoriaMetrics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As of Skyline v4.0.0 there is the ability to install Prometheus and VictoriaMetrics
via the build script.  If you opt to install Prometheus, VictoriaMetrics will be
installed as well.  If you have your own Prometheus server/s and you want to
ingest metrics from your own Promethei, you **must** install VictoriaMetrics, e.g.
``INSTALL_VICTORIAMETRICS=1``.  Skyline uses VictoriaMetrics for its own longterm
storage of labelled metrics.

Do note that if you install Prometheus it will be configured to send all it's
metrics to Skyline, this may not be what you desire and it is mainly for testing.

By default in the below code block they are **not** installed as they are not
required by Skyline.  Unlike Graphite which is required for this Skyline build
because Skyline is configured to send it's metrics to the installed Graphite.

Additional components
~~~~~~~~~~~~~~~~~~~~~

Bear in mind this is a **test** build script it may install Graphite, Prometheus
and VictoriaMetrics, but it is **for testing**.  It is not recommended to run
these applications on a proper Skyline server, they should be run elsewhere.
This will run fine as it is but it will become overloaded at a certain metric
volume if external metrics are sent to it.  The addition of these components
here are for convenience sake, not production.

Deploy using the build script
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This **should not** be run on any machine that is already doing something, it
should be a machine that is dedicated to running Skyline.  Running this on an
existing node may break whatever you are running on the machine.

This build script must be run as the root user (not tested with sudo), but the
deployed Skyline does not run as root, it is runs with a deployed skyline user.
Graphite services are run as root and proxied via nginx which restricts access
by IP address.

With Ubuntu the build script needs to be supervised because apt asks questions a
number of times and requires user input.  Although the latest is not tested on
Ubuntu yet so if you have a problem use CentOS Stream 9 (alma 9, rocky 9, et al
may work as well but not tested).

Replace all the ``<YOUR_`` variables with your own variables before pasting into
the terminal or change them in an editor and save on the server to
/etc/skyline/skyline.dawn.conf.  Note that other initial variables in the
skyline.dawn.sh can also be overridden by declaring them in
/etc/skyline/skyline.dawn.conf, below are the ones a normal user needs to be
concerned with and things such as REDIS_STACK_SERVER_VERSION, etc are not covered
here.  If you wish to know what the other variables are or experiment with them
please review the skyline.dawn.sh file.

Note that by default in the script telegraf and node_exporter **are installed**
if you do not override the INSTALL_TELEGRAF and INSTALL_NODE_EXPORTER variables
in the below config and set them to 0.  If telegraf is installed it sends server,
mysql/mariadb and redis metrics to Graphite (and Graphite sends them to Skyline).
You do choose to have telegraf installed make sure that TELEGRAF_INTERFACES is
set with the interface or interfaces that you want metrics collected on.

If this is just a test node, do elect to install Prometheus/VictoriaMetrics, 
telegraf and node_exporter.  This will feed the system lots of data, around
3000 metrics.

If you are deploying this with a view to running it as your Skyline server then
consider disabling whatever components that you will not require and consider
how you want to set it up with regards to your existing telemetry/collectors.

.. code-block:: bash

  # Create the Skyline config dir
  mkdir -p /etc/skyline
  # Create YOUR Skyline dawn config and REPLACE THE <YOUR_ variables with your own
  # If you wish to install Prometheus and/or VictoriaMetrics ensure you change
  # the INSTALL_PROMETHEUS and INSTALL_VICTORIAMETRICS value to 1 below
  SKYLINE_RELEASE="v5.0.0-alpha"
  echo 'YOUR_SERVER_IP_ADDRESS="<YOUR_SERVER_PUBLIC_IP>"       # YOUR Skyline server public IP address
  YOUR_SKYLINE_SERVER_FQDN="<YOUR_SKYLINE_FQDN>"               # YOUR Skyline server FQDN (does not need DNS for testing purposes)
  YOUR_OTHER_IP_ADDRESS="<YOUR_IP_ADDRESS>"                    # YOUR current public IP address that you will be connecting from
  FIREWALL_ENABLED=1                                           # CentOS ONLY - whether to enable the firewall and restrict as to YOUR_OTHER_IP_ADDRESS
  DISABLE_IPV6=1                                               # CentOS ONLY - whether to disable ipv6
  WEBAPP_AUTH_USER="admin"                                     # The username you want to use for http authentication
  WEBAPP_AUTH_USER_PASSWORD="<YOUR_HTTP_AUTH_PASSWORD>"        # The password you want to use for http authentication
  MYSQL_ROOT_PASSWORD="<YOUR_ROOT_MYSQL_USER_PASSWORD>"        # The MySQL root user password
  MYSQL_SKYLINE_PASSWORD="<YOUR_SKYLINE_MYSQL_USER_PASSWORD>"  # The Skyline DB user password
  REDIS_PASSWORD="<YOUR_REALLY_REALLY_LONG_Redis_PASSWORD>"    # The Redis password
  SKYLINE_RELEASE="'$SKYLINE_RELEASE'"                         # The Skyline release/branch/commit to deploy
  INSTALL_GRAPHITE=1                                           # Install Graphite 0 = no, 1 = yes
  GRAPHITE_VERSION="1.1.10"
  GRAPHITE_RETENTION="60s:7d,10m:2y"                           # The Graphite retention periods you want
  INSTALL_PROMETHEUS=0                                         # Install Prometheus 0 = no, 1 = yes (CentOS Stream 9 only)
  PROMETHEUS_VERSION="2.55.0"
  INSTALL_VICTORIAMETRICS=0                                    # Install VictoriaMetric 0 = no, 1 = yes (CentOS Stream 9 only)
  VICTORIAMETRICS_VERSION="1.105.0"
  INSTALL_TELEGRAF=1                                           # Install telegraf 0 = no, 1 = yes (CentOS Stream 9 only)
  # TELEGRAF_INTERFACES="[\"eth0\",\"ens3\",\"enp3s0\"]"       # An example of network interfaces
  TELEGRAF_INTERFACES="[<YOUR_INTERFACES>]"  "                 # The network interfaces of THIS server you want metrics collected for escape the double quotes as in the example
  INSTALL_NODE_EXPORTER=1                                      # Install node_exporter 0 = no, 1 = yes (CentOS Stream 9 only)
  ' > /etc/skyline/skyline.dawn.conf

  # Check that the configurations are as desired, specifically check that
  # the IP address declared in YOUR_SERVER_IP_ADDRESS and the YOUR_OTHER_IP_ADDRESS
  # variables are correct
  cat /etc/skyline/skyline.dawn.conf

  if [ -f /etc/redhat-release ]; then
    yum -y install wget
  else
    sudo apt-get install wget
  fi

  # Fetch
  wget -O /tmp/skyline.dawn.sh "https://raw.githubusercontent.com/earthgecko/skyline/${SKYLINE_RELEASE}/utils/dawn/skyline.dawn.sh"

  # Review it
  cat /tmp/skyline.dawn.sh

  # Run it
  chmod 0755 /tmp/skyline.dawn.sh
  /tmp/skyline.dawn.sh   # This will take ~30mins

Copy the final part of the output from the build and save it as it contains all
the details regarding the build, list endpoints, ports, configs, etc.  You will
need all of those to view and manage the all the components you elected to
install.

Remeber to add the entry into **YOUR** /etc/hosts file that maps the instance IP
to YOUR_SKYLINE_SERVER_FQDN and open the URL in your web browser, accepting the
self signed SSL certificate.

Apologies for repeating things here and there, but think of it as reinforcement
learning :)  There is so much to explain, some times things get repeated.

Check the Skyline logs.

.. code-block:: bash

  # Check the logs
  tail -n 60 /var/log/skyline/*.log

.. raw:: html

   <p style="font-size: 0.8em; color: gray; text-align: left;">
       v5.0.0-alpha Numberwang series: A number you have seen < 1000. What other number/s explains the < 1000 number?
       See the next release to see if you got Numberwang!
   </p>


All Installed services are controlled via systemctl

**Skyline services**:
- analyzer.service
- analyzer_batch.service
- boundary.service
- crucible.service
- flux.service
- horizon.service
- ionosphere.service
- luminosity.service
- mirage.service
- panorama.service
- snab.service
- snab_flux_load_test.service
- thunder.service
- vista.service
- webapp.service

**Graphite services**:
- carbon-cache.service
- carbon-relay.service
- graphite.service

**MariaDB**:
- mariadb.service

**memcached**:
- memcached.service

**nginx**:
- nginx.service

**Redis**:
- redis-stack-server.service

**Prometheus**:
- prometheus.service

**VictoriaMetrics**:
- victoriametrics.service

**Telegraf**:
- telegraf.service

**node_exporter**:
- node-exporter.service

All services can be stopped and started using the service name (with or without
the .service extension) for example:

.. code-block:: bash

  systemctl start analyzer
  systemctl status analyzer
  systemctl stop analyzer

Skyline will now be running and in a few hours it will probably start to identify
some anomalies in it's own metrics or possibly in Graphite or Prometheus metrics.
Remember it needs at least 7 days worth of metric data before it starts working
properly with a full tank of fuel.  At which point you will want to decide what
important metrics you want to start off them and classify as Mirage metrics, by
specifying them in :mod:`settings.ALERTS`.

Relevant configuration files
============================

- Skyline nginx - /etc/nginx/conf.d/<YOUR_SKYLINE_FQDN>.conf
- Graphite nginx - /etc/nginx/conf.d/graphite.conf
- Redis - /etc/redis-stack.conf
- Graphite - /opt/graphite/conf/carbon.conf, /opt/graphite/conf/relay-rules.conf
  /opt/graphite/conf/storage-schemas.conf and /opt/graphite/conf/storage-aggregation.conf
- Skyline - /etc/skyline/skyline.conf and /opt/skyline/github/skyline/skyline/settings.py
- Prometheus - /etc/prometheus/prometheus.yml
- VictoriaMetrics - /opt/victoriametrics/victoriametrics.service
- Telegraf - /etc/telegraf/telegraf.conf and /etc/telegraf/telegraf.d/\*.conf
- node_exporter - node scrape job in /etc/prometheus/prometheus.yml

Take Skyline for a spin
=======================

After having browsed through the Skyline UI and Graphite and letting it run for
a while (> 1 hour) so data Skyline is populated with some data, if you wish to
just test Skyline and get a feel for what it does and see it do some real
analysis, etc, you can run snab_flux_load_test.  However if you opted not to
install telegraf (or Prometheus and node_exporter) then your Skyline should have
enough data to get a feel for it and you will not need to run snab_flux_load_test.

Be aware if you run snab_flux_load_test you will probably want to destroy the
instance and rebuild it if you want to run it for real because
snab_flux_load_test will create a lot of test metrics and data.

If you are running a 4 CPU instance then testing with 15000 metrics is
reasonable (if not decide for yourself how many to test with).  However if you
have telegraf and/or Prometheus installed you will already have 1000s of
metrics.

To run snab_flux_load_test change the following variable in
/opt/skyline/github/skyline/skyline/settings.py to the below values.

.. code-block:: python

  ANALYZER_PROCESSES = 3   # Must be <= to CPUs available
  SNAB_FLUX_LOAD_TEST_ENABLED = True
  SNAB_FLUX_LOAD_TEST_METRICS = 15000

Restart analyzer and then start snab_flux_load_test

.. code-block:: python

  systemctl restart analyzer
  systemctl start snab_flux_load_test

You will immediately see the number of metrics in ``carbon.relays.<HOSTNAME>-a.metricsReceived``
go up and after a short time if you have let Skyline run for > 1 hour beforehand
you will start to see on the ``carbon.`` and ``skyline.`` metric namespaces.

Remember after playing around with snab_flux_load_test, destroy the instance and
rebuild it clean for your real metrics.

Post installation tasks
=======================

If you wish to keep the instance running and use it then you need to do the
following things.

- Configure the machine firewall to only allow the machine's own public IP and
  **trusted** IPs on the <PUBLIC_IP> on the following ports
  2003 (Graphite line protocol), 2004 (Graphite pickle protocol),
  8888 (Graphite webapp) and 443 (Skyline).
- Change the Graphite config in /opt/graphite/conf/carbon.conf under the
  ``[relay]`` section **only** change ``LINE_RECEIVER_INTERFACE``,
  ``PICKLE_RECEIVER_INTERFACE`` to listen on the public IP rather than 127.0.0.1
  so that other machines can send Graphite data via <PUBLIC_IP>:2003 and :2004
- Change the ``CARBON_HOST`` and ``SKYLINE_METRICS_CARBON_HOST`` in the Skyline
  settings file /opt/skyline/github/skyline/skyline/settings.py to public IP
  rather than 127.0.0.1 as well.
- Replace the self-signed SSL certificate that is serving Skyline with a real
  SSL certificate.
- Add the machine's own public IP and other trusted IPs to
  /etc/nginx/conf.d/<YOUR_SKYLINE_FQDN>.conf and /etc/nginx/conf.d/graphite.conf
- Install and configure postfix or sendmail (or use AWS SES) to handle email and
  configure Skyline SMTP related settings.
- In /opt/skyline/github/skyline/skyline/settings.py replace the occurrences of
  ``no_email`` with the email address you want alerts sent to.

