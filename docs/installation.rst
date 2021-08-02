============
Installation
============

This is going to take some time.

Intended audience
-----------------

Skyline is not really a ``localhost`` application, it needs lots of data, unless
you have a ``localhost`` Graphite or pickle Graphite to your localhost, however
Skyline and all its components could be run on localhost if you wanted but it
is not recommended.

Given the specific nature of Skyline, it is assumed that the audience will have
a certain level of technical knowledge, e.g. it is assumed that the user will be
familiar with the installation, configuration, operation and security practices
and considerations relating to the following components:

- Graphite
- Redis
- MySQL
- Apache/nginx
- memcached

However, these types of assumptions even if stated are not useful or helpful to
someone not familiar with all the ins and outs of some required thing. This
installation document is specifically related to the required installs and
configurations of things that are directly related Skyline.  Although it cannot
possibly cover all possible set ups or scenarios, it does describe
recommendations in terms of configurations of the various components and how and
where they should be run in relation to Skyline.  There are no cfengine, puppet,
chef or ansible patterns here.

The documentation is aimed at installing Skyline securely by default.  It is
possible to run Skyline very insecurely, however this documentation does not
specify how to do that.  Both the set up and documentation are verbose.  Setting
up Skyline takes a while.

Skyline's default settings and documentation are aimed to run behind a SSL
terminated and authenticated reverse proxy and use Redis authentication.  This
makes the installation process more tedious, but it means that all these
inconvenient factors are not left as an after thought or added to some TODO list
or issue when you decide after trying Skyline, "Yes! I want Skyline, this is
cool".

For notes regarding automation and configuration management see the section at
the end of this page.

What the components do
----------------------

- Graphite - sends metric data to Skyline Horizon via a pickle (Python object
  serialization).  Graphite is a separate application that will probably be
  running on another server, although Graphite could run on the same server, in
  a production environment it will probably be a remote machine or container.
  Graphite is not part of Skyline.
- Redis - stores :mod:`settings.FULL_DURATION` seconds (usefully 24 hours worth)
  of time series data that Graphite sends to Skyline Horizon and Horizon writes
  the data to Redis.  Skyline Analyzer pulls the data from Redis for analysis.
  Redis must run on the same host as Skyline.  It may be possible to run Redis
  in another container or VM that is on the same host.
- MySQL/MariaDB - stores data about anomalies and time series features profile
  fingerprints for matching and learning things that are not anomalous.  mariadb
  can run on the same host as Skyline or it can be remote.  Running the DB
  remotely will make the Skyline a little bit slower.
- Apache/nginx - Skyline serves the webapp via gunicorn and Apache/nginx
  handles endpoint routing, SSL termination and basic http auth.  Ideally the
  reverse proxy should be run on the same host as Skyline.
- memcached - caches Ionosphere MySQL data, memcached should ideally be run on
  the same host as Skyline.

``sudo``
~~~~~~~~

Use ``sudo`` appropriately for your environment wherever necessary.

Steps
-----

.. note:: All the documentation and testing is based on running Skyline in a
  Python-3.8.10 virtualenv, if you choose to deploy Skyline another way, you are
  on your own.  Although it is possible to run Skyline in a different type of
  environment, it does not lend itself to repeatability or a common known state.

Skyline configuration
~~~~~~~~~~~~~~~~~~~~~

All Skyline configuration is handled in ``skyline/settings.py`` and in this
documentation configuration options are referred to via their docstrings name
e.g. :mod:`settings.FULL_DURATION` which links to their description in the
documentation.

There are lots of settings in ``settings.py`` do not feel intimidated by this.
The default settings should be adequate and reasonable for starting out with.
The settings that you must change and take note of are all documented further on
this page and labelled with ``[USER_DEFINED]`` in the settings.py

.. note:: You will encounter settings that are described as ADVANCED
  FEATURE or EXPERIMENTAL.  Many of these settings are not necessarily fully
  described or even partially documented as they require a deeper understanding
  of the Skyline internals, generally in terms of how Skyline pipelines, analyses
  and alerts on the data, in order to understand their use, implementation and
  execution.  This knowledge comes from use and experience and the documentation
  cannot describe every aspect of all the highly configurable manners in which
  Skyline can be run.

Dawn
~~~~

- Should you wish to review the build steps, component builds and installs
  described below, there is a convenience build script for **testing** purposes
  **only** in `utils/dawn/skyline.dawn.sh` see
  `Dawn <development/dawn.html>`__ section

Python virtualenv
~~~~~~~~~~~~~~~~~

- The first part of the installation is to build Python and create a
  Python-3.8.10 virtualenv for Skyline to run in.  For this first step in the
  installation process see and follow the steps laid out in
  `Running Skyline in a Python virtualenv <running-in-python-virtualenv.html>`__

Firewall
~~~~~~~~

- Please set up all the firewall rules to restrict access to the following
  **before** you continue to install the other components:

  - The IP address and port being used to reverse proxy the Webapp e.g.
    <YOUR_SERVER_IP_ADDRESS>:443, ensure that this is only accessible to
    specified IPs in iptables/ip6tables (further these addresses should also be
    added to the reverse proxy conf as ``Allow from`` or ``allow`` defines when
    you create the reverse proxy conf file).
  - The IP address and port being used by MySQL/MariaDB, if you are not binding
    MySQL/MariaDB to 127.0.0.1 only, ensure that the MySQL/MariaDB port declared
    in :mod:`settings.PANORAMA_DBPORT` (default 3306) is only accessible to
    specified IPs in iptables/ip6tables
  - Allow the IP address of your Graphite server/s on ports 2024 and 2025 (the
    default Graphite to Skyline Horizon ports)
  - The IP address and port being used by Redis should be mentioned here, just
    in case you are NOT running Redis with `bind 127.0.0.1`.  You should be for
    Skyline.  Consider only running Redis bound to the 127.0.0.1 interface.  If
    you have some reason for wanting Redis accessible on any other IP read the
    Redis section below, specifically the review https://redis.io/topics/security
    part.  Even if you are running multiple distributed Skyline instances Redis
    should still be bound to 127.0.0.1 only, as Skyline makes an API endpoint
    available to remote Skyline instances for any required remote Redis data
    retrieval and preprocessing.
  - If you are going to run Vista and Flux, ensure that the Skyline IP is
    allowed to connect to the Graphite node on the `PICKLE_RECEIVER_PORT`
  - Please ensure you handle all of these with iptables AND ip6tables (or the
    equivalent) **before continuing**.

Redis
~~~~~

- Install Redis - see `Redis.io <http://redis.io/>`__
- Ensure that you review https://redis.io/topics/security
- Ensure Redis has socket enabled **with the following permissions** in your
  redis.conf

::

    unixsocket /tmp/redis.sock
    unixsocketperm 777

.. note:: The unixsocket on the apt redis-server package is
  ``/var/run/redis/redis.sock`` if you use this path ensure you change
  :mod:`settings.REDIS_SOCKET_PATH` to this path

- Ensure Redis has a long ``requirepass`` set in redis.conf
- Ensure Redis ``bind`` is set in redis.conf, consider specifically stating
  ``bind 127.0.0.1`` even if you are going to run multiple distributed Skyline
  instances, Skyline gets remote Redis data preprocessed via a Skyline API so
  there is no need to bind Redis to any other IP.
- Start Redis

memcached
~~~~~~~~~

- Install memcached and start memcached see `memcached.org <https://memcached.org/>`__
- Ensure that you start memcached only bound to 127.0.0.1 by passing the daemon
  the option ``-l 127.0.0.1``, Skyline only requires memcached locally.

Skyline directories
~~~~~~~~~~~~~~~~~~~

- Make the required directories

.. code-block:: bash

    mkdir /var/log/skyline
    mkdir /var/run/skyline
    mkdir /var/dump

    mkdir -p /opt/skyline/panorama/check
    mkdir -p /opt/skyline/mirage/check
    mkdir -p /opt/skyline/crucible/check
    mkdir -p /opt/skyline/crucible/data
    mkdir -p /opt/skyline/ionosphere/check
    mkdir -p /opt/skyline/flux/processed_uploads
    mkdir /etc/skyline
    mkdir /tmp/skyline

.. note:: Ensure you provide the appropriate ownership and permissions to the
  above specified directories for the user you wish to run the Skyline process
  as.

.. code-block:: bash

    # Example using user and group Skyline
    chown skyline:skyline /var/log/skyline
    chown skyline:skyline /var/run/skyline
    chown skyline:skyline /var/dump
    chown -R skyline:skyline /opt/skyline/panorama
    chown -R skyline:skyline /opt/skyline/mirage
    chown -R skyline:skyline /opt/skyline/crucible
    chown -R skyline:skyline /opt/skyline/ionosphere
    chown -R skyline:skyline /opt/skyline/flux
    chown skyline:skyline /tmp/skyline

Skyline and dependencies install
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- git clone Skyline (git should have been installed in the `Running in Python
  virtualenv <running-in-python-virtualenv.html>`__ section) and it is
  recommended to then git checkout the commit reference of the latest stable
  release.

.. code-block:: bash

    mkdir -p /opt/skyline/github
    cd /opt/skyline/github
    git clone https://github.com/earthgecko/skyline.git
    # If you wish to switch to a specific commit or the latest release
    #cd /opt/skyline/github/skyline
    #git checkout <COMMITREF>

- Once again using the Python-3.8.10 virtualenv,  install the requirements using
  the virtualenv pip, this can take some time.

.. warning:: When working with virtualenv Python versions you must always
  remember to use the activate and deactivate commands to ensure you are using
  the correct version of Python.  Although running a virtualenv does not affect
  the system Python, not using activate can result in the user making errors
  that MAY affect the system Python and packages.  For example, a user does not
  use activate and just uses pip not bin/pip3 and pip installs some packages.
  User error can result in the system Python being affected.  Get in to the
  habit of always using explicit bin/pip3 and bin/python3 commands to ensure
  that it is harder for you to err.

.. code-block:: bash

    PYTHON_MAJOR_VERSION="3"
    PYTHON_VIRTUALENV_DIR="/opt/python_virtualenv"
    PROJECT="skyline-py3810"

    cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
    source bin/activate

    # As of statsmodels 0.9.0 numpy, et al need to be installed before
    # statsmodels in requirements
    # https://github.com/statsmodels/statsmodels/issues/4654
    bin/"pip${PYTHON_MAJOR_VERSION}" install $(cat /opt/skyline/github/skyline/requirements.txt | grep "^numpy\|^scipy\|^patsy" | tr '\n' ' ')
    bin/"pip${PYTHON_MAJOR_VERSION}" install $(cat /opt/skyline/github/skyline/requirements.txt | grep "^pandas")

    # This can take lots of minutes...
    bin/"pip${PYTHON_MAJOR_VERSION}" install -r /opt/skyline/github/skyline/requirements.txt

    deactivate


- Copy the ``skyline.conf`` and edit the ``USE_PYTHON`` as appropriate to your
  set up if it is not using PATH
  ``/opt/python_virtualenv/projects/skyline-py3810/bin/python3.8``

.. code-block:: bash

    cp /opt/skyline/github/skyline/etc/skyline.conf /etc/skyline/skyline.conf
    vi /etc/skyline/skyline.conf  # Set USE_PYTHON as appropriate to your setup

Apache reverse proxy
~~~~~~~~~~~~~~~~~~~~

- OPTIONAL but **recommended**, serving the Webapp via gunicorn with Apache or
  nginx reverse proxy.  Below highlights Apache but similar steps are required
  with nginx.

  - Setup Apache (httpd) and see the example configuration file in your cloned
    directory ``/opt/skyline/github/skyline/etc/skyline.httpd.conf.d.example``
    for nginx see ``/opt/skyline/github/skyline/etc/skyline.nginx.conf.d.example``
    modify all the ``<YOUR_`` variables as appropriate for you environment - see
    `Apache and gunicorn <webapp.html#apache-and-gunicorn>`__
  - Create a SSL certificate and update the SSL configurations in the Skyline
    Apache config (or your reverse proxy)

::

    SSLCertificateFile "<YOUR_PATH_TO_YOUR_CERTIFICATE_FILE>"
    SSLCertificateKeyFile "<YOUR_PATH_TO_YOUR_KEY_FILE>"
    SSLCertificateChainFile "<YOUR_PATH_TO_YOUR_CHAIN_FILE_IF_YOU_HAVE_ONE_OTHERWISE_COMMENT_THIS_LINE_OUT>"

- Update your reverse proxy config with the X-Forwarded-Proto header.

::

    RequestHeader set X-Forwarded-Proto "https"

- Add a user and password for HTTP authentication, the user does not have to
  be admin it can be anything, e.g.

.. code-block:: bash

    htpasswd -c /etc/httpd/conf.d/.skyline_htpasswd admin

.. note:: Ensure that the user and password for Apache match the user and
  password that you provide in `settings.py` for
  :mod:`settings.WEBAPP_AUTH_USER` and :mod:`settings.WEBAPP_AUTH_USER_PASSWORD`

- Deploy your Skyline Apache or nginx configuration file and restart httpd or
  nginx


Skyline database
~~~~~~~~~~~~~~~~

- Create the Skyline MySQL/MariaDB database for Panorama (see
  `Panorama <panorama.html>`__) and Ionosphere.

Skyline settings
~~~~~~~~~~~~~~~~

The Skyline settings are declared in the settings.py file as valid Python
variables which are used in code.  The settings values therefore need to be
defined correctly as the required Python types.  Strings, floats, ints, lists,
dicts and tuples are used in the various settings.  Examples of these Python
types are briefly outlined here to inform the user of the types.

.. code-block:: python

    a_string = 'single quoted string'  # str
    another_string = '127.0.0.1'  # str
    a_float = 0.1  # float
    an_int = 12345  # int
    a_list = [1.1, 1.4, 1.7]  # list
    another_list_of_strings = ['one', 'two', 'bob']  # list
    a_list_of_lists = [['server1.cpu.user', 23.6, 1563912300], ['server2.cpu.user', 3.22, 1563912300]]  # list
    a_dict = {'key': 'value'}  # dict
    a_nested_dict = {'server': {'name':'server1.cpu.user', 'value': 23.6, 'timestamp': 1563912300}}  # dict
    a_tuple = ('server1.cpu.user', 23.6, 1563912300)  # tuple
    a_tuple_of_tuples = (('server1.cpu.user', 23.6, 1563912300), ('server2.cpu.user', 3.22, 1563912300))  # tuple

There are a lot of settings in Skyline because it is highly configurable in many
different aspects and it has a lot of advanced features in terms of clustering,
other time series analysis capabilities and analysis methodologies.  This means
there is a lot of settings that will make no sense to the user.  The important
ones are labelled with ``[USER_DEFINED]`` in the settings.py

Required changes to settings.py follow.

- Edit the ``skyline/settings.py`` file and enter your appropriate settings,
  specifically ensure you set the following variables to the correct
  settings for your environment, see the documentation links and docstrings in
  the ``skyline/settings.py`` file for the full descriptions of each variable.
  Below are the variables you must set and are labelled in settings.py with
  ``[USER_DEFINED]``:

  - :mod:`settings.REDIS_SOCKET_PATH` if different from ```/tmp/redis.sock```
  - :mod:`settings.REDIS_PASSWORD`
  - :mod:`settings.GRAPHITE_HOST`
  - :mod:`settings.GRAPHITE_PROTOCOL`
  - :mod:`settings.GRAPHITE_PORT`
  - :mod:`settings.CARBON_PORT`
  - :mod:`settings.SERVER_METRICS_NAME`
  - :mod:`settings.SKYLINE_FEEDBACK_NAMESPACES` - An assumption is made that
    your Skyline and Graphite hosts will have feedback metrics but...
  - :mod:`settings.DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES` - While the
    assumption is true, please assess metrics in theses namespaces that you
    do not want to classified as feedback metrics. Any metrics that are not
    related to the running of Skyline or Graphite or a few that you do want
    to monitor, e.g. some KPI metrics like ``load15`` or ``disk.used_percent``
  - :mod:`settings.PAGERDUTY_OPTS` and :mod:`settings.SLACK_OPTS` if to be used,
    if so ensure that :mod:`settings.PAGERDUTY_ENABLED` and
    :mod:`settings.SLACK_ENABLED` are set to ``True`` as appropriate.
  - :mod:`settings.CANARY_METRIC`
  - :mod:`settings.ENABLE_MIRAGE`, it is recommended to set this to ``True`` the
    default is ``False`` simply for backwards compatibility.
  - :mod:`settings.ALERTS` - remember to only add a few key metrics to begin
    with.  If you want Skyline to start working almost immediately AND you
    have Graphite populated with more than 7 days of data, you can enable and
    start Mirage too and declare the SECOND_ORDER_RESOLUTION_HOURS in each
    ALERTS tuple as 168.
  - :mod:`settings.SMTP_OPTS`
  - :mod:`settings.SLACK_OPTS` - if you are going to use Slack
  - :mod:`settings.PAGERDUTY_OPTS` - if you are going to use PagerDuty
  - :mod:`settings.HORIZON_IP`
  - :mod:`settings.THUNDER_CHECKS` by default all thunder checks are done for
    the main analysis apps, if there are any apps you are not running disable
    the appropriate thunder checks.
  - :mod:`settings.THUNDER_OPTS` ensure you update these
  - :mod:`settings.MIRAGE_ENABLE_ALERTS` set this to ```True``` if you want to
    have Mirage running as described above.
  - If you are deploying with a Skyline MySQL Panorama DB straight away ensure
    that :mod:`settings.PANORAMA_ENABLED` is set to ``True`` and set all the
    other Panorama related variables as appropriate.  Enabling Panorama from the
    start is RECOMMENDED as it is integral to Ionosphere and Luminosity.
  - :mod:`settings.PANORAMA_DBHOST`
  - :mod:`settings.PANORAMA_DBUSERPASS`
  - :mod:`settings.MIRAGE_ENABLE_ALERTS` set this to ```True``` the default is
    ``False`` simply for backwards compatibility.
  - :mod:`BOUNDARY_SMTP_OPTS` although you will not start with running Boundary
    set the SMTP opts anyway.
  - :mod:`settings.BOUNDARY_PAGERDUTY_OPTS` - if you are going to use PagerDuty
  - :mod:`settings.BOUNDARY_SLACK_OPTS` - if you are going to use Slack
  - :mod:`settings.WEBAPP_AUTH_USER`
  - :mod:`settings.WEBAPP_AUTH_USER_PASSWORD`
  - :mod:`settings.SKYLINE_URL`
  - :mod:`settings.SERVER_PYTZ_TIMEZONE`
  - :mod:`settings.MEMCACHE_ENABLED`
  - :mod:`settings.FLUX_SELF_API_KEY` although you may not use flux, change this
    anyway.

.. code-block:: bash

    cd /opt/skyline/github/skyline/skyline
    cp settings.py settings.py.original.bak
    vi settings.py    # Probably better to open in your favourite editor

.. note:: a special settings variable that needs mentioning is the alerter
  :mod:`settings.SYSLOG_ENABLED`.  This variable by default is ``True`` and
  in this mode Skyline sends all anomalies to syslog and Panorama to record ALL
  anomalies to the database not just anomalies for metrics that have a
  :mod:`settings.ALERTS` tuple defined.  However this is the desired default
  state.  This setting basically enables the anomaly detection on everything
  with 3-sigma and builds the anomalies database, it is not noisy.  At this
  point in your implementation, the distinction between alerts and general
  Skyline anomaly detection and constructing an anomalies data set must once
  again be pointed out.

- TEST your settings!

.. code-block:: bash

    /opt/skyline/github/skyline/bin/test_settings.sh

- The above test is not yet 100% coverage but it covers the main settings.
- For later implementing and working with Ionosphere and setting up learning (see
  `Ionosphere <ionosphere.html>`__) after you have the other Skyline apps up and
  running.

- If you are **upgrading**, at this point return to the
  `Upgrading <upgrading/index.html>`__ or Release notes page.

Starting and testing the Skyline installation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Before you test Skyline by seeding Redis with some test data, ensure
  that you have configured the firewall/iptables/ip6tables with the appropriate
  restricted access.
- Start the Skyline apps

.. code-block:: bash

    /opt/skyline/github/skyline/bin/horizon.d start
    /opt/skyline/github/skyline/bin/analyzer.d start
    /opt/skyline/github/skyline/bin/mirage.d start
    /opt/skyline/github/skyline/bin/webapp.d start
    # And Panorama if you have set up in the DB at this stage
    /opt/skyline/github/skyline/bin/panorama.d start
    /opt/skyline/github/skyline/bin/ionosphere.d start
    /opt/skyline/github/skyline/bin/luminosity.d start

    # You can also start thunder - Skyline's internal monitoring but it may
    # fire a few alerts until you have some metrics being fed in, but that is
    # OK.
    /opt/skyline/github/skyline/bin/thunder.d start

- Check the log files to ensure things started OK and are running and there are
  no errors.

.. note:: When checking a log make sure you check the log for the appropriate
  time, Skyline can log fast, so short tails may miss some event you expect
  between the restart and tail.

.. code-block:: bash

    # Check what the logs reported when the apps started
    head -n 20 /var/log/skyline/*.log

    # How are they running
    tail -n 20 /var/log/skyline/*.log

    # Any errors - each app
    find /var/log/skyline -type f -name "*.log" | while read skyline_logfile
    do
      echo "#####
    # Checking for errors in $skyline_logfile"
      cat "$skyline_logfile" | grep -B2 -A10 -i "error ::\|traceback" | tail -n 60
      echo ""
      echo ""
    done

-  Seed Redis with some test data.

.. note:: if you are UPGRADING and you are using an already populated Redis
  store, you can skip seeding data.

.. note:: if you already have Graphite pickling data to Horizon seeding data
  will not work as Horizon/listen will already have a connection and will be
  reading the Graphite pickle.

.. code-block:: bash

    # NOTE: if Graphite carbon-relay is ALREADY sending data to Horizon, seed_data
    #       will most likely fail as Horizon/listen will already have a connection
    #       and will be reading the Graphite pickle.  If you wish to test seeding
    #       data, stop Graphite carbon-relay and restart Horizon, BEFORE running
    #       seed_data.py.  Run seed_data.py and then restart Horizon and start
    #       Graphite carbon-relay again after seed data has run.
    cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
    source bin/activate
    "bin/python${PYTHON_MAJOR_VERSION}" /opt/skyline/github/skyline/utils/seed_data.py
    deactivate

- Check the Skyline Webapp frontend on the Skyline machine's IP address and the
  appropriate port depending whether you are serving it proxied or direct, e.g
  ``https://YOUR_SKYLINE_IP``.  The ``horizon.test.pickle`` metric anomaly should
  be in the dashboard after the seed\_data.py is complete.  If Panorama is set
  up you will be able to see that in the /panorama view and in the
  :red:`re`:brow:`brow` view as well.
- This will ensure that the Horizon service is properly set up and can
  receive data. For real data, you have some options relating to
  getting a data pickle from Graphite see `Getting data into
  Skyline <getting-data-into-skyline.html>`__
- Check the log files again to ensure things are running and there are
  no errors.
- Once you have your :mod:`settings.ALERTS` configured to test them see
  `Alert testing <alert-testing.html>`__

Configure Graphite to send data to Skyline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Now you can configure your Graphite to pickle data to Skyline see
  `Getting data into Skyline <getting-data-into-skyline.html>`__
- If you have opted to not set up Panorama, later see set up
  `Panorama <panorama.html>`__

Other Skyline components
~~~~~~~~~~~~~~~~~~~~~~~~

- For Mirage set up see `Mirage <mirage.html>`__
- For Boundary set up see `Boundary <boundary.html>`__
- For more in-depth Ionosphere set up see `Ionosphere <ionosphere.html>`__
  however Ionosphere is only relevant once Skyline has at least
  :mod:`settings.FULL_DURATION` data in Redis.

Automation and configuration management notes
---------------------------------------------

The installation of packages in the ``requirements.txt`` can take a long time,
specifically the pandas build.  This will usually take longer than the default
timeouts in most configuration management.

That said, ``requirements.txt`` can be run in an idempotent manner, **however**
a few things need to be highlighted:

1. A first time execution of ``bin/"pip${PYTHON_MAJOR_VERSION}" install -r /opt/skyline/github/skyline/requirements.txt``
   will timeout on configuration management.  Therefore consider running this
   manually first.  Once pip has installed all the packages, the
   ``requirements.txt`` will run idempotent with no issue and be used to
   upgrade via a configuration management run when the ``requirements.txt`` is
   updated with any new versions of packages (with the possible exception of
   pandas).  It is obviously possible to provision each requirement individually
   directly in configuration management and not use pip to ``install -r`` the
   ``requirements.txt``, however remember that the virtualenv pip needs to be used
   and pandas needs a LONG timeout value, which not all package classes provide,
   if you use an exec of any sort, ensure the pandas install has a long timeout.
