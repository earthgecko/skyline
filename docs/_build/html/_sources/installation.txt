============
Installation
============

This is going to take some time.

Intended audience
-----------------

Skyline is not really a ``localhost`` application, it needs lots of data, unless
you have a ``localhost`` Graphite or pickle Graphite to your localhost.

Given the specific nature of Skyline, it is assumed that the audience will have
a certain level of technical knowledge, e.g. it is assumed that the user will be
familiar with the installation, configuration, operation and security practices
and considerations relating to the following components:

- Graphite
- Redis
- MySQL
- Apache (or nginx although there are no examples here)
- memcached

However, these types of assumptions even if stated are not useful or helpful to
someone not familiar with all the ins and outs of some required thing. This
installation document is specifically related to the required installs and
configurations of things that are directly related Skyline.  Although it cannot
possibly cover all possible set ups or scenarios, it does describe
recommendations in terms of configurations of the various components and how and
where they should be run in relation to Skyline.  There are no cfengine, puppet,
chef, ansible or docker patterns here.

The documentation is aimed at installing Skyline securely by default.  It is
possible to run Skyline very insecurely, however this documentation does not
specify how to do that.  Both the set up and documentation are verbose.  Setting
up Skyline takes a while.

Skyline's default settings and documentation are aimed to run behind a SSL
terminated and authenticated reverse proxy and use Redis authentication.  This
makes the installation process more tedious, but it means that all these
inconvenient factors are not left as an after thought or added to some TODO list
or issue when you decide after trying Skyline, "Yes! I want to Skyline, this is
cool".

For notes regarding automation and configuration management see the section at
the end of this page.

What the components do
----------------------

- Graphite - sends metric data to Skyline Horizon via a pickle.  Graphite is a
  separate application that will probably be running on another server, although
  Graphite could run on the same server, in a production environment it would
  probably be a remote machine or container.  Graphite is not part of Skyline.
- Redis - stores mod:`settings.FULL_DURATION` seconds (usefully 24 hours worth)
  of time series data.  Redis must run on the same host as Skyline.  It may be
  possible to run Redis in another container or VM that was on the same host,
  but this has not been tested.
- MySQL - stores data about anomalies and time series features fingerprints for
  learning things that are not anomalous.  MySQL ideally should run on the same
  host as Skyline or in another container or VM that was on the same host,
  but this has not been tested.
- Apache (or nginx) - serves the Skyline webapp via gunicorn and handles SSL
  termination and basic http auth.  Ideally should run on the same host as
  Skyline.
- memcached - caches Ionosphere MySQL data, must run on the same host as Skyline
  or perhaps in another container or VM that was on the same host, but this has
  not been tested.

``sudo``
~~~~~~~~

Use ``sudo`` appropriately for your environment wherever necessary.

Steps
-----

.. note:: All the documentation and testing is based on running Skyline in a
  Python-2.7.14 virtualenv, if you choose to deploy Skyline another way, you are
  on your own.  Although it is possible to run Skyline in a different type of
  environment, it does not lend itself to repeatability or a common known state.

- Create a python-2.7.14 virtualenv for Skyline to run in see `Running in
  Python virtualenv <running-in-python-virtualenv.html>`__
- Please set up all the firewall rules to restrict access to the following
  **before** you continue to install the other components:

  - The IP address and port being used to reverse proxy the Webapp e.g.
    <YOUR_SERVER_IP_ADDRESS>:443, ensure that this is only accessible to
    specified IPs in iptables/ip6tables (further these addresses should also be
    added to the reverse proxy conf as ``Allow from`` defines when you create
    the reverse proxy conf file).
  - The IP address and port being used by MySQL, if you are not binding MySQL to
    127.0.0.1 only, ensure that the MySQL port declared in
    :mod:`settings.PANORAMA_DBPORT` (default 3306) is only accessible to
    specified IPs in iptables/ip6tables
  - Allow the IP address of your Graphite server/s on ports 2024 and 2025 (the
    default Graphite pickle ports)
  - The IP address and port being used by Redis, which if you are not running
    multiple distributed Skyline instances should be 127.0.0.1
  - Please ensure you handle all of these with iptables AND ip6tables (or the
    equivalent) before continuing.

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
  ``bind 127.0.0.1`` or ``bind 127.0.0.1 <OTHER_IP_YOU_WANT_REDIS_TO_BIND_TO>``
  if you are going to run multiple distributed  Skyline instances.
- Start Redis
- Install memcached and start memcached see `memcached.org <https://memcached.org/>`__
- Ensure that you start memcached only bound to 127.0.0.1 by passing the daemon
  the option ``-l 127.0.0.1``, Skyline only requires memcached locally.
- Make the required directories

.. code-block:: bash

    mkdir /var/log/skyline
    mkdir /var/run/skyline
    mkdir /var/dump

    mkdir -p /opt/skyline/panorama/check
    mkdir -p /opt/skyline/mirage/check
    mkdir -p /opt/skyline/crucible/check
    mkdir -p /opt/skyline/crucible/data
    mkdir -p /opt/skyline/ionosphere
    mkdir /etc/skyline
    mkdir /tmp/skyline

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

- Once again using the Python-2.7.14 virtualenv,  install the requirements using
  the virtualenv pip, this can take some time.

.. warning:: When working with virtualenv Python versions you must always
  remember to use the activate and deactivate commands to ensure you are using
  the correct version of Python.  Although running a virtualenv does not affect
  the system Python, not using activate can result in the user making errors
  that MAY affect the system Python and packages.  For example, a user does not
  use activate and just uses pip not bin/pip2.7 and pip installs some packages.
  User error can result in the system Python being affected.  Get in to the
  habit of always using explicit bin/pip2.7 and bin/python2.7 commands to ensure
  that it is harder for you to err.

.. code-block:: bash

    PYTHON_MAJOR_VERSION="2.7"
    PYTHON_VIRTUALENV_DIR="/opt/python_virtualenv"
    PROJECT="skyline-py2714"

    cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
    source bin/activate

    # This can take lots of minutes...
    bin/"pip${PYTHON_MAJOR_VERSION}" install -r /opt/skyline/github/skyline/requirements.txt

    deactivate

- Copy the ``skyline.conf`` and edit the ``USE_PYTHON`` as appropriate to your
  set up if it is not using PATH
  ``/opt/python_virtualenv/projects/skyline-py2714/bin/python2.7``

.. code-block:: bash

    cp /opt/skyline/github/skyline/etc/skyline.conf /etc/skyline/skyline.conf
    vi /etc/skyline/skyline.conf  # Set USE_PYTHON as appropriate to your setup

- OPTIONAL but **recommended**, serving the Webapp via gunicorn with an Apache
  reverse proxy.

  - Setup Apache (httpd) and see the example configuration file in your cloned
    directory ``/opt/skyline/github/skyline/etc/skyline.httpd.conf.d.example``
    modify all the ``<YOUR_`` variables as appropriate for you environment - see
    `Apache and gunicorn <webapp.html#apache-and-gunicorn>`__
  - Create a SSL certificate and update the SSL configurations in the Skyline
    Apache config (or your reverse proxy)

::

    SSLCertificateFile "<YOUR_PATH_TO_YOUR_CERTIFICATE_FILE>"
    SSLCertificateKeyFile "<YOUR_PATH_TO_YOUR_KEY_FILE>"
    SSLCertificateChainFile "<YOUR_PATH_TO_YOUR_CHAIN_FILE_IF_YOU_HAVE_ONE_OTHERWISE_COMMENT_THIS_LINE_OUT>"

- Update your Apache (or reverse proxy config) with the X-Forwarded-Proto header.

::

    RequestHeader set X-Forwarded-Proto "https"

- Add a user and password for HTTP authentication, the user does not have to
  be admin it can be anything, e.g.

.. code-block:: bash

    htpasswd -c /etc/httpd/conf.d/.skyline_htpasswd admin

.. note:: Ensure that the user and password for Apache match the user and
  password that you provide in `settings.py` for
  :mod:`settings.WEBAPP_AUTH_USER` and :mod:`settings.WEBAPP_AUTH_USER_PASSWORD`

- Deploy your Skyline Apache configuration file and restart httpd.
- Create the Skyline MySQL database for Panorama (see
  `Panorama <panorama.html>`__) and Ionosphere.
- Edit the ``settings.py`` file and enter your appropriate settings,
  specifically ensure you set the following variables to the correct
  settings for your environment, see the documentation links and docstrings in
  the `settings.py` file for the full descriptions of each variable.  Below are
  the variables you must set:

  - :mod:`settings.REDIS_SOCKET_PATH` if different from ```/tmp/redis.sock```
  - :mod:`settings.REDIS_PASSWORD`
  - :mod:`settings.GRAPHITE_HOST`
  - :mod:`settings.GRAPHITE_PROTOCOL`
  - :mod:`settings.GRAPHITE_PORT`
  - :mod:`settings.CARBON_PORT`
  - :mod:`settings.SERVER_METRICS_NAME`
  - :mod:`settings.CANARY_METRIC`
  - :mod:`settings.ALERTS` - remember to only add a few key metrics to begin
    with.  If you want Skyline to start working almost immediately AND you
    have Graphite populated with more than 7 days of data, you can enable and
    start Mirage too and declare the SECOND_ORDER_RESOLUTION_HOURS in each
    ALERTS tuple as 168.
  - :mod:`settings.MIRAGE_ENABLE_ALERTS` set this to ```True``` if you want to
    have Mirage running as described above.
  - :mod:`settings.SMTP_OPTS`
  - :mod:`settings.HIPCHAT_OPTS` and :mod:`settings.PAGERDUTY_OPTS` if to be
    used,  if so ensure that :mod:`settings.HIPCHAT_ENABLED` and
    :mod:`settings.PAGERDUTY_ENABLED` are set to ``True``
  - :mod:`settings.HORIZON_IP`
  - If you are deploying with a Skyline MySQL Panorama DB straight away ensure
    that :mod:`settings.PANORAMA_ENABLED` is set to ``True`` and set all the
    other Panorama related variables as appropriate.  Enabling Panorama from the
    start is RECOMMENDED as it is integral to Ionosphere and Luminosity.
  - :mod:`settings.WEBAPP_AUTH_USER`
  - :mod:`settings.WEBAPP_AUTH_USER_PASSWORD`
  - :mod:`settings.WEBAPP_ALLOWED_IPS`
  - :mod:`settings.SKYLINE_URL`
  - :mod:`settings.SERVER_PYTZ_TIMEZONE`
  - :mod:`settings.MEMCACHE_ENABLED`

.. code-block:: bash

    cd /opt/skyline/github/skyline/skyline
    vi settings.py

- For later implementing and working with Ionosphere and setting up learning (see
  `Ionosphere <ionosphere.html>`__) after you have the other Skyline apps up and
  running.
- If you are **upgrading**, at this point return to the
  `Upgrading <upgrading/index.html>`__ page.
- Before you test Skyline by seeding Redis with some test data, ensure
  that you have configured the firewall/iptables/ip6tables with the appropriate
  restricted access.
- Start the Skyline apps

.. code-block:: bash

    /opt/skyline/github/skyline/bin/horizon.d start
    /opt/skyline/github/skyline/bin/analyzer.d start
    /opt/skyline/github/skyline/bin/webapp.d start
    # And Panorama if you have set up in the DB at this stage
    /opt/skyline/github/skyline/bin/panorama.d start
    /opt/skyline/github/skyline/bin/ionosphere.d start
    /opt/skyline/github/skyline/bin/luminosity.d start

- Check the log files to ensure things started OK and are running and there are
  no errors.

.. note:: When checking a log make sure you check the log for the appropriate
  time, Skyline can log lots fast, so short tails may miss some event you
  expect between that restart and tail.

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

.. code-block:: bash

    cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
    source bin/activate
    bin/python2.7 /opt/skyline/github/skyline/utils/seed_data.py
    deactivate

- Check the Skyline Webapp frontend on the Skyline machine's IP address and the
  appropriate port depending whether you are serving it proxied or direct, e.g
  ``https://YOUR_SKYLINE_IP``.  The ``horizon.test.udp`` metric anomaly should
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
- Now you can configure your Graphite to pickle data to Skyline see
  `Getting data into Skyline <getting-data-into-skyline.html>`__
- If you have opted to not set up Panorama, later see set up
  `Panorama <panorama.html>`__
- For Mirage set up see `Mirage <mirage.html>`__
- For Boundary set up see `Boundary <boundary.html>`__
- For Ionosphere set up see `Ionosphere <ionosphere.html>`__

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
   ``requirements.txt``, however remember the the virtualenv pip needs to be used
   and pandas needs a LONG timeout value, which not all package classes provide,
   if you use an exec of any sort, ensure the pandas install has a long timeout.
