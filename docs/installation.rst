============
Installation
============

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
- Apache

This installation document is specifically related to the required installs and
configurations of things that are directly related Skyline.  For notes regarding
automation and configuration management see the section at the end of this page.

``sudo``
~~~~~~~~

Use ``sudo`` appropriately for your environment wherever necessary.

Steps
-----

.. note:: All the documentation and testing is based on running Skyline in a
  Python-2.7.12 virtualenv, if you choose to deploy Skyline another way, you are
  on your own.  Although it is possible to run Skyline in a different type of
  environment, it does not lend itself to repeatability or a common known state.

-  Create a python-2.7.12 virtualenv for Skyline to run in see `Running in
   Python virtualenv <running-in-python-virtualenv.html>`__
-  Setup firewall rules to restrict access to the following:

  - :mod:`settings.WEBAPP_IP` - default is 127.0.0.1
  - :mod:`settings.WEBAPP_PORT` - default 1500
  - The IP address and port being used to reverse proxy the Webapp (if implementing) e.g. <YOUR_SERVER_IP_ADDRESS>:8080
  - The IP address and port being used by MySQL (if implementing)
  - The IP address and ports 2024 and 2025
  - The IP address and port being used by Redis

-  Install Redis - see `Redis.io <http://redis.io/>`__
-  Ensure Redis has socket enabled **with the following permissions** in your
   redis.conf

::

    unixsocket /tmp/redis.sock
    unixsocketperm 777

.. note:: The unixsocket on the apt redis-server package is
  ``/var/run/redis/redis.sock`` if you use this path ensure you change
  :mod:`settings.REDIS_SOCKET_PATH` to this path

-  Start Redis
-  Make the required directories

.. code-block:: bash

    mkdir /var/log/skyline
    mkdir /var/run/skyline
    mkdir /var/dump/

    mkdir -p /opt/skyline/panorama/check
    mkdir -p /opt/skyline/mirage/check
    mkdir -p /opt/skyline/crucible/check
    mkdir -p /opt/skyline/crucible/data
    mkdir /etc/skyline
    mkdir /tmp/skyline

- git clone Skyline (git should have been installed in the `Running in Python
  virtualenv <running-in-python-virtualenv.html>`__ section)

.. code-block:: bash

    mkdir -p /opt/skyline/github
    cd /opt/skyline/github
    git clone https://github.com/earthgecko/skyline.git

.. code-block:: bash

    mkdir -p /opt/skyline/github
    cd /opt/skyline/github
    git clone https://github.com/earthgecko/skyline.git

- Once again using the Python-2.7.12 virtualenv,  install the requirements using
  the virtualenv pip, this can take a long time, the pandas install takes quite
  a while.

.. code-block:: bash

    PYTHON_MAJOR_VERSION="2.7"
    PYTHON_VIRTUALENV_DIR="/opt/python_virtualenv"
    PROJECT="skyline-py2712"

    cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
    source bin/activate

    # Install the mysql-connector-python package first on its own as due to it
    # having to be downloaded and installed from MySQL, if it is not installed
    # an install -r will fail as pip cannot find mysql-connector-python
    bin/"pip${PYTHON_MAJOR_VERSION}" install http://cdn.mysql.com/Downloads/Connector-Python/mysql-connector-python-1.2.3.zip#md5=6d42998cfec6e85b902d4ffa5a35ce86

    # The MySQL download source can now be commented it out of requirements.txt
    cat /opt/skyline/github/skyline/requirements.txt | grep -v "cdn.mysql.com/Downloads" > /tmp/requirements.txt

    # This can take lots and lots of minutes...
    bin/"pip${PYTHON_MAJOR_VERSION}" install -r /tmp/requirements.txt

    # NOW wait at least 7 minutes (on a Linode 4 vCPU, 4GB RAM, SSD cloud node anyway)
    # and once completed, deactivate the virtualenv

    deactivate

- Copy the ``skyline.conf`` and edit the ``USE_PYTHON`` as appropriate to your
  setup if it is not using PATH
  ``/opt/python_virtualenv/projects/skyline-py2712/bin/python2.7``

.. code-block:: bash

    cp /opt/skyline/github/skyline/etc/skyline.conf /etc/skyline/skyline.conf
    vi /etc/skyline/skyline.conf # Set USE_PYTHON as appropriate to your setup

- OPTIONAL but **recommended**, serving the Webapp via gunicorn with an Apache
  reverse proxy.

  - Setup Apache (httpd) and see the example configuration file in your cloned
    directory ``/opt/skyline/github/skyline/etc/skyline.httpd.conf.d.example``
    modify all the ``<YOUR_`` variables as appropriate for you environment - see
    `Apache and gunicorn <webapp.html#apache-and-gunicorn>`__
  - Add a user and password for HTTP authentication, e.g.

.. code-block:: bash

    htpasswd -c /etc/httpd/conf.d/.skyline_htpasswd admin

.. note:: Ensure that the user and password for Apache match the user and
  password that you provide in `settings.py` for
  :mod:`settings.WEBAPP_AUTH_USER` and :mod:`settings.WEBAPP_AUTH_USER_PASSWORD`

- Deploy your Skyline Apache configuration file and restart httpd.
- Create the Skyline MySQL database for Panorama (see
  `Panorama <panorama.html>`__).  Although this is optional, it is
  **recommended** as Panorama does give you a full historical view of all the
  triggered anomalies, which is very useful for providing insight into what, how
  and when metrics are triggering as anomalous.
- Edit the ``settings.py`` file and enter your appropriate settings,
  specifically ensure you set the following variables to the correct
  setting for your environment, see the documentation links and docstrings in
  the `settings.py` file forthe full descriptions of each variable:

  - :mod:`settings.GRAPHITE_HOST`
  - :mod:`settings.GRAPHITE_PROTOCOL`
  - :mod:`settings.GRAPHITE_PORT`
  - :mod:`settings.SERVER_METRICS_NAME`
  - :mod:`settings.CANARY_METRIC`
  - :mod:`settings.ALERTS`
  - :mod:`settings.SMTP_OPTS`
  - :mod:`settings.HIPCHAT_OPTS` and :mod:`settings.PAGERDUTY_OPTS` if to be
    used,  if so ensure that :mod:`settings.HIPCHAT_ENABLED` and
    :mod:`settings.PAGERDUTY_ENABLED` are set to ``True``
  - If you are deploying with a Skyline MySQL Panorama DB straight away ensure
    that :mod:`settings.PANORAMA_ENABLED` is set to ``True`` and set all the
    other Panorama related variables as appropriate.
  - :mod:`settings.WEBAPP_AUTH_USER`
  - :mod:`settings.WEBAPP_AUTH_USER_PASSWORD`
  - :mod:`settings.WEBAPP_ALLOWED_IPS`

.. code-block:: bash

    cd /opt/skyline/github/skyline/skyline
    vi settings.py

- If you are **upgrading**, at this point return to the
  `Upgrading <upgrading.html>`__ page.
- Before you test Skyline by seeding Redis with some test data, ensure
  that you have configured the firewall/iptables with the appropriate restricted
  access.
- Start the Skyline apps

.. code-block:: bash

    /opt/skyline/github/skyline/bin/horizon.d start
    /opt/skyline/github/skyline/bin/analyzer.d start
    /opt/skyline/github/skyline/bin/webapp.d start
    # And Panorama if you have setup in the DB at this stage
    /opt/skyline/github/skyline/bin/panorama.d start

-  Check the log files to ensure things are running and there are no
   errors.

.. code-block:: bash

    tail /var/log/skyline/*.log

-  Seed Redis with some test data

.. code-block:: bash

    cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
    source bin/activate
    bin/python2.7 /opt/skyline/github/skyline/utils/seed_data.py
    deactivate

- Check the Skyline Webapp frontend on the Skyline machine's IP address and the
  appropriate port depending whether you are serving it proxied or direct, e.g
  ``http://YOUR_SKYLINE_IP:8080`` or ``http://YOUR_SKYLINE_IP:1500``.  The
  ``horizon.test.udp`` metric anomaly should be in the dashboard after the
  seed\_data.py is complete.  If Panorama is set up you will be able to see that
  in the /panorama view and in the :red:`re`:brow:`brow` view as well.

- Check the log files again to ensure things are running and there are
  no errors.

- This will ensure that the Horizon service is properly set up and can
  receive data. For real data, you have some options relating to
  getting a data pickle from Graphite see `Getting data into
  Skyline <getting-data-into-skyline.html>`__

- Once you have your ALERTS configured to test them see
  `Alert testing <alert-testing.html>`__

- If you have opted to not setup Panorama, later see setup
  `Panorama <panorama.html>`__

- For Mirage setup see `Mirage <mirage.html>`__

- For Boundary setup see `Boundary <boundary.html>`__

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

2. The mysql-connector-python package is pulled directly from MySQL as no pip
   version exists.  Therefore during the build process it is recommended to pip
   install the MySQL source package first and then the line out comment in
   ``requirements.txt``.  The ``mysql-connector-python==1.2.3`` line then ensures
   the dependency is fulfilled.
