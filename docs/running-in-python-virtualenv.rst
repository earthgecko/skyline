======================================
Running Skyline in a Python virtualenv
======================================

Running Skyline in a Python virtualenv is the recommended way to run
Skyline. This allows for Skyline apps and all dependencies to be
isolated from the system Python version and packages and allows Skyline
to be run with a specific version of Python and as importantly specific
versions of the dependencies packages.

The possible overhead of adding Python virtualenv functionality to any
configuration management is probably worth the effort in the long run.

``sudo``
~~~~~~~~

Use ``sudo`` appropriately for your environment wherever necessary.

HOWTO Python virtualenv Skyline
===============================

Dependencies
~~~~~~~~~~~~

Building Python versions from the Python sources in Python virtualenv
requires the following system dependencies:

- RedHat family - only CentOS 8 is tested, for el6 and el7 deps see `Running in
  a Python 2.7 virtualenv <running-in-python-virtualenv-py2.html>`__ .

.. code-block:: bash

    yum -y install epel-release
    # CentOS 8 only tested
    yum -y install autoconf zlib-devel openssl-devel sqlite-devel bzip2-devel \
      gcc gcc-c++ readline-devel ncurses-devel gdbm-devel freetype-devel \
      libpng-devel python38 wget tar git xz-devel

- Debian family - only tested on Ubuntu 16.04 and 18.04

.. code-block:: bash

    apt-get -y install build-essential
    apt-get -y install autoconf zlib1g-dev libssl-dev libsqlite3-dev libbz2-dev \
      libreadline6-dev libgdbm-dev libncurses5 libncurses5-dev libncursesw5 \
      libfreetype6-dev libxft-dev python-pip wget tar git

virtualenv
~~~~~~~~~~

Regardless of your OS as long as you have pip installed you can install
virtualenv. *NOTE:* if you are using a version of Python virtualenv
already, this may not suit your set up.

This is using your **system** pip at this point only to install virtualenv.

.. code-block:: bash

    # CentOS 8
    pip3 install --user virtualenv==16.7.9

    # Ubuntu 16.04 and 18.04
    pip install virtualenv==16.7.9

Python version
~~~~~~~~~~~~~~

Below we use the path ``/opt/python_virtualenv``, which you can substitute
with any path you choose.  We are going to use the Python-3.8.10 source and
build and install an isolated Python-3.8.10, this has no effect on your system
Python:

.. code-block:: bash

    PYTHON_VERSION="3.8.10"
    PYTHON_MAJOR_VERSION="3.8"
    PYTHON_VIRTUALENV_DIR="/opt/python_virtualenv"

    mkdir -p "${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}"
    mkdir -p "${PYTHON_VIRTUALENV_DIR}/projects"

    cd "${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}"
    wget -q "https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz"
    tar -zxvf "Python-${PYTHON_VERSION}.tgz"

    cd ${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/Python-${PYTHON_VERSION}

    ./configure --prefix=${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}

    make

    # Optionally here if you have the time or interest you can run
    # make test

    make altinstall


You will now have a Python-3.8.10 environment with the Python
executable: ``/opt/python_virtualenv/versions/3.8.10/bin/python3.8``

Create a Skyline Python virtualenv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once again using Python-3.8.10:

.. code-block:: bash

    PYTHON_VERSION="3.8.10"
    PYTHON_MAJOR_VERSION="3.8"
    PYTHON_VIRTUALENV_DIR="/opt/python_virtualenv"
    PROJECT="skyline-py3810"

    cd "${PYTHON_VIRTUALENV_DIR}/projects"
    virtualenv --python="${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/bin/python${PYTHON_MAJOR_VERSION}" "$PROJECT"


Make sure to add the ``/etc/skyline/skyline.conf`` file with the Python - see
`Installation <installation.html>`__
