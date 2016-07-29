======================================
Running Skyline in a Python virtualenv
======================================

Running Skyline in a Python virtualenv is the recommended way to run
Skyline. This allows for Skyline apps and all dependencies to be
isolated from the system Python version and packages and allows Skyline
be be run with a specific version of Python and as importantly specific
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

-  RedHat family

.. code-block:: bash

    yum -y install autoconf zlib-devel openssl-devel sqlite-devel bzip2-devel python-pip

-  Debian family

.. code-block:: bash

    apt-get -y install autoconf zlib1g-dev libssl-dev libsqlite3-dev lib64bz2-dev python-pip

virtualenv
~~~~~~~~~~

Regardless of your OS as long as you have pip installed you can install
virtualenv. *NOTE:* if you are using a version of Python virtualenv
already, this may not suit your setup.

virtualenv must be >= 15.0.1 due to some recent changes in the pip and
setuptools, see *Recent changes in the pip environment* section in
`Requirements <requirements.html#recent-changes-in-the-pip-environment>`__
for more details.

.. code-block:: bash

    pip install 'virtualenv>=15.0.1'

Python version
~~~~~~~~~~~~~~

Below we use the PATH /opt/python\_virtualenv, which you can substitute
with any path you choose and we are going to use python-2.7.12:

.. code-block:: bash

    PYTHON_VERSION="2.7.12"
    PYTHON_MAJOR_VERSION="2.7"
    PYTHON_VIRTUALENV_DIR="/opt/python_virtualenv"
    PR0JECT="skyline-py2712"

    mkdir -p "${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}"
    mkdir -p "${PYTHON_VIRTUALENV_DIR}/projects"

    cd "${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}"
    wget -q "https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz"
    tar -zxvf "Python-${PYTHON_VERSION}.tgz"

    cd ${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/Python-${PYTHON_VERSION}
    ./configure --prefix=${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}
    make
    make altinstall

You will now have a Python-2.7.12 environment with the Python
executable: ``/opt/python_virtualenv/versions/2.7.12/bin/python2.7``

Create a Skyline Python virtualenv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A word of warning relating to pip, setuptools and distribute if you have
opted not to use the above as you have Python virtualenv already. As of
virtualenv 15.0.1 the pip community adopted the new pkg\_resources
\_markerlib package structure, which means the following in the
virtualenv context:

-  distribute cannot be installed
-  pip must be >=8.1.0
-  setuptools must be >=20.2.2

Once again using Python-2.7.12:

.. code-block:: bash

    PYTHON_VERSION="2.7.12"
    PYTHON_MAJOR_VERSION="2.7"
    PYTHON_VIRTUALENV_DIR="/opt/python_virtualenv"
    PROJECT="skyline-py2712"

    cd "${PYTHON_VIRTUALENV_DIR}/projects"
    virtualenv --python="${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/bin/python${PYTHON_MAJOR_VERSION}" "$PROJECT"


Make sure to add the ``/etc/skyline/skyline.conf`` file - see
`Installation <installation.html>`__
