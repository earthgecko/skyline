=============================
Vista - BigQuery - virtualenv
=============================

Due to shared dependencies in existing packages that Skyline uses relating to
protobuf and Google auth, etc, to ensure that existing Skyline or Opentelemetry
functionality does not break by enabling BigQuery functionality, an isolated
Python virtualenv needs to be deployed.  The packages required for BigQuery
functionality are then installed in this isolated virtualenv and Skyline loads
those when BigQuery is used.

Because BigQuery functionality is optional, this is the simplest method by
which to achieve stability.

This section describes the deployment of a virtualenv for isolated BigQuery
related packages.

You will already have Skyline up and running and the Python version deployed at
this stage so to create the new virtualenv do the following.

Create the skyline-bq Python virtualenv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    PYTHON_VERSION="3.10.15"
    PYTHON_MAJOR_VERSION="3.10"
    PYTHON_VIRTUALENV_DIR="/opt/python_virtualenv"
    PROJECT="skyline-by-py31015"

    cd "${PYTHON_VIRTUALENV_DIR}/projects"
    virtualenv --python="${PYTHON_VIRTUALENV_DIR}/versions/${PYTHON_VERSION}/bin/python${PYTHON_MAJOR_VERSION}" "$PROJECT"

    # Create a symlink to the virtualenv so that it can be defined statically in
    # settings and when an upgrade to a new virtualenv is made the symlink is
    # simply replaced with a symlink to the new virtualenv and the variable
    # project path does not have to be declared in settings as change every time
    ln -sf "${PYTHON_VIRTUALENV_DIR}/projects/skyline-bq" "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"

    cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
    source bin/activate
    bin/pip3.10 install -r /opt/skyline/github/skyline/bq-requirements.txt
    deactivate
    cd


Now set :mod:`settings.VISTA_BQ_VIRTUALENV` to ``'/opt/python_virtualenv/projects/skyline-bq/lib/python3.10/site-packages'`` to the settings.py
