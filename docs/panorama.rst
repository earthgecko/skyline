Panorama
========

The Panorama service is responsible for recording the metadata for each anomaly.

It is important to remember that the Skyline analysis apps only alert on metrics
that have alert tuples set.  Panorama records samples of all metrics that are
flagged as anomalous.  Sampling at :mod:`settings.PANORAMA_EXPIRY_TIME`, the
default is 900 seconds.

There is a Panorama view in the Skyline Webapp frontend UI to allow you to
search and view historical anomalies.

Create a MySQL database
-----------------------

You can install and run MySQL on the Skyline server or use any existing MySQL
server to create the database on.  The Skyline server just has to be able to
access the database with the user and password you configure in ``settings.py``
:mod:`settings.PANORAMA_DBUSER` and :mod:`settings.PANORAMA_DBUSERPASS`

- See ``skyline.sql`` in your cloned Skyline repo for the schema creation script
- Enable Panorama and set the other Panorama settings in ``settings.py``
- Start Panorama (use you appropriate PATH) - or go back to `Installation`_ and
  continue with the installation steps and Panorama will be started later in the
  installation process.

.. code-block:: bash

    /opt/skyline/github/skyline/bin/panorama.d start

.. _Installation: ../html/installation.html
