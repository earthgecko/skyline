Panorama
========

The Panorama service is responsible for recording the metadata for each anomaly.

It is important to remember that the Skyline analysis apps only alert on metrics
that have alert tuples set.  Panorama records samples of all metrics that are
flagged as anomalous.  Sampling at :mod:`settings.PANORAMA_EXPIRY_TIME`, the
default is 900 seconds.

The :mod:`settings.PANORAMA_CHECK_MAX_AGE` ensures that Panorama only processes
checks that are not older than this value.  This mitigates against Panorama
stampeding against the MySQL database, if either Panorama or MySQL were stopped
and there are a lot of Panorama check files queued to process.  If this is set
to 0, Panorama will process all checks, regardless of age.

There is a Panorama view in the Skyline Webapp frontend UI to allow you to
search and view historical anomalies.

Create a MySQL database
-----------------------

You can install and run MySQL on the Skyline server or use any existing MySQL
server to create the database on.  The Skyline server just has to be able to
access the database with the user and password you configure in ``settings.py``
:mod:`settings.PANORAMA_DBUSER` and :mod:`settings.PANORAMA_DBUSERPASS`

.. note:: It is recommended, if possible that MySQL is configured to use a single
  file per InnoDB table with the MySQL config option - ``innodb_file_per_table=1``
  This is due to the fact that the anomalies and Ionosphere related MySQL tables
  are InnoDB tables and all the other core Skyline DB tables are MyISAM.
  If you are adding the Skyline DB to an existing MySQL database server please
  consider the ramifications to your setup.  It is not a requirement, just a
  tidier and more efficient way to run MySQL InnoDB tables in terms of
  managing space allocations with InnoDB and it segregates databases from each
  other in the context on the .ibd file spaces, making for easier management of
  each individual database in terms of ibd file space.  However that was really
  only an additional caution.  Retrospectively, it is unlikely that the
  anomalies table will ever really be a major problem in terms of the page space
  requirements any time soon.  It appears and it is hoped anyway, time and
  really big data sets may invalidate this in the future, Gaia DR1 MySQL say :)

- See ``skyline.sql`` in your cloned Skyline repo for the schema creation script
- Enable Panorama and set the other Panorama settings in ``settings.py``
- Start Panorama (use you appropriate PATH) - or go back to `Installation`_ and
  continue with the installation steps and Panorama will be started later in the
  installation process.

.. code-block:: bash

    /opt/skyline/github/skyline/bin/panorama.d start

.. _Installation: ../html/installation.html
