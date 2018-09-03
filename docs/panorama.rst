Panorama
========

The Panorama service is responsible for recording data for each anomaly.

It is important to remember that the Skyline analysis apps only alert on metrics
that have alert tuples set.  However if you have :mod:`settings.SYSLOG_ENABLED`
set to ``True`` syslog acts as an alerter, just like a SMTP, hipchat or
pagerduty alerter.  Therefore having this set to ``True`` means that Panorama
will record samples of **all** metrics that are flagged as anomalous.  Sampling
at :mod:`settings.PANORAMA_EXPIRY_TIME`, the default is 900 seconds.  Although
this may result in a lot of entries in the anomalies DB table, it is useful for
helping with root cause analysis.

New records are used to trigger further analysis of the metric population in
Luminosity to analyse the metric population for cross correlations.  However,
only metrics with a :mod:`settings.ALERTS` tuple trigger Luminosity, but all
metric anomalies are recorded.

The :mod:`settings.PANORAMA_CHECK_MAX_AGE` ensures that Panorama only processes
checks that are not older than this value.  This mitigates against Panorama
stampeding against the MySQL database, if either Panorama or MySQL were stopped
and there are a lot of Panorama check files queued to process.  If this is set
to 0, Panorama will process all checks, regardless of age.

There is a Panorama view in the Skyline Webapp frontend UI to allow you to
search and view historic anomalies.

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
  and create the skyline database (also create a user with permissions on the
  database, e.g.

```
mysql -u root -p < /opt/skyline/github/skyline/skyline/skyline.sql
mysql -u root -p -e "GRANT ALL ON skyline.* TO 'skyline'@'localhost' IDENTIFIED BY '$YOUR_MYSQL_SKYLINE_PASSWORD' \
FLUSH PRIVILEGES;"
```

- Enable Panorama and review the other Panorama settings in ``settings.py``
- Start Panorama (use your appropriate PATH) - or go back to `Installation`_ and
  continue with the installation steps and Panorama will be started later in the
  installation process.

.. code-block:: bash

    /opt/skyline/github/skyline/bin/panorama.d start

.. _Installation: ../html/installation.html
