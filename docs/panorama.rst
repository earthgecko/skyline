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
helping with root cause analysis and correlations.

New records are used to trigger further analysis of the metric population in
Luminosity to analyse the metric population for cross correlations.  However,
only metrics with a :mod:`settings.ALERTS` tuple trigger Luminosity, but all
metric anomalies are recorded.

The :mod:`settings.PANORAMA_CHECK_MAX_AGE` ensures that Panorama only processes
checks that are not older than this value.  This mitigates against Panorama
stampeding against the MySQL database, if either Panorama or MariaDB/MySQL were
stopped and there are a lot of Panorama check files queued to process.  If this
is set to 0, Panorama will process all checks, regardless of age.

There is a Panorama view in the Skyline Webapp frontend UI to allow you to
search and view historic anomalies.

The skyline MariaDB/MySQL database
----------------------------------

You can install and run MariaDB/MySQL on the Skyline server itself or use any
existing MariaDB/MySQL server to create the database on or use a Galera cluster.
The Skyline server just has to be able to access the database with the user and
password you configure in  ``settings.py`` :mod:`settings.PANORAMA_DBUSER` and
:mod:`settings.PANORAMA_DBUSERPASS`

Skyline has moved to MariaDB and although MySQL and MariaDB should be
interoperable, MariaDB has recently departed from full MySQL compatibility so
it is recommended that you use the official MariaDB-server rather than MySQL if
possible.  Going forward Skyline will only be developed and tested against
MariaDB.

InnoDB tables
^^^^^^^^^^^^^

It is recommended, if possible that MariaDB/MySQL is configured to use a single
file per InnoDB table with the MariaDB/MySQL config option -
``innodb_file_per_table=1``.  This is due to the fact that the anomalies and
Ionosphere related MariaDB/MySQL tables are InnoDB tables and all the other
core Skyline DB tables are MyISAM.

The MyISAM storage engine is used for the metadata type tables because it is
a simpler structure and faster for data which is often queried and FULL TEXT
searching.

The InnoDB storage engine is used for the anomaly table - mostly writes.
z_fp_ tables are InnoDB tables and each metric that has an Ionosphere features
profile created (e.g. has been trained) has a z_fp_<metric_id> and a
z_ts_<metric_id> table created, therefore if you have 10000 metrics and you
created features profiles (trained) for each one, there would be 20000 tables.
Bear in mind, that not all metrics will have features profiles created as you
have to manually train to create each one.

If you are adding the Skyline database to an existing MariaDB/MySQL database
server please consider the ramifications to your set up.  It is not a
requirement, just a tidier and more efficient way to run MariaDB/MySQL InnoDB
tables in terms of managing space allocations with InnoDB and it segregates
databases from each other in the context on the .ibd file spaces, making for
easier management of each individual database in terms of ibd file space.
However that was really only an additional caution.  Retrospectively, it is
unlikely that the anomalies table will ever really be a major problem in terms
of the page space requirements any time soon.

MyISAM and InnoDB or InnoDB only
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The default Skyline database schema is designed for optimum performance
through the use of MyISAM and InnoDB tables where appropriate for the data and
table usage.  However the Skyline database can be run with InnoDB only tables.
The small performance loss that may result from not using the MyISAM tables
where appropriate is gained in the ability to run the database on a MariaDB
Galera cluster to achieve high availability and resilience on the database.
If you wish to use InnoDB tables only, see the lines to uncomment in the snippet
below.

ID offsets
^^^^^^^^^^
It is assumed that the database is not running with an offset and the database
increments ids by 1.  If you are running an offset please review all SQL updates
to determine if you need to change anything in the SQL when you apply Skyline
SQL updates.  Although it is highly unlikely to ever need to be considered.

Create the skyline MariaDB/MySQL database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- See ``skyline.sql`` in your cloned Skyline repo for the schema creation script
  and create the skyline database (also create a user with permissions on the
  database, e.g.

.. code-block:: bash

    # IF YOU WISH TO USE INNODB TABLES ONLY UNCOMMENT THE FOLLOWING LINES
    # For Galera clustering
    #cp /opt/skyline/github/skyline/skyline/skyline.sql /opt/skyline/github/skyline/skyline/skyline.myisam.sql
    #cat /opt/skyline/github/skyline/skyline/skyline.myisam.sql | sed -e 's/MyISAM/InnoDB/g' > /opt/skyline/github/skyline/skyline/skyline.sql

    # Note if you are using MariaDB on the database host, the password is longer
    # required for the root user.  If you are using MySQL use password as
    # appropriate
    mysql -u root < /opt/skyline/github/skyline/skyline/skyline.sql
    # Example permissions, change localhost to as appropriate for your set up
    mysql -u root -e "GRANT ALL ON skyline.* TO 'skyline'@'localhost' IDENTIFIED BY '$YOUR_MYSQL_SKYLINE_PASSWORD' \
    FLUSH PRIVILEGES;"

- Enable Panorama and review the other Panorama settings in ``settings.py``
- Start Panorama (use your appropriate PATH) - or go back to `Installation`_ and
  continue with the installation steps and Panorama will be started later in the
  installation process.

.. code-block:: bash

    /opt/skyline/github/skyline/bin/panorama.d start

.. _Installation: ../html/installation.html
