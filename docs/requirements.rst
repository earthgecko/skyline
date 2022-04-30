============
Requirements
============


The requirements are:

- The recommended minimum spec for the Skyline server is a 4 CPU / 8GB RAM
  instance. For a small metric population a single CPU would suffice.
- A server running CentOS Stream 8, Ubuntu 18.04 or Ubuntu 20.04.  Although
  CentOS Stream 8 is recommended because Skyline is developed, tested and run
  in production on CentOS Stream 8, only building is tested on Ubuntu.
- virtualenv
- Python >= 3.8.13 (running in an isolated virtualenv)
- Redis
- MariaDB
- memcached (optional)
- Additionally, a Graphite instance is required that is being populated with
  metric data from telegraf, sensu, prometheus remote_storage_adapter, et al or
  whatever your preferred metric collector is. Skyline use Graphite as its long
  term time series database.  Graphite can run on the Skyline instance.

If you using the quickstart install script, then Graphite will be deployed on
the server as well (if you do not disable the Graphite install).

``requirements.txt``
####################

The ``requirements.txt`` file lists the required packages and the last
verified as working version with Python-3.8 within a virtualenv.
