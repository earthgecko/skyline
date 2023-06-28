============
Requirements
============


The requirements are:

- The recommended minimum spec for the Skyline server is a 4 CPU / 8GB RAM
  instance. For a small metric population a single CPU would suffice.
- A server running CentOS Stream 8, Ubuntu 20.04 or Ubuntu 22.04.  Although
  CentOS Stream 8 is recommended because Skyline is developed, tested and run
  in production on CentOS Stream 8, only building is tested on Ubuntu.
- virtualenv
- Python 3.8.17 (running in an isolated virtualenv)
- Redis (or redis-stack-server)
- MariaDB
- memcached (optional)
- Additionally, a Graphite, Prometheus or VictoriaMetrics instance is required
  that is being populated with metric data from telegraf, sensu, prometheus
  remote_storage_adapter, et al or whatever your preferred metric collector is.
  Skyline use Graphite and/or VictoriaMetrics for long term time series
  database.  Graphite and/or VictoriaMetrics can run on the Skyline instance.
- A slack account, although optional, almost **all** of Skyline's messaging is
  done via slack, along with all the links and functionality to train metrics.
  A channel and bot in a free-tier slack account is sufficient.

If you using the quickstart install script, then Graphite will be deployed on
the server as well (if you do not disable the Graphite install) and you can opt
to install Prometheus (to send data to Flux) and/or VictoriaMetrics (to store
longer term data).

``requirements.txt``
####################

The ``requirements.txt`` file lists the required packages and the last
verified as working version with Python-3.8 within a virtualenv.
