============
Requirements
============

The requirements are:

- A Graphite instance being populated with metric data from telegraf, sensu,
  prometheus remote_storage_adapter, et al or whatever your preferred metric
  collector is. Skyline use Graphite as its long term time series database.
- Linux (and probably any environment that supports Python virtualenv
  and bash)
- virtualenv
- Python >= 3.8.10 (running in an isolated vitualenv)
- Redis
- mariadb
- memcached (optional)

``requirements.txt``
####################

The ``requirements.txt`` file lists the required packages and the last
verified as working version with Python-3.8 within a virtaulenv.
