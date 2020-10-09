============
Requirements
============

The requirements are:

- First you need an instance of Graphite running
- Then you need some things feeding Graphite metrics (see statsd, sensu,
  telegraf, nc)
- Linux (and probably any environment that supports Python virtualenv
  and bash)
- virtualenv
- Python >= 3.8.3 (running in an isolated vitualenv)
- Redis
- MySQL/mariadb
- memcached (optional)

``requirements.txt``
####################

The ``requirements.txt`` file lists the required packages and the last
verified as working version with Python-3.8 within a virtaulenv.
