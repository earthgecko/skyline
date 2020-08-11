======================
Upgrading - py2 to py3
======================

This section covers the steps required to upgrade from running Skyline on py2 to
py3.

If you are currently running Skyline on Python 2.7 ensure that you first ensure
that your Skyline is upgraded and running on v1.3.1 before switching over to
Python 3.

Overview of the upgrade process
-------------------------------

- After updating to v1.3.1, both DB and your settings.py and ensuring that
  v1.3.1 is running under Python-2.7 with your new settings.py
- Create a Python 3.8.x virtualenv with all pip requirements see
  `Running Skyline in a Python virtualenv <running-in-python-virtualenv.html>`__
  and `Installation - Skyline and dependencies install <installation.html#skyline-and-dependencies-install>`_
  taking note that you will need to install the v2.0.0 dependencies e.g.
  https://raw.githubusercontent.com/earthgecko/skyline/v2.0.0/requirements.txt
- Backup your settings.py
- Update your Skyline code to v2.0.0
- Update your settings.py to include the new v2.0.0 settings
- Backup your DB
- Update your DB with updates/sql/v2.0.0.sql
- Update /etc/skyline.conf with the new Python 3.8.x virtualenv
- Stop all Skyline services
- Start all Skyline services

The Update notes on the release page describes the steps in detail, see
https://skyline-1.of-networks.co.uk/static/docs/releases/2_0_0.html#update-notes
