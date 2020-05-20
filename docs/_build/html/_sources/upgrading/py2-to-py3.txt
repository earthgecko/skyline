======================
Upgrading - py2 to py3
======================

This section covers the steps required to upgrade from running Skyline on py2 to
py3.

If you are currently running Skyline on Python 2.7 ensure that you first ensure
that your Skyline is upgraded and running on v2.0.0 before switching over to
Python 3.

- After updating to at least v2.0.0, both DB and your settings.py and ensuring
  that v2.0.0 is running under v2.0.0 with your new settings.py
- Make a Python 3.7.x virtualenv with all pip requirements
- Stop all Skyline services
- Update /etc/skyline.conf with new virtualenv
- Start all Skyline services
