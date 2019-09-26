*************************
Development - dawn-docker
*************************

Build script - utils/dawn/skyline.docker.dawn.sh
================================================

The Skyline dawn docker build script is for **TESTING** only, it is not
recommended for production use and it is not meant for localhost (unless you
have a CentOS 7.6 VM running on you localhost).  If you want to run Skyline on
docker on your localhost see the `Docker <docker.html>`__ page.

This script installs and sets up the required components to run and test Skyline
running in docker containers on CentOS 7.6.

IMPORTANT NOTES
~~~~~~~~~~~~~~~

- The script is IPv4 ONLY.  It DISABLES IPv6 is on the system.
- This script manages the basic requirements on firewall-cmd and firewalld
  it allows public access to port 22, 80 and 443 and restricted access to port
  8888 for Graphite.

USER DEFINED VARIABLES
~~~~~~~~~~~~~~~~~~~~~~

Please populate these variables as appropriate with the values of YOUR set up
and write them to /etc/skyline/skyline.dawn.conf to be sourced by the build
script.

.. code-block:: bash

  YOUR_SERVER_IP_ADDRESS="x.x.x.x"  # YOUR Skyline server public IPv4 address
  YOUR_OTHER_IP_ADDRESS="x.x.x.x"   # YOUR current public IPv4 address that you will be connecting from
  SKYLINE_RELEASE="v1.2.18"         # The Skyline release to deploy

Deploying Skyline on CentOS 7.6 using skyline.docker.dawn.sh
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use sudo as appropriate.

.. code-block:: bash

  # Create the skyline.dawn.conf files and add the USER DEFINED VARIABLES
  vi /etc/skyline/skyline.dawn.conf

  # Fetch the build script
  yum -y install wget
  wget -O /tmp/skyline.docker.dawn.sh https://raw.githubusercontent.com/earthgecko/skyline/master/utils/dawn/skyline.docker.dawn.sh

  # Always review scripts before running them
  cat /tmp/skyline.docker.dawn.sh

  # Run the build script
  chmod 0755 /tmp/skyline.docker.dawn.sh
  /tmp/skyline.docker.dawn.sh


.. code-block:: bash

  # Check the logs
  tail -n 60 /var/log/skyline/*.log
