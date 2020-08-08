docker - EXPERIMENTAL AND NO LONGER MAINTAINED
==============================================

*NOT SUITABLE FOR PRODUCTION*

No longer maintained
--------------------

Running Skyline on docker is possible but it is no longer maintained.  It is a
very complex pattern that is not realistic to maintain with every release,
building, testing and updating deps, building new dockerrr layers every time a
change is made... who can be bothered?  Add to that supporting the divergence of
all things container is not something that aids or has anything to do with
anomaly detection.  Therefore dealing with docker, docker-composer, podman,
buildah, skopeo, runc, k8s and all the other container facets is no longer
being maintained.

Why docker was added
--------------------

Because the team at wix-playground forked Skyline and dockerized in its simplest
context.

Merging the wix-playground's changes Skyline had to be tested running on docker
and with that, the docker event horizon was crossed.

Docker does add a lot of potential in terms of developing Skyline in integrate
with other things.

So Skyline can now be run on docker in a meaningful way, with thanks going to
the team @wix-playground for pulling Skyline over the docker event horizon.

Docker should only be used for **testing** and it is probably **not suitable**
for production.  Running the Skyline Docker application should only be done on a
developer or test machine until you are familiar with the docker application and
can modify it to fit with docker practices and standards for your environment.

Not suitable for production
---------------------------

Although important directories are persisted to local directories using volumes
one of the containers *WILL* lose data!!!!  If OOMkiller kicks in on the host
and the mysql container is killed the data *CAN BE LOST* as the new mysql image
initialises the mounted volume and goes back to default.  You have been warned.

Docker application
------------------

Seeing as it is possible to ran anything on docker and your localhost, the entire
Skyline stack can be composed with docker with every dependencies provided for
by a container, constructing a docker-compose multi-container Docker application
for Skyline.

The Skyline Docker application provides the following containers:

- busybox: to provide a shared /tmp/docker volume for a Redis unix socket
- mysql
- redis
- memcached
- graphite-statsd
- skyline

Running on a Docker user defined network.

The use of docker is EXPERIMENTAL.  It has a lot more components than it
requires to be just a simple Skyline docker container.  It is not really
container friendly yet.  Containers should run one thing each, here this is not
the case, the Skyline docker application is basically a group of containerised
VMs.

No extensive testing has been done with Skyline on docker and no security or
docker best practices have been applied.

The experimental Skyline docker implementation uses the graphite-statsd docker
image https://hub.docker.com/r/graphiteapp/graphite-statsd/ to provide Skyline
with data.

Requirements
------------

- git
- rsync
- docker (Docker Engine version 17.09.0 and higher)
- docker-compose

Notable docker aspects
----------------------

Feel free to skip to start of the Installation section if you are not interested
in the docker internals.

The docker Skyline container currently runs on the debian:stretch docker image,
not debian:latest which was recently bumped to up to buster.  This is less than
ideal seeing as this image ships with more vulnerabilities than any other
popular docker OS image.  All future docker work should be focused on getting
Skyline to run on the alpine image with each Skyline application running as
individual containers.

For now it is a monolithic stretch container, which is useful for development
purposes at least.

There are hard coded variables in terms of the user defined network which is
subnet 172.118.0.0/16 called skylinenetwork.  For all the container static IPs
see the docker-compose.yml file.

Installation - quick start
--------------------------

The data for the Skyline, Graphite, Redis and MySQL containers are persisted in
the below patterns.

There is a convenience build script for testing purposes only in
utils/dawn/skyline.docker.dawn.sh see
`Development - dawn-docker <development/dawn-docker.html>`__ section (NO LONGER
MAINTAINED) or you can follow the simple steps below.

.. note:: Ensure you provide the appropriate ownership and permissions to the
  below specified directories for the user you wish to run the Skyline process
  as.

If you want to modify the default docker settings that is deployed, after
cloning add the file /opt/skyline/github/skyline/skyline/docker.settings.py with
your desired settings based on the cloned
/opt/skyline/github/skyline/skyline/docker.settings.py.default

.. code-block:: bash

    # Create the required directories for persisting data
    # NOTE if you change this, you will have to modify all the paths in the
    # skyline/Dockerfile to match
    # Also NOTE due to docker processes running as root you will need to use
    # sudo to read or clean up these dirs or files
    sudo mkdir -p /opt/skyline/github

    # If your group is different from your username like on MacOS it will probably
    # be staff
    YOUR_GROUP="$USER"
    sudo chown -R $USER:$YOUR_GROUP /opt/skyline

    # Define SKYLINE_DOCKER_HOME directory
    SKYLINE_DOCKER_HOME="/opt/skyline/docker"

    mkdir -p $SKYLINE_DOCKER_HOME/skyline-docker-skyline-1/skyline
    mkdir -p $SKYLINE_DOCKER_HOME/skyline-docker-mysql-1/var/lib/mysql
    mkdir -p $SKYLINE_DOCKER_HOME/skyline-docker-redis-1/var/lib/redis
    mkdir -p $SKYLINE_DOCKER_HOME/skyline-docker-graphite-statsd-1/opt/graphite/storage
    mkdir -p $SKYLINE_DOCKER_HOME/skyline-docker-graphite-statsd-1/var/log
    mkdir -p $SKYLINE_DOCKER_HOME/skyline-docker-skyline-1/opt/skyline/ionosphere
    mkdir -p $SKYLINE_DOCKER_HOME/skyline-docker-skyline-1/var/log/skyline
    mkdir -p $SKYLINE_DOCKER_HOME/skyline-docker-skyline-1/var/log/apache2

    cd /opt/skyline/github
    git clone https://github.com/earthgecko/skyline
    # Switch to a branch or git commitref if you want to

    # If you do not want to use the default settings add your desired settings
    # to /opt/skyline/github/skyline/skyline/docker.settings.py based on the
    # cloned /opt/skyline/github/skyline/skyline/docker.settings.py.default

    # Now copy the repo to the container directory
    rsync -az --exclude .git/ /opt/skyline/github/skyline/ $SKYLINE_DOCKER_HOME/skyline-docker-skyline-1/skyline/
    cd $SKYLINE_DOCKER_HOME/skyline-docker-skyline-1/skyline

    docker-compose build

    docker-compose up  # first run takes a while for all the apps to start and initialise
    # After 2 or 3 minutes when all the apps have started and are logging
    # consistently you can Ctrl+c to stop the containers and then start detached
    # Ctrl+c
    docker-compose up -d

Working with the Docker stack on localhost
------------------------------------------

To access the Skyline and Graphite applications on your localhost you need to
add the following entries to your `/etc/hosts` file (or the equivalent)

.. code-block:: bash

    echo "172.118.0.7 skyline-docker-skyline-1" >> /etc/hosts
    echo "172.118.0.6 skyline-docker-graphite-statsd-1" >> /etc/hosts

You can now access the apps in your browser (using the default username and
password if you did not add your own docker.settings.py) via:

- https://skyline-docker-skyline-1 (username: admin password: skyline-docker-skyline-1)
- https://skyline-docker-graphite-statsd-1 (user: admin pass: skyline-docker-skyline-1)

You need to accept the self signed SSL certificates for each in your browser.

In this scenario metric transport can be provided from your localhost directly
to the skyline-docker-graphite-statsd-1 via various means.  You can configure
any local Graphite compatible metric collector to send metrics directly to
skyline-docker-graphite-statsd-1:2013.  Using telegraf for instance:

telegraf ---> skyline-docker-graphite-statsd-1:2013 ---> skyline-docker-skyline-1

telegraf.conf would need:

.. code-block:: ini

  [[outputs.graphite]]
    servers = ["skyline-docker-graphite-statsd-1:2013"]

The Skyline docker container is configured and deployed at build using:

- utils/docker/init.sh
- utils/docker/configure.sh
- with the utils/docker/configs/skyline/skyline/etc/skyline/skyline.dawn.conf
  configuration variables.
- and skyline/docker.settings.py.default (or skyline/docker.settings.py if you
  provided it)

Accessing the Skyline containers via the command line:

.. code-block:: bash

    # List the containers
    docker ps
    # Skyline container access
    docker exec -it skyline_skyline-docker-skyline-1_1 /bin/bash
    # Graphite container access
    docker exec -it skyline-docker-graphite-statsd-1 /bin/sh
    # Redis container access
    docker exec -it skyline-docker-redis-1 /bin/bash
    # MySQL container access
    docker exec -it skyline-docker-mysql-1 /bin/bash
    # memcached container access
    docker exec -it skyline-docker-memcached-1 /bin/bash

You can also change the Skyline settings.py for the docker instance directly on
the skyline_skyline-docker-skyline-1_1 container in
`/opt/skyline/github/skyline/skyline/docker.settings.py` or in the source on the
host filesystem `$SKYLINE_DOCKER_HOME/skyline-docker-skyline-1/skyline/skyline/settings.py`,
then stop the Skyline container and start it again.

.. code-block:: bash

    docker stop skyline_skyline-docker-skyline-1_1
    docker start skyline_skyline-docker-skyline-1_1

Or restart the Skyline apps one by one in the container itself.

Other app configurations can be changed in the relevant configuration files in
the relevant local app volume directories as well, e.g.

- /opt/skyline/github/skyline/utils/docker/configs/skyline/redis/etc/redis (container volume /etc/redis)
- /opt/skyline/docker/skyline-docker-skyline-1/skyline/utils/docker/configs/skyline/graphite-statsd/opt/graphite/conf (container volume /opt/graphite/conf)
- /opt/skyline/docker/skyline-docker-skyline-1/skyline/utils/docker/configs/skyline/graphite-statsd/opt/statsd/config (container volume /opt/statsd/config)
- /opt/skyline/docker/skyline-docker-skyline-1/skyline/utils/docker/configs/skyline/graphite-statsd/etc/nginx (container volume /etc/nginx)
- /opt/skyline/docker/skyline-docker-skyline-1/skyline/utils/docker/configs/skyline/graphite-statsd/etc/logrotate.d (container volume /etc/logrotate.d)

Skyline optimisations for containerisation
------------------------------------------

Changes were made to Skyline from using Python multiprocessing Manager().list()
to using Redis sets to reduce the overall footprint greatly especially in terms
of process count and resident segment sizes.
