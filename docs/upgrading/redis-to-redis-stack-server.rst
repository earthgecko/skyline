=======================================
Upgrading - Redis to redis-stack-server
=======================================

This section covers the steps required to upgrade from running Skyline Redis to
redis-stack-server.  It requires the Redis server version to be >= 5 as the RDB
data from Redis 5 is compatibile with Redis 6.2

**This is a guide only**, your Redis set up and configuration may be different, first
read through all the steps and determine how they fit in with your set up, paths,
ports, etc.

Although redis.io has yum/apt repos with a packaged version they provide
redis-stack-server RC (release candidate) packages at times and in terms of Skyline
Redis needs to be stable, using the download binaries for the OS is a more stable
method and the method described here.

Assumptions made:
- Your Redis is on port 6379
- YOU read the paths in the below guide and assess them relating to your set up

Things you need to determine beforehand:
- Your Redis password
- The path to your existing Redis dump.rdb (e.g. /var/lib/redis/6379/dump.rdb)
- Whether you just want to add ``module redistimeseries.so`` to your Redis config
  or whether you want to run the Redis 6.2 config.

The upgrade process.  First we install redis-stack-server (CentOS and Ubuntu
patterns below), then we stop Skyline services, stop Redis, copy the dump.rdb
file to redis-stack-server and then start redis-stack-server and start the
Skyline services again.

You will then need to update any of your own things that start/stop or
control/monitor the redis-server process and update to use the
redis-stack-server process.

- First install redis-stack-server
- For CentOS 8 Stream (Ubuntu is below)

.. code-block:: bash

  # CentOS 8
  REDIS_STACK_VERSION="redis-stack-server-6.2.6-v7"
  mkdir /opt/redis-stack-server
  wget -O "/opt/redis-stack-server/${REDIS_STACK_VERSION}.rhel8.x86_64.tar.gz" "https://packages.redis.io/redis-stack/${REDIS_STACK_VERSION}.rhel8.x86_64.tar.gz"
  cd /opt/redis-stack-server
  tar -zxvf "${REDIS_STACK_VERSION}.rhel8.x86_64.tar.gz"
  # Deploy as the redis.io yum package deploys
  mkdir -p /opt/redis-stack
  chown nobody:nobody /opt/redis-stack
  rsync -az "/opt/redis-stack-server/${REDIS_STACK_VERSION}/" /opt/redis-stack/
  chown -R nobody:nobody /opt/redis-stack
  mkdir -p /var/lib/redis-stack
  chown -R nobody:nobody /var/lib/redis-stack

- Ubuntu

.. code-block:: bash

  # Ubuntu 20.04 and 22.04
  source /etc/lsb-release
  REDIS_STACK_VERSION="redis-stack-server-6.2.6-v7"
  mkdir /opt/redis-stack-server
  wget -O "/opt/redis-stack-server/${REDIS_STACK_VERSION}.${DISTRIB_CODENAME}.x86_64.tar.gz" "https://packages.redis.io/redis-stack/${REDIS_STACK_VERSION}.${DISTRIB_CODENAME}.x86_64.tar.gz"
  cd /opt/redis-stack-server
  tar -zxvf "${REDIS_STACK_VERSION}.${CODENAME}.x86_64.tar.gz"
  mkdir -p /opt/redis-stack
  chown nobody:nogroup /opt/redis-stack
  rsync -az "/opt/redis-stack-server/${REDIS_STACK_VERSION}/" /opt/redis-stack/
  chown -R nobody:nogroup /opt/redis-stack
  mkdir -p /var/lib/redis-stack
  chown -R nobody:nogroup /var/lib/redis-stack

- On both CentOS and Ubuntu create the systemd service file

.. code-block:: bash

  echo "[Unit]
  Description=Redis stack server
  Documentation=https://redis.io/
  After=network.target

  [Service]
  Type=simple
  User=nobody
  ExecStart=/opt/redis-stack/bin/redis-server /etc/redis-stack.conf
  WorkingDirectory=/var/lib/redis-stack
  UMask=0077

  [Install]
  WantedBy=multi-user.target" > /etc/systemd/system/redis-stack-server.service
  systemctl daemon-reload

- Decide on the Redis config you want to use.  If you want to use your own, then
  copy it to the redis-stack.conf and add ``loadmodule /opt/redis-stack/lib/redistimeseries.so``:

.. code-block:: bash

  # SET THIS TO YOUR_REDIS_CONFIG
  #REDIS_CONF=/etc/redis.conf
  #REDIS_CONF=/etc/redis/6379/redis.conf
  REDIS_CONF=<YOUR_REDIS_CONFIG>

  cp /etc/redis-stack.conf /etc/redis-stack.conf.${REDIS_STACK_VERSION}.bak
  cat "$REDIS_CONF" \
    | sed -e 's/# loadmodule \/path\/to\/other_module.so/# loadmodule \/path\/to\/other_module\.so\nloadmodule \/opt\/redis-stack\/lib\/redistimeseries\.so/1' \
    > /etc/redis-stack.conf
  # Whichs adds the following line
  # loadmodule /opt/redis-stack/lib/redistimeseries.so

- If you want to use the default Redis 6.2 config with the required changes
  then do the following

.. code-block:: bash

  # SET THIS TO YOUR REDIS PASSWORD
  REDIS_PASSWORD=<YOUR_REDIS_PASSWORD>

  cp /etc/redis-stack.conf /etc/redis-stack.conf.${REDIS_STACK_VERSION}.bak
  wget -O /opt/redis-stack-server/redis.6.2.github.conf https://raw.githubusercontent.com/redis/redis/6.2/redis.conf
  cat /opt/redis-stack-server/redis.6.2.github.conf \
    | sed -e 's/# unixsocketperm 700/# unixsocketperm 700\nunixsocket \/tmp\/redis\.sock\nunixsocketperm 777/1' \
    | sed -e 's/# requirepass foobared/# requirepass foobared\nrequirepass '$REDIS_PASSWORD'/1' \
    | sed -e 's/# loadmodule \/path\/to\/other_module.so/# loadmodule \/path\/to\/other_module\.so\nloadmodule \/opt\/redis-stack\/lib\/redistimeseries\.so/1' \
    | sed -e 's/bind 127\.0\.0\.1 -::1/# bind 127\.0\.0\.1 -::1\nbind 127\.0\.0\.1/1' \
    | sed -e 's/# supervised auto/# supervised auto\nsupervised systemd/1' \
    | sed -e 's/dir \.\//# dir \.\/\ndir \/var\/lib\/redis-stack/1' \
    | sed -e 's/# maxmemory-policy noeviction/# maxmemory-policy noeviction\nmaxmemory-policy noeviction/1' \
    | sed -e 's/appendonly no/# appendonly no\nappendonly yes/1' \
    | sed -e 's/# save 60 10000/# save 60 10000\n# Skyline reduce save I\/O\nsave 300 1\n save 60 200000\nsave 10 500000/1' \
    > /opt/redis-stack-server/skyline.redis.6.2.conf
  cat /opt/redis-stack-server/skyline.redis.6.2.conf > /etc/redis-stack.conf

- redis-stack-server is now installed
- Stop/disable any/all service controls like monit, etc that are controlling
  Skyline services.

.. code-block:: bash

  # Stop/disable any/all service controls like monit, etc that are controlling
  # Skyline services.

  # NOTE ALL SKYLINE SERVICES ARE LISTED HERE, REMOVE TO ONES YOU DO NOT RUN
  # or do not wish to run.
  SKYLINE_SERVICES="analyzer
  analyzer_batch
  mirage
  crucible
  boundary
  ionosphere
  luminosity
  panorama
  webapp
  vista
  snab
  flux
  horizon"
  for i in $SKYLINE_SERVICES
  do
    systemctl stop "$i"
  done

- Stop/disable any/all service controls like monit, etc that are controlling
  Redis, for example.

.. code-block:: bash

  # Stop/disable any/all service controls like monit, etc that are controlling
  # Redis.
  systemctl stop redis
  systemctl disable redis
  # ALSO REMOVE ANY CHECKS THAT WOULD RESTART THE REDIS SERVICE

- Copy the dump.rdb file to redis-stack-server

.. code-block:: bash

  # SET THE PATH TO YOUR dump.rdb file, e.g.
  # RDB_FILE="/var/lib/redis/6379/dump.rdb"
  RDB_FILE="<YOUR_RDB_FILE>"

  cp "$RDB_FILE" /var/lib/redis-stack/

- Start redis-stack-server

.. code-block:: bash

  systemctl start redis-stack-server
  systemctl enable redis-stack-server
  # Test
  /opt/redis-stack/bin/redis-cli -a $REDIS_PASSWORD INFO

- Start your Skyline services.

.. code-block:: bash

  for i in $SKYLINE_SERVICES
  do
    systemctl start "$i"
  done

- Ensure you update your builds/config_management to use and monitor
  the redis-stack-server service and remove the redis service and
  add also add the new changes and new config file.
