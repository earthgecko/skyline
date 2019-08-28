if [ ! -f /usr/sbin/apache2 ]; then
  sudo apt-get install apache2 net-tools -y
fi

if [ -f /etc/skyline/skyline.dawn.conf ]; then
  source /etc/skyline/skyline.dawn.conf
  echo "source user conf - /etc/skyline/skyline.dawn.conf, OK"
fi

APACHE_NAME="apache2"
mkdir -p /etc/$APACHE_NAME/ssl
if [[ ! -f /etc/$APACHE_NAME/ssl/$YOUR_SKYLINE_SERVER_FQDN.apache.key || ! -f /etc/$APACHE_NAME/ssl/$YOUR_SKYLINE_SERVER_FQDN.apache.crt ]]; then
  echo "Creating self signed SSL certificate for $YOUR_SKYLINE_SERVER_FQDN"
  sleep 1
  openssl req -new -newkey rsa:4096 -days 365 -nodes -x509 \
    -subj "/C=US/ST=None/L=None/O=Testing/CN=$YOUR_SKYLINE_SERVER_FQDN" \
    -keyout /etc/$APACHE_NAME/ssl/$YOUR_SKYLINE_SERVER_FQDN.apache.key -out /etc/$APACHE_NAME/ssl/$YOUR_SKYLINE_SERVER_FQDN.apache.crt
  if [ $? -ne 0 ]; then
    echo "error :: openssl failed to create self signed SSL certificate for $YOUR_SKYLINE_SERVER_FQDN"
    exit 1
  fi
else
  echo "Skipping creation of self signed SSL certificate for $YOUR_SKYLINE_SERVER_FQDN, already done."
  sleep 1
fi
HTTP_AUTH_FILE="/etc/apache2/.skyline_htpasswd"
if [ ! -f $HTTP_AUTH_FILE ]; then
  echo "Creating http auth file - $HTTP_AUTH_FILE"
  sleep 1
  htpasswd -b -c $HTTP_AUTH_FILE $WEBAPP_AUTH_USER $WEBAPP_AUTH_USER_PASSWORD
  if [ $? -ne 0 ]; then
    echo "error :: htpasswd failed to create $HTTP_AUTH_FILE"
    exit 1
  fi
else
  echo "Creating http auth file - $HTTP_AUTH_FILE"
  sleep 1
fi
SKYLINE_HTTP_CONF_FILE="/etc/apache2/sites-available/skyline.conf"
if [ ! -f $SKYLINE_HTTP_CONF_FILE ]; then
  echo "Creating http config - $SKYLINE_HTTP_CONF_FILE"
  sleep 1
  YOUR_ERROR_LOG="\/var\/log\/${APACHE_NAME}\/skyline.error.log"
  YOUR_CUSTOM_LOG="\/var\/log\/${APACHE_NAME}\/skyline.access.log"
  YOUR_PATH_TO_YOUR_CERTIFICATE_FILE="\/etc\/${APACHE_NAME}\/ssl\/${YOUR_SKYLINE_SERVER_FQDN}.apache.crt"
  YOUR_PATH_TO_YOUR_KEY_FILE="\/etc\/${APACHE_NAME}\/ssl\/${YOUR_SKYLINE_SERVER_FQDN}.apache.key"
  if [ "$OS" == "CentOS" ]; then
    YOUR_HTPASSWD_FILE="\/etc\/${APACHE_NAME}\/conf.d\/.skyline_htpasswd"
  else
    YOUR_HTPASSWD_FILE="\/etc\/${APACHE_NAME}\/.skyline_htpasswd"
  fi

#  cat /skyline/etc/skyline.httpd.conf.d.example \
  cat /opt/skyline/github/skyline/etc/skyline.docker.apache2.conf.d.example \
    | sed -e 's/<YOUR_SERVER_IP_ADDRESS>/'$YOUR_SERVER_IP_ADDRESS'/g' \
    | sed -e 's/<YOUR_SKYLINE_SERVER_FQDN>/'$YOUR_SKYLINE_SERVER_FQDN'/g' \
    | sed -e 's/<YOUR_EMAIL>/'$YOUR_EMAIL'/g' \
    | sed -e 's/<YOUR_ERROR_LOG>/'$YOUR_ERROR_LOG'/g' \
    | sed -e 's/"<YOUR_CUSTOM_LOG>"/"'$YOUR_CUSTOM_LOG'" combined/g' \
    | sed -e 's/<YOUR_PATH_TO_YOUR_CERTIFICATE_FILE>/'$YOUR_PATH_TO_YOUR_CERTIFICATE_FILE'/g' \
    | sed -e 's/<YOUR_PATH_TO_YOUR_KEY_FILE>/'$YOUR_PATH_TO_YOUR_KEY_FILE'/g' \
    | sed -e 's/SSLCertificateChainFile/#SSLCertificateChainFile/g' \
    | sed -e 's/<YOUR_HTPASSWD_FILE>/'$YOUR_HTPASSWD_FILE'/g' \
    | sed -e 's/<YOUR_OTHER_IP_ADDRESS>/'$YOUR_OTHER_IP_ADDRESS'/g' > $SKYLINE_HTTP_CONF_FILE
else
  echo "Skipping creating http config - $SKYLINE_HTTP_CONF_FILE, already done."
fi
a2enmod ssl
a2enmod proxy
a2enmod proxy_http
a2enmod headers
a2enmod rewrite
sed -i 's/.*IfModule.*//g;s/.*LoadModule.*//g' /etc/apache2/sites-available/skyline.conf
a2ensite skyline.conf

# Add YOUR_SKYLINE_SERVER_FQDN to the hosts file to stop Apache complaining
if [ $(cat /etc/hosts | grep -c "$YOUR_SERVER_IP_ADDRESS $YOUR_SKYLINE_SERVER_FQDN") -eq 0 ]; then
  echo "$YOUR_SERVER_IP_ADDRESS $YOUR_SKYLINE_SERVER_FQDN" >> /etc/hosts
fi

if [ $(cat /etc/hosts | grep -c "$YOUR_GRAPHITE_IP_ADDRESS $YOUR_GRAPHITE_SERVER_FQDN") -eq 0 ]; then
  echo "$YOUR_GRAPHITE_IP_ADDRESS $YOUR_GRAPHITE_SERVER_FQDN" >> /etc/hosts
fi

chown -R www-data:www-data /var/log/apache2
service apache2 restart

#### Skyline settings ####
echo "Populating variables in the Skyline settings.py"
sleep 1
if [ -f /opt/skyline/github/skyline/skyline/docker.settings.py ]; then
  if [ ! -f /opt/skyline/github/skyline/skyline/settings.py.original ]; then
    cat /opt/skyline/github/skyline/skyline/settings.py > /opt/skyline/github/skyline/skyline/settings.py.original
  fi
  cat /opt/skyline/github/skyline/skyline/docker.settings.py > /opt/skyline/github/skyline/skyline/settings.py
  echo "Populated variables in the Skyline settings.py using /opt/skyline/github/skyline/skyline/docker.settings.py"
  sleep 1
else
  if [ ! -f /opt/skyline/github/skyline/skyline/settings.py.original ]; then
    cat /opt/skyline/github/skyline/skyline/settings.py > /opt/skyline/github/skyline/skyline/settings.py.original
  fi
  # @modified 20180823 - Bug #2552: seed_data.py testing with UDP does not work (GH77)
  # Only requires a public IP if Grpahite is going to pickle to it, but seeing as
  # this is a test node, make it 127.0.0.1 as there are no iptables on the IP or
  # ports 2025 or 2024
  #    | sed -e "s/HORIZON_IP = .*/HORIZON_IP = '$YOUR_SERVER_IP_ADDRESS'/g" \
  cat /opt/skyline/github/skyline/skyline/settings.py.original \
    | sed -e "s/SERVER_METRICS_NAME = .*/SERVER_METRICS_NAME = 'skyline-docker-skyline-1'/g" \
    | sed -e "s/REDIS_PASSWORD = .*/REDIS_PASSWORD = '$REDIS_PASSWORD'/g" \
    | sed -e 's/PANORAMA_ENABLED = .*/PANORAMA_ENABLED = True/g' \
    | sed -e "s/WEBAPP_AUTH_USER = .*/WEBAPP_AUTH_USER = '$WEBAPP_AUTH_USER'/g" \
    | sed -e 's/PANORAMA_ENABLED = .*/PANORAMA_ENABLED = True/g' \
    | sed -e "s/WEBAPP_AUTH_USER_PASSWORD = .*/WEBAPP_AUTH_USER_PASSWORD = '$WEBAPP_AUTH_USER_PASSWORD'/g" \
    | sed -e "s/WEBAPP_ALLOWED_IPS = .*/WEBAPP_ALLOWED_IPS = ['127.0.0.1', '$YOUR_OTHER_IP_ADDRESS']/g" \
    | sed -e "s/SKYLINE_URL = .*/SKYLINE_URL = 'https:\/\/$YOUR_SKYLINE_SERVER_FQDN'/g" \
    | sed -e 's/MEMCACHE_ENABLED = .*/MEMCACHE_ENABLED = True/g' \
    | sed -e "s/PANORAMA_DBUSER = .*/PANORAMA_DBUSER = 'root'/g" \
    | sed -e "s/PANORAMA_DBHOST = .*/PANORAMA_DBHOST = 'mysql'/g" \
    | sed -e "s/REDIS_SOCKET_PATH = .*/REDIS_SOCKET_PATH = '\/tmp\/docker\/redis.sock'/g" \
    | sed -e "s/HORIZON_IP = .*/HORIZON_IP = '$YOUR_SERVER_IP_ADDRESS'/g" \
    | sed -e "s/MEMCACHE_ENABLED = .*/MEMCACHE_ENABLED = True/g" \
    | sed -e "s/CARBON_HOST = .*/CARBON_HOST = 'skyline-docker-graphite-statsd-1'/g" \
    | sed -e "s/PANORAMA_DBUSERPASS = .*/PANORAMA_DBUSERPASS = '$MYSQL_SKYLINE_PASSWORD'/g" > /opt/skyline/github/skyline/skyline/settings.py
  if [ $? -ne 0 ]; then
    echo "error :: failed to populate the variables in /opt/skyline/github/skyline/skyline/settings.py"
    exit 1
  else
    echo "Populated variables in the Skyline settings.py"
    sleep 1
  fi
fi

if [ -f /tmp/docker_build ]; then
  echo "Not installing skyline DB during docker build"
else
# @added 20190621 - Branch #3002: docker
# On the initial docker-compose up, the mysql container can take longer to
# initiated than it takes the skyline docker container to get to this point.
# Added a wait for up to 120 seconds until MySQL is available
  MYSQL_UP=1
  START_MYSQL_WAIT=$(date +%s)
  while [ $MYSQL_UP -ne 0 ]
  do
    NOW=$(date +%s)
    WAITED_FOR=$((NOW-START_MYSQL_WAIT))
    if [ $WAITED_FOR -ge 120 ]; then
      MYSQL_UP=0
      break
    fi
    mysql -uroot -h skyline-docker-mysql-1 -sss -e "SHOW DATABASES" > /dev/null
    MYSQL_EXIT_CODE=$?
    if [ $MYSQL_EXIT_CODE -eq 0 ]; then
      echo "MySQL on skyline-docker-mysql-1 is now up and available"
      MYSQL_UP=0
    else
      START_MYSQL_WAIT=$(date +%s)
      sleep 2
    fi
  done

  SKYLINE_DB_INSTALLED=$(mysql -uroot -h skyline-docker-mysql-1 -sss -e "SHOW DATABASES" | grep -c skyline)
  if [ $SKYLINE_DB_INSTALLED -eq 0 ]; then
    echo "Creating skyline DB"
    mysql -uroot -h skyline-docker-mysql-1 < /opt/skyline/github/skyline/skyline/skyline.sql
    if [ $? -ne 0 ]; then
      echo "error :: failed to create the skyline DB using /opt/skyline/github/skyline/skyline/skyline.sql"
      exit 1
    else
      echo "Created the skyline DB using /opt/skyline/github/skyline/skyline/skyline.sql"
    fi
  else
    echo "skyline DB already created"
  fi
fi
# Remove pid files
sudo rm -rf /var/run/skyline
sudo mkdir -p /var/run/skyline
