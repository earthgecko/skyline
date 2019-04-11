sudo apt-get install apache2 -y

#### USER DEFINED VARIABLES ####
YOUR_SERVER_IP_ADDRESS="$(ifconfig eth0 | grep -o "inet.*" | cut -d " " -f2)"                         # YOUR Skyline server public IP address
YOUR_SKYLINE_SERVER_FQDN="$(hostname -f)"        # YOUR Skyline server FQDN
YOUR_EMAIL="skyline@wix.com"                                # YOUR email address for the httpd server admin
YOUR_OTHER_IP_ADDRESS="0.0.0.0"                          # YOUR current public IP address that you will be connecting from
WEBAPP_AUTH_USER="admin"                                   # The username you want to use for http authentication
WEBAPP_AUTH_USER_PASSWORD="$(echo {$HOSTNAME}_skyline)"    # The password you want to use for http authentication
MYSQL_ROOT_PASSWORD="XXXXXXXXX"     # The MySQL root user password
MYSQL_SKYLINE_PASSWORD="XXXXXXXXX"  # The Skyline DB user password
REDIS_PASSWORD="XXXXXXXXX"       # The Redis password
SKYLINE_RELEASE="v1.2.121"                 # The Skyline release to deploy


APACHE_NAME="apache2"
mkdir -p /etc/$APACHE_NAME/ssl
if [[ ! -f /etc/$APACHE_NAME/ssl/apache.key || ! -f /etc/$APACHE_NAME/ssl/apache.crt ]]; then
  echo "Creating self signed SSL certificate for $YOUR_SKYLINE_SERVER_FQDN"
  sleep 1
  openssl req -new -newkey rsa:4096 -days 365 -nodes -x509 \
    -subj "/C=US/ST=None/L=None/O=Testing/CN=$YOUR_SKYLINE_SERVER_FQDN" \
    -keyout /etc/$APACHE_NAME/ssl/apache.key -out /etc/$APACHE_NAME/ssl/apache.crt
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
  YOUR_PATH_TO_YOUR_CERTIFICATE_FILE="\/etc\/${APACHE_NAME}\/ssl\/apache.crt"
  YOUR_PATH_TO_YOUR_KEY_FILE="\/etc\/${APACHE_NAME}\/ssl\/apache.key"
  if [ "$OS" == "CentOS" ]; then
    YOUR_HTPASSWD_FILE="\/etc\/${APACHE_NAME}\/conf.d\/.skyline_htpasswd"
  else
    YOUR_HTPASSWD_FILE="\/etc\/${APACHE_NAME}\/.skyline_htpasswd"
  fi

  cat /opt/skyline/github/skyline/etc/skyline.httpd.conf.d.example \
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
service apache2 restart