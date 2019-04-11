cd /skyline

echo $PWD
mkdir -p /var/log/skyline
mkdir -p /var/run/skyline
mkdir -p /var/dump
mkdir -p /opt/skyline/panorama/check
mkdir -p /opt/skyline/mirage/check
mkdir -p /opt/skyline/crucible/check
mkdir -p /opt/skyline/crucible/data
mkdir -p /opt/skyline/ionosphere
mkdir -p /etc/skyline
mkdir -p /tmp/skyline


cp /skyline/etc/skyline_docker.conf /etc/skyline/skyline.conf

bash docker_scripts/configure_apache.sh

/skyline/bin/horizon.d start
/skyline/bin/analyzer.d start
/skyline/bin/webapp.d start


# tail -f /var/log/skyline/*
