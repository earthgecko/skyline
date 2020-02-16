cd /skyline

echo $PWD
mkdir -p /var/log/skyline
mkdir -p /var/run/skyline
mkdir -p /var/dump
mkdir -p /opt/skyline/panorama/check
mkdir -p /opt/skyline/mirage/check
mkdir -p /opt/skyline/crucible/check
mkdir -p /opt/skyline/crucible/data
mkdir -p /opt/skyline/ionosphere/check
mkdir -p /etc/skyline
mkdir -p /tmp/skyline

/bin/cp -f /opt/skyline/github/skyline/etc/skyline_docker.conf /etc/skyline/skyline.conf
/bin/cp -f /opt/skyline/github/skyline/utils/docker/configs/skyline/skyline/etc/skyline/skyline.dawn.conf /etc/skyline/skyline.dawn.conf

if [ -n "$docker_build" ]; then
  echo "docker_build - $docker_build"
  export docker_build
  touch /tmp/docker_build
fi
sudo bash /opt/skyline/github/skyline/utils/docker/configure.sh

if [ -n "$docker_build" ]; then
  if [ $docker_build -eq 1 ]; then
    echo "Skyline container built, OK"
    rm -rf /tmp/docker_build
  fi
else
  # Clean up
  find /skyline/skyline -type f -name "*.pyc" -exec rm -f {} \;
  /skyline/bin/panorama.d start
  /skyline/bin/horizon.d start
  /skyline/bin/flux.d start
  /skyline/bin/vista.d start
  /skyline/bin/analyzer.d start
  /skyline/bin/webapp.d start
  /skyline/bin/mirage.d start
  /skyline/bin/boundary.d start
  /skyline/bin/ionosphere.d start
  /skyline/bin/luminosity.d start
  /skyline/bin/crucible.d start
  tail -f /var/log/skyline/*.log
fi
