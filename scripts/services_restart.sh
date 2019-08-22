#!/bin/bash
echo "Restarting Skyline services"
sleep 1
SERVICES="/opt/skyline/github/skyline/bin/horizon.d
/opt/skyline/github/skyline/bin/panorama.d
/opt/skyline/github/skyline/bin/analyzer.d
/opt/skyline/github/skyline/bin/webapp.d
/opt/skyline/github/skyline/bin/ionosphere.d
/opt/skyline/github/skyline/bin/luminosity.d
/opt/skyline/github/skyline/bin/boundary.d"
for i_service in $SERVICES
do
  sudo $i_service restart
  if [ $? -ne 0 ]; then
    echo "error :: failed to restart $i_service"
    exit 1
  fi
done

