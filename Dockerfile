FROM debian:latest as base
RUN apt-get update && apt-get install build-essential checkinstall sudo vim git wget memcached python-pip libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev -y
RUN mkdir -p /skyline
COPY . /skyline
WORKDIR /skyline
ENV PYTHONPATH=/usr/local/lib/python2.7/dist-packages:$PYTHONPATH
RUN sh /skyline/docker_scripts/install_environment.sh


FROM base as run_skyline
RUN sh /skyline/docker_scripts /init.sh
EXPOSE 1500 3306 2024
