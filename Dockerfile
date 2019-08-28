FROM debian:stretch as base
# Hard coded to stretch as buster is now latest and has not been tested
#FROM debian:latest as base

ARG docker_build
RUN echo "docker_build - $docker_build"

RUN apt-get update && apt-get install build-essential checkinstall sudo vim git wget memcached python-pip libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev mysql-client -y

# Install requirements first before COPY . /skyline which invalidates all
# subsequent cache layers
COPY requirements.txt /tmp/requirements.txt
WORKDIR /tmp
RUN pip install docutils
RUN pip install $(cat requirements.txt | grep "^numpy\|^scipy\|^patsy" | tr '\n' ' ')
RUN pip install $(cat requirements.txt | grep "^pandas")
RUN pip install -r requirements.txt
RUN pip install --upgrade setuptools

RUN apt-get -y install apache2 net-tools

RUN mkdir -p /opt/skyline/github/skyline
#COPY . /skyline
#WORKDIR /skyline
COPY . /opt/skyline/github/skyline
WORKDIR /opt/skyline/github/skyline

FROM base as skyline-docker-skyline-1
#COPY . /skyline
COPY . /opt/skyline/github/skyline
ARG docker_build

RUN ln -sf /opt/skyline/github/skyline /skyline
RUN chmod 0755 /skyline/utils/docker/init.sh
EXPOSE 1500 2024 443
RUN sh /skyline/utils/docker/init.sh
