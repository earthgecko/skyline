FROM debian:latest

RUN apt-get update
RUN apt-get install build-essential checkinstall sudo vim git wget memcached python-pip -y
RUN sudo apt-get install libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev -y

RUN git clone https://github.com/wix-playground/skyline.git
WORKDIR /skyline

ENV PYTHONPATH=/usr/local/lib/python2.7/dist-packages:$PYTHONPATH

#skyline webserver port
EXPOSE :1500 

#Panorama DB Port
EXPOSE :3306

#graphite collection port
EXPOSE :2024

RUN sh /skyline/init.sh

