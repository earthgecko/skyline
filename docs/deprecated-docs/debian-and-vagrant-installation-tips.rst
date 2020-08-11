====================================
Debian and Vagrant Installation Tips
====================================

Please note that this info is old and has not been updated since 25 Sep 2013

### Get a Wheezy box
From this useful [tutorial](http://dominique.broeglin.fr/2012/02/25/wheezy-64-vagrant-base-box.html)

The previous version, Debian Squeeze, struggled with an easy installation of
Redis and you can get it working with back-ports.

```
vagrant box add wheezy64 http://dl.dropbox.com/u/937870/VMs/wheezy64.box
mkdir skyline
cd skyline
vagrant init wheezy64
```

### Extra Vagrant configuration
Forward the port required for the web application

```
config.vm.network :forwarded_port, guest: 1500, host: 1500
```

### Python requirements
You'll need **pip** to be able to add some of the requirements for python, as
well as **python-dev** without which some of the packages used with pip will
not work!

```
sudo apt-get install python-dev
sudo apt-get install python-pip
```

After that we can clone the project’s repository


```
git clone https://github.com/earthgecko/skyline.git
```

Once we have the project we can change into the folder and start going through
all the python dependencies.

```
cd skyline
sudo pip install -r requirements.txt
sudo apt-get install python-numpy python-scipy python-scikits.statsmodels
sudo pip install patsy msgpack_python
```

### Skyline dependencies

```
sudo pip install -r requirements.txt
sudo apt-get install python-numpy python-scipy python-scikits.statsmodels
sudo pip install patsy msgpack_python
```

### Redis 2.6 on Debian Wheezy
Version 2.6 is available on the Wheezy back-port which means it will be
available in the next Debian stable version and was back ported to Wheezy.

```
sudo vim /etc/apt/sources.list
```

Add the back-port, replace **$YOUR_CONFIGURATION** according to your local
configuration.

```
deb http://**$YOUR_CONFIGURATION**.debian.org/debian/ wheezy-backports main
```

Update and install Redis-Server, don't forget to kill the process as it will be
started with the default config right after the installation.

```
sudo apt-get update
sudo apt-get -t wheezy-backports install redis-server
sudo pkill redis
```

### Skyline configuration
With all dependencies installed you can now copy an example for the settings
file. This will be the place to add your Graphite URL and other details but for
now we only need to update the IP bound to the web application so the Vagrant
host will display it.

```
cp src/settings.py.example src/settings.py
```

Edit file for bind.

```
vim src/settings.py
```

And change the IP to listen on host's requests.

```
WEBAPP_IP = '0.0.0.0'
```
