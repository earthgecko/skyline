cd /usr/src
sudo wget https://www.python.org/ftp/python/2.7.14/Python-2.7.14.tgz
sudo tar xzf Python-2.7.14.tgz
cd Python-2.7.14
sudo ./configure --enable-optimizations
sudo make install
sudo rm -rf /usr/src/Python-2.7.14*

python -V

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

cd /skyline
sudo pip install docutils
sudo pip install $(cat requirements.txt | grep "^numpy\|^scipy\|^patsy" | tr '\n' ' ')
sudo pip install $(cat requirements.txt | grep "^pandas")
sudo pip install -r requirements.txt

cp /skyline/etc/skyline_docker.conf /etc/skyline/skyline.conf
export PYTHONPATH=/usr/local/lib/python2.7/dist-packages:$PYTHONPATH