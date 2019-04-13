cd /usr/src
sudo wget https://www.python.org/ftp/python/2.7.14/Python-2.7.14.tgz
sudo tar xzf Python-2.7.14.tgz
cd Python-2.7.14
sudo ./configure --enable-optimizations --enable-unicode=ucs4 && make
sudo make install
sudo rm -rf /usr/src/Python-2.7.14*

python -V

cd /skyline
echo $PWD

# To suppress "You are using pip version 8.1.1, however version 19.0.3 is available."
# TODO: check if works with newer version of pip
sudo pip install --upgrade pip

sudo pip install docutils
sudo pip install $(cat requirements.txt | grep "^numpy\|^scipy\|^patsy" | tr '\n' ' ')
sudo pip install $(cat requirements.txt | grep "^pandas")
sudo pip install -r requirements.txt
sudo pip install --upgrade setuptools

