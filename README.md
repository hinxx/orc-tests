# orc tests

## C++

wget https://dlcdn.apache.org/orc/orc-1.8.2/orc-1.8.2.tar.gz
tar xf orc-1.8.2.tar.gz

mkdir build-1.8.2
cd build-1.8.2
cmake ../orc-1.8.2 -DBUILD_JAVA=OFF -DCMAKE_BUILD_TYPE=DEBUG -DCMAKE_INSTALL_PREFIX=../install-1.8.2

make -j
make install
cd ..


cd tests
make

## C++ git

git clone git@github.com:hinxx/orc.git orc-git

mkdir build-git
cd build-git
cmake ../orc-git/ -DCMAKE_BUILD_TYPE=DEBUG -DBUILD_JAVA=OFF -DCMAKE_INSTALL_PREFIX=$(pwd)/../install-git

make -j11
make install


## Java git

cd java
./mvnw -Dmaven.test.skip=true package

./mvnw -Dmaven.test.skip=true -pl examples -am package

./mvnw -Dmaven.test.skip=true -pl tools -am package



## Python

wget https://www.python.org/ftp/python/3.10.10/Python-3.10.10.tgz
tar xf Python-3.10.10.tgz
cd Python-3.10.10

On Centos7 see this post on how to enable ssl: https://stackoverflow.com/a/75114549
Otherwise the pip will not work!!

        sudo yum install -y epel
        sudo yum install -y openssl11-devel
        sed -i 's/PKG_CONFIG openssl /PKG_CONFIG openssl11 /g' configure

Also on Centos7:

        source /opt/rh/devtoolset-8/enable


./configure --prefix=PREFIX --enable-optimizations
make -j
make install

PREFIX/bin/python3 --version
Python 3.10.10

cd orc-tests
../tools/stage-python/bin/python3 -m venv venv
source venv/bin/activate

pip install numpy
pip install pyarrow
pip install requests
pip install pandas

python TEST.py
