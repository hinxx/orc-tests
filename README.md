# orc tests

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
