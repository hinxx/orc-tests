ORC := ../install-1.8.2
LIBS := -lorc -llz4 -lprotobuf -lprotoc -lsnappy -lz -lzstd
LIBS += -pthread -lm

all: test1 test2 test3 test4 memtable

test1: test1.cpp
	g++ -I$(ORC)/include -Wall -O0 -ggdb3 test1.cpp -L$(ORC)/lib $(LIBS) -o test1

test2: test2.cpp
	g++ -I$(ORC)/include -Wall -O0 -ggdb3 test2.cpp -L$(ORC)/lib $(LIBS) -o test2

test3: test3.cpp
	g++ -Wall -O0 -ggdb3 test3.cpp -pthread -lm -o test3

test4: test4.cpp
	g++ -Wall -O0 -ggdb3 test4.cpp -pthread -lm -o test4

memtable: memtable.cpp
	g++ -Wall -O0 -ggdb3 memtable.cpp -pthread -lm -o memtable

clean:
	rm -f test1 test2 test3 test4

