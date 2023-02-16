ORC := ../install-1.8.2
LIBS := -lorc -llz4 -lprotobuf -lprotoc -lsnappy -lz -lzstd
LIBS += -pthread -lm

all: test1 test2

test1: test1.cpp
	g++ -I$(ORC)/include -Wall -O0 -ggdb3 test1.cpp -L$(ORC)/lib $(LIBS) -o test1

test2: test2.cpp
	g++ -I$(ORC)/include -Wall -O0 -ggdb3 test2.cpp -L$(ORC)/lib $(LIBS) -o test2

clean:
	rm -f test1 test2

