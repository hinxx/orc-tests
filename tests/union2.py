import pyorc
from datetime import datetime


# the union has one column! We need to only write a single value of the CORRECT tyoe!
# the internal code infers the datatype of a row to determine which union logical column
# to update.
# see https://github.com/noirello/pyorc/issues/61

def test1(filename):
    fp = open(filename, "wb")

    # a double will NOT be treated as an integer therefore written to column 1
    # an integer gets written to column 0
    # OK
    writer1 = pyorc.Writer(fp, "struct<col1:uniontype<int,double>>")

    writer1.write((0,))
    writer1.write((1.0,))
    writer1.write((22.0,))
    writer1.write((33,))
    writer1.write((0,))
    writer1.write((1,))
    writer1.close()
    fp.close()

    # orc-contents pyorc-1.orc 
    # {"col1": {"tag": 0, "value": 0}}
    # {"col1": {"tag": 1, "value": 1}}
    # {"col1": {"tag": 1, "value": 22}}
    # {"col1": {"tag": 0, "value": 33}}
    # {"col1": {"tag": 0, "value": 0}}
    # {"col1": {"tag": 0, "value": 1}}


def test2(filename):
    fp = open(filename, "wb")

    # an integer will be infered as a double; we get no writes to int column
    # NOT OK
    writer1 = pyorc.Writer(fp, "struct<col1:uniontype<double,int>>")

    writer1.write((0,))
    writer1.write((1.0,))
    writer1.write((22.0,))
    writer1.write((33,))
    writer1.write((0,))
    writer1.write((1,))
    writer1.close()
    fp.close()

    # orc-contents pyorc-2.orc 
    # {"col1": {"tag": 0, "value": 0}}
    # {"col1": {"tag": 0, "value": 1}}
    # {"col1": {"tag": 0, "value": 22}}
    # {"col1": {"tag": 0, "value": 33}}
    # {"col1": {"tag": 0, "value": 0}}
    # {"col1": {"tag": 0, "value": 1}}


def test3(filename):
    fp = open(filename, "wb")

    # see test1
    # string is infered correctly
    # OK
    writer1 = pyorc.Writer(fp, "struct<col1:uniontype<int,double,string>>")

    writer1.write((0,))
    writer1.write((3.3,))
    writer1.write(('dodo',))
    writer1.write((1,))
    writer1.write(('emu',))
    writer1.close()
    fp.close()

    # orc-contents pyorc-3.orc 
    # {"col1": {"tag": 0, "value": 0}}
    # {"col1": {"tag": 1, "value": 3.3}}
    # {"col1": {"tag": 2, "value": "dodo"}}
    # {"col1": {"tag": 0, "value": 1}}
    # {"col1": {"tag": 2, "value": "emu"}}


def test4(filename):
    fp = open(filename, "wb")

    # see test3
    # binary string is infered correctly
    # OK
    writer1 = pyorc.Writer(fp, "struct<col1:uniontype<int,double,string,binary>>")

    writer1.write((0,))
    writer1.write((3.3,))
    writer1.write(('dodo',))
    writer1.write((1,))
    writer1.write((b'junk',))
    writer1.write((8,))
    writer1.close()
    fp.close()

    # orc-contents pyorc-4.orc 
    # {"col1": {"tag": 0, "value": 0}}
    # {"col1": {"tag": 1, "value": 3.3}}
    # {"col1": {"tag": 2, "value": "dodo"}}
    # {"col1": {"tag": 0, "value": 1}}
    # {"col1": {"tag": 3, "value": [106, 117, 110, 107]}}
    # {"col1": {"tag": 0, "value": 8}}


def test5(filename):
    fp = open(filename, "wb")

    # see test4
    # timestamp is infered correctly as is the extra string in the struct
    # OK
    writer1 = pyorc.Writer(fp, "struct<col1:string,col2:timestamp,col3:uniontype<int,double,string,binary>>")

    writer1.write(('row1', datetime.utcnow(), 0))
    writer1.write(('row2', datetime.utcnow(), 3.3))
    writer1.write(('row3', datetime.utcnow(), 'dodo'))
    writer1.write(('row4', datetime.utcnow(), b'junk'))
    writer1.close()
    fp.close()

    # orc-contents pyorc-5.orc 
    # {"col1": "row1", "col2": "2023-02-22 07:33:43.270087", "col3": {"tag": 0, "value": 0}}
    # {"col1": "row2", "col2": "2023-02-22 07:33:43.270121", "col3": {"tag": 1, "value": 3.3}}
    # {"col1": "row3", "col2": "2023-02-22 07:33:43.270229", "col3": {"tag": 2, "value": "dodo"}}
    # {"col1": "row4", "col2": "2023-02-22 07:33:43.270283", "col3": {"tag": 3, "value": [106, 117, 110, 107]}}


# test1('pyorc-1.orc')
# test2('pyorc-2.orc')
# test3('pyorc-3.orc')
# test4('pyorc-4.orc')
test5('pyorc-5.orc')
