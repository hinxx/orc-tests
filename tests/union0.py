import pandas as pd
import pyarrow as pa
from pyarrow import orc
import json


TAG_INT = 0
TAG_FLOAT = 1
TAG_STRING = 2


class MyData(object):
    def __init__(self):
        self.pvnames = pd.Series(dtype='string', name='pvname')
        self.timestamps = pd.Series(dtype='datetime64[ns]', name='timestamp')
        # self.tags = pd.Series(dtype='uint8', name='tag')
        # self.offsets = pd.Series(dtype='uint32', name='offset')
        self.integers = pd.Series(dtype='int32', name='integer')
        self.integers_offset = 0
        self.floats = pd.Series(dtype='float64', name='float')
        self.floats_offset = 0
        self.strings = pd.Series(dtype='string', name='string')
        self.strings_offset = 0

        # self.pvnames = []
        # self.timestamps = []
        self.tags = []
        self.offsets = []
        # self.integers = []
        # self.integers_offset = 0
        # self.floats = []
        # self.floats_offset = 0
        # self.strings = []
        # self.strings_offset = 0


    def append(self, pvname, jdata):
        df = pd.DataFrame(jdata[0]['data'])
        df['timestamp'] = pd.to_datetime(df['secs'] + df['nanos'] * 1e-9, unit='s')
        df = df[['timestamp', 'val']]
        # df['pvname'] = jdata[0]['meta']['name']

        num = len(df['val'])
        print('pvname', pvname)
        print('num of values', num)
        # print('data type', df['val'].dtypes)
        # print('data type inferred', df['val'].infer_objects().dtypes)
        if (df['val'].dtypes == object):
            # try to convert to string only!
            df['val'] = df['val'].convert_dtypes(infer_objects=True, convert_string=True, convert_integer=False, convert_boolean=False, convert_floating=False)
            # print('data type converted', df['val'].dtypes)
        print('data', df['val'])
        # print('timestamp', df['timestamp'])
        print('df', df)

        self.pvnames = pd.concat([self.pvnames, pd.Series(num * [pvname], dtype='string')], ignore_index=True)
        self.timestamps = pd.concat([self.timestamps, df['timestamp']], ignore_index=True)
        values = df['val'].to_list()
        if df['val'].dtypes == 'int64':
            self.integers = pd.concat([self.integers, df['val']], ignore_index=True)
            self.tags += num * [TAG_INT]
            self.offsets += list(range(self.integers_offset, self.integers_offset + num))
            self.integers_offset += num
        elif df['val'].dtypes == 'float64':
            self.floats = pd.concat([self.floats, df['val']], ignore_index=True)
            self.tags += num * [TAG_FLOAT]
            self.offsets += list(range(self.floats_offset, self.floats_offset + num))
            self.floats_offset += num
        elif df['val'].dtypes == 'string':
            self.strings = pd.concat([self.strings, df['val']], ignore_index=True)
            self.tags += num * [TAG_STRING]
            self.offsets += list(range(self.strings_offset, self.strings_offset + num))
            self.strings_offset += num

    def flush(self):
        union_schema = pa.union([
            pa.field('int64', pa.int64()),
            pa.field('float64', pa.float64()),
            pa.field('string', pa.string())
            ], 'dense')
        schema = pa.schema([
            ('pvname', pa.string()),
            ('timestamp', pa.timestamp('ns')),
            ('value', union_schema)
            ])
        union = pa.UnionArray.from_dense(
            pa.array(self.tags, type='int8'),
            pa.array(self.offsets, type='int32'),
            [pa.Array.from_pandas(self.integers), pa.Array.from_pandas(self.floats), pa.Array.from_pandas(self.strings)],
            ['int64', 'float64', 'string']
            )
        table = pa.Table.from_arrays([
            pa.Array.from_pandas(self.pvnames),
            pa.Array.from_pandas(self.timestamps),
            union
            ], schema=schema)

        print('timestamps', self.timestamps)
        print('table', table)
        writer = orc.ORCWriter('demo1.orc', dictionary_key_size_threshold=1)
        writer.write(table)


mydata = MyData()

pvname1 = 'DTL-010:PBI-FC-001:PROC1-Scale'
test1 = '''
[ 
{ "meta": { "name": "DTL-010:PBI-FC-001:PROC1-Scale" , "PREC": "6" },
"data": [ 
{ "secs": 1657888813, "val": 0.011021, "nanos": 439549854, "severity":0, "status":0, "fields": { "cnxlostepsecs": "1657887747","cnxregainedepsecs": "1657888819"} }
] }
 ]
'''

pvname2 = 'DTL-010:PBI-FC-001:PROC1-Offset'
test2 = '''
[ 
{ "meta": { "name": "DTL-010:PBI-FC-001:PROC1-Offset" , "PREC": "2" },
"data": [ 
{ "secs": 1657888813, "val": -32580.0, "nanos": 439546767, "severity":0, "status":0, "fields": { "cnxlostepsecs": "1657887747","cnxregainedepsecs": "1657888819"} }
] }
 ]
'''

pvname3 = 'PBI-BPM01:Ctrl-AMC-120:DIFF-A-RoiFirstSampleR'
test3 = '''
[ 
{ "meta": { "name": "PBI-BPM01:Ctrl-AMC-120:DIFF-A-RoiFirstSampleR" , "PREC": "0" },
"data": [ 
{ "secs": 1675847612, "val": 5, "nanos": 287076386, "severity":0, "status":0 },
{ "secs": 1675847612, "val": 53, "nanos": 587076386, "severity":0, "status":0 }
] }
 ]
'''

pvname4 = 'PBI-BPM01:Ctrl-AMC-110:ADCoreVersion_RBV'
test4 = '''
[ 
{ "meta": { "name": "PBI-BPM01:Ctrl-AMC-110:ADCoreVersion_RBV" , "PREC": "0" },
"data": [ 
{ "secs": 1657203790, "val": "3.10.0", "nanos": 92787912, "severity":0, "status":0, "fields": { "cnxlostepsecs": "1657203784","DESC": "","cnxregainedepsecs": "1657203792"} }
] }
 ]
'''

pvname5 = 'DTL-010:PBI-FC-001:PROC1-Offset'
test5 = '''
[ 
{ "meta": { "name": "DTL-010:PBI-FC-001:PROC1-Offset" , "PREC": "2" },
"data": [ 
{ "secs": 1657888813, "val": -33580.0, "nanos": 839546767, "severity":0, "status":0, "fields": { "cnxlostepsecs": "1657887747","cnxregainedepsecs": "1657888819"} }
] }
 ]
'''

mydata.append(pvname1, json.loads(test1))
mydata.append(pvname2, json.loads(test2))
mydata.append(pvname3, json.loads(test3))
mydata.append(pvname4, json.loads(test4))
mydata.append(pvname5, json.loads(test5))

print(mydata.pvnames.to_list())
print(mydata.timestamps.to_list())
print('mydata.offsets', mydata.offsets)
print('mydata.tags', mydata.tags)
print(mydata.integers.to_list())
print(mydata.integers_offset)
print(mydata.floats.to_list())
print(mydata.floats_offset)
print(mydata.strings.to_list())
print(mydata.strings_offset)

mydata.flush()
'''
table pyarrow.Table
pvname: string
timestamp: timestamp[ns]
value: dense_union<int64: int64=0, float64: double=1, string: string=2>
  child 0, int64: int64
  child 1, float64: double
  child 2, string: string
----
pvname: [["DTL-010:PBI-FC-001:PROC1-Scale","DTL-010:PBI-FC-001:PROC1-Offset","PBI-BPM01:Ctrl-AMC-120:DIFF-A-RoiFirstSampleR","PBI-BPM01:Ctrl-AMC-120:DIFF-A-RoiFirstSampleR","PBI-BPM01:Ctrl-AMC-110:ADCoreVersion_RBV","DTL-010:PBI-FC-001:PROC1-Offset"]]
timestamp: [[2022-07-15 12:40:13.439549952,2022-07-15 12:40:13.439546880,2023-02-08 09:13:32.287076352,2023-02-08 09:13:32.587076352,2022-07-07 14:23:10.092787968,2022-07-15 12:40:13.839546624]]
value: [  -- is_valid: all not null  -- type_ids: [1,1,0,0,2,1]  -- value_offsets: [0,1,0,1,0,2]
  -- child 0 type: int64
[5,53]
  -- child 1 type: double
[0.011021,-32580,-33580]
  -- child 2 type: string
["3.10.0"]]
Traceback (most recent call last):
  File "pd_orc.py", line 173, in <module>
    mydata.flush()
  File "pd_orc.py", line 100, in flush
    writer.write(table)
  File "/data/data/Code/orc/python/venv/lib/python3.8/site-packages/pyarrow/orc.py", line 289, in write
    self.writer.write(table)
  File "pyarrow/_orc.pyx", line 443, in pyarrow._orc.ORCWriter.write
  File "pyarrow/error.pxi", line 121, in pyarrow.lib.check_status
pyarrow.lib.ArrowNotImplementedError: Unknown or unsupported Arrow type: dense_union<int64: int64=0, float64: double=1, string: string=2>
'''
