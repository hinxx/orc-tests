import pandas as pd
import pyarrow as pa
from pyarrow import orc
import json


names = pd.Series(['row1', 'row2', 'row3', 'row4', 'row5', 'row6'], dtype='string', name='name')
timestamps = pd.Series([pd.Timestamp('2022-07-15 12:40:13.439549952'), pd.Timestamp('2022-07-15 12:40:13.439546880'), pd.Timestamp('2023-02-08 09:13:32.287076352'), pd.Timestamp('2023-02-08 09:13:32.587076352'), pd.Timestamp('2022-07-07 14:23:10.092787968'), pd.Timestamp('2022-07-15 12:40:13.839546624')], dtype='datetime64[ns]', name='timestamp')
tags = pd.Series([1, 1, 0, 0, 2, 1], dtype='uint8', name='tag')
offsets = pd.Series([0, 1, 0, 1, 0, 2], dtype='uint32', name='offset')
integers = pd.Series([5, 53], dtype='int64', name='integer')
floats = pd.Series([0.011021, -32580.0, -33580.0], dtype='float64', name='float')
strings = pd.Series(['3.10.0'], dtype='string', name='string')

union_schema = pa.union([
    pa.field('int64', pa.int64()),
    pa.field('float64', pa.float64()),
    pa.field('string', pa.string())
    ], 'dense')
schema = pa.schema([
    ('name', pa.string()),
    ('timestamp', pa.timestamp('ns')),
    ('value', union_schema)
    ])
union = pa.UnionArray.from_dense(
    pa.array(tags, type='int8'),
    pa.array(offsets, type='int32'),
    [   pa.Array.from_pandas(integers),
        pa.Array.from_pandas(floats),
        pa.Array.from_pandas(strings)
    ],
    ['int64', 'float64', 'string']
    )
table = pa.Table.from_arrays([
    pa.Array.from_pandas(names),
    pa.Array.from_pandas(timestamps),
    union
    ], schema=schema)

print('table', table)
writer = orc.ORCWriter('union1.orc', dictionary_key_size_threshold=1)
writer.write(table)
