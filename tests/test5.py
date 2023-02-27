from time import sleep, perf_counter
from threading import Thread, Lock

import numpy as np
import pyarrow as pa
from pyarrow import parquet
import pandas as pd
import json
import datetime
import os
import requests
from array import array


# Will get the data and info for a list of pvs from the archiver and 
# write a parquet file using a schema without an union.
# Using pandas only to extract initial data from json.
# Not using pandas for building the pyarrow table because I thought
# there was an issue with pandas binary type support (seems not as per
# https://github.com/pandas-dev/pandas/issues/51526); building of the
# arrays for the pyarrow table is done using python lists.
# Waveforms (only byte sized type) are supported by converting
# the list of values to a binary string and then saving that
# as binary column.


NUM_THREADS = 1
NUM_PV_PER_THREAD = 10

threads = [None] * NUM_THREADS

schema = pa.schema([
    ('pvname', pa.string()),
    ('part1', pa.string()),
    ('timestamp', pa.timestamp('ns')),
    ('integer', pa.int64()),
    ('float', pa.float64()),
    ('string', pa.string()),
    ('binary', pa.binary())
])



def get_data(pvname, start, end):
    archiver = 'archiver-01'
    url_stub = 'http://%s.tn.esss.lu.se:17668/retrieval/data/getData.json'
    url = url_stub % archiver
    params = {'pv': pvname,
            'from': start.isoformat() + 'Z',
            'to': end.isoformat() + 'Z'}
    res = requests.get(url, params=params)
    # print(res.request.url)
    if res.status_code == requests.codes.ok:
        # print('sucess', url)
        return res.json()
    return None


def get_info(pvname):
    '''
    obtain PV info from the archiver and save it to local file
    if the local file exists skip download
    '''
    archiver = 'archiver-01'
    url_stub = 'http://%s.tn.esss.lu.se:17665/mgmt/bpl/getPVDetails'

    url = url_stub % archiver
    local_info_file = './aa-info/'+pvname+'.json'
    if not os.path.exists(local_info_file):
        params = {'pv': pvname}
        res = requests.get(url, params=params)
        if res.status_code == requests.codes.ok:
            with open(local_info_file, 'w') as fp:
                json.dump(res.json(), fp, indent=2)
            return res.json()
    else:
        with open(local_info_file, 'r') as fp:
            return json.load(fp)
    return []


def get_dbr(pvname):
    infos = get_info(pvname)
    for info in infos:
        if info['name'] == 'Archiver DBR type (from typeinfo):':
            return info['value']
    return None


def task(work, id):
    print('Starting the task %d: %s ... %s, size %d' % (id, work['start'], work['end'], len(work['payload'])))

    # pvnames = pd.Series(dtype='string', name='pvname')
    # timestamps = pd.Series(dtype='datetime64[ns]', name='timestamp')
    # integers = pd.Series(dtype='Int64', name='integer')
    # floats = pd.Series(dtype='float64', name='float')
    # strings = pd.Series(dtype='string', name='string')
    # dtype 'binary[pyarrow]' obtained by: pd.ArrowDtype(pa.binary())
    # binaries = pd.Series(dtype='binary[pyarrow]', name='binary')

    pvnames = []
    partition1 = []
    timestamps = []
    integers = []
    floats = []
    strings = []
    binaries = []

    for pvname in work['payload']:
        # print('pvname', pvname)
        dbr = get_dbr(pvname)
        data = get_data(pvname, work['start'], work['end'] )
        if dbr and data and len(data) and 'data' in data[0] and len(data[0]['data']):
            print('pvname', pvname, 'have dbr & data')
            if pvname in work['last']:
                # remove the duplicate (first) event; already in the previous request
                if work['last'][pvname] == data[0]['data'][0]:
                    del data[0]['data'][0]

            df = pd.DataFrame(data[0]['data'])
            num = len(df['val'])

            if dbr in  ['DBR_SCALAR_INT', 'DBR_SCALAR_SHORT', 'DBR_SCALAR_BYTE', 'DBR_SCALAR_ENUM']:
                # integers = pd.concat([integers, df['val']], ignore_index=True)
                # floats = pd.concat([floats, pd.Series(num * [None], dtype='float64')], ignore_index=True)
                # strings = pd.concat([strings, pd.Series(num * [None], dtype='string')], ignore_index=True)
                # binaries = pd.concat([binaries, pd.Series(num * [None], dtype='binary[pyarrow]')], ignore_index=True)
                integers += df['val'].to_list()
                floats += num * [None]
                strings += num * [None]
                binaries += num * [None]
            elif dbr in ['DBR_SCALAR_DOUBLE', 'DBR_SCALAR_FLOAT']:
                # floats = pd.concat([floats, df['val']], ignore_index=True)
                # integers = pd.concat([integers, pd.Series(num * [None], dtype='Int64')], ignore_index=True)
                # strings = pd.concat([strings, pd.Series(num * [None], dtype='string')], ignore_index=True)
                # binaries = pd.concat([binaries, pd.Series(num * [None], dtype='binary[pyarrow]')], ignore_index=True)
                floats += df['val'].to_list()
                integers += num * [None]
                strings += num * [None]
                binaries += num * [None]
            elif dbr == 'DBR_SCALAR_STRING':
                # strings = pd.concat([strings, df['val']], ignore_index=True)
                # integers = pd.concat([integers, pd.Series(num * [None], dtype='Int64')], ignore_index=True)
                # floats = pd.concat([floats, pd.Series(num * [None], dtype='float64')], ignore_index=True)
                # binaries = pd.concat([binaries, pd.Series(num * [None], dtype='binary[pyarrow]')], ignore_index=True)
                strings += df['val'].to_list()
                integers += num * [None]
                floats += num * [None]
                binaries += num * [None]
            elif dbr == 'DBR_WAVEFORM_BYTE':
                # integers = pd.concat([integers, pd.Series(num * [None], dtype='Int64')], ignore_index=True)
                # floats = pd.concat([floats, pd.Series(num * [None], dtype='float64')], ignore_index=True)
                # strings = pd.concat([strings, pd.Series(num * [None], dtype='string')], ignore_index=True)
                # binary = memoryview(ar.array('B', df['val'].to_list()[0])).tobytes()
                # convert python list to a bytes() object that pyarrow likes for binary data type
                for item in df['val'].to_list():
                    binaries += [memoryview(array('B', item)).tobytes()]
                integers += num * [None]
                floats += num * [None]
                strings += num * [None]
            else:
                print('unhandled DBR:', dbr)
                continue

            # pvnames = pd.concat([pvnames, pd.Series(num * [pvname], dtype='string')], ignore_index=True)
            # timestamps = pd.concat([timestamps, pd.to_datetime(df["secs"] + df["nanos"] * 1e-9, unit="s")], ignore_index=True)
            
            pvnames += num * [pvname]
            partition1 += num * [pvname.split(':')[0]]
            # data type will be pandas Timestamp()
            timestamps += pd.to_datetime(df["secs"] + df["nanos"] * 1e-9, unit="s").to_list()

            # save the last event; to check for duplicate in next request
            work['last'][pvname] = data[0]['data'][-1]
        else:
            print('pvname', pvname, 'NOT HANDLED!! DBR:', dbr, 'DATA:', data)


    # work['result'] = pd.DataFrame({
    #     'pvname': pvnames,
    #     'timestamp': timestamps,
    #     'integer': integers,
    #     'float': floats,
    #     'string': strings,
    #     'binary': binaries
    # })

    work['result'] = pa.table([pvnames, partition1, timestamps, integers, floats, strings, binaries], schema=schema)
    # if nulls are not there .. do this?!
    # work['result'] = pa.table([
    #     pa.array(pvnames, type=pa.string(), from_pandas=True),
    #     pa.array(timestamps, type=pa.timestamp('ns'), from_pandas=True),
    #     pa.array(integers, type=pa.int64(), from_pandas=True),
    #     pa.array(floats, type=pa.float64(), from_pandas=True),
    #     pa.array(strings, type=pa.string(), from_pandas=True),
    #     pa.array(binaries, type=pa.binary(), from_pandas=True)], schema=orc_schema)

    print(f'The task {id} completed')


jdata = None
with open('PBI-archiver-01-subset.json') as fp:
    jdata = json.load(fp)
print('handling %d pvs' % len(jdata))


start_time = perf_counter()

# start_date = '2021-01-01'
# end_date = '2023-03-01'
start_date = '2022-07-11'
end_date = '2022-07-12'

# total = len(jdata)
total = 100

writer = parquet.ParquetWriter('demo5.parquet', schema=schema, version='2.6')

last = {}
dt = datetime.datetime.fromisoformat(start_date)
end_dt = datetime.datetime.fromisoformat(end_date)
# work = [None] * NUM_THREADS
# go over the desired timeframe
while dt < end_dt:
    range_start = dt
    # range_end = dt + datetime.timedelta(hours=1)
    range_end = dt + datetime.timedelta(days=1)

    # go over all the pv names and start N threads
    offset = 0
    while offset < total:
        threads = [None] * NUM_THREADS
        work = [None] * NUM_THREADS

        for n in range(len(threads)):
            start = offset
            size = min(NUM_PV_PER_THREAD, total - offset)
            payload = jdata[start:start+size]
            offset = start + size

            w = {
                'start': range_start,
                'end': range_end,
                'payload': payload,
                'last': last,
                'result': None,
            }
            work[n] = w

            threads[n] = Thread(target=task, args=(w, n))
            threads[n].start()

            if offset == total:
                break

        # wait for the threads to complete
        for n in range(len(threads)):
            if threads[n] is not None:
                threads[n].join()

        # handle the results
        table_n = []
        for n in range(len(threads)):
            if threads[n] is not None:
                # print('handling:', n, threads[n], work[n])
                # print(n, 'results, shape', work[n]['result'].shape)
                print('RESULT:\n', work[n]['result'])
                table_n.append(work[n]['result'])

        # df = pd.concat(dfs, ignore_index=True)
        # df = df.set_index('timestamp')

        # table = pa.table(df, schema=orc_schema)
        table = pa.concat_tables(table_n)
        table.sort_by('timestamp')

        # writer.write(table)

        # Local dataset write
        parquet.write_to_dataset(table, root_path='dataset_name', partition_cols=['part1'], version='2.6')

        print('\nLOOP\n')

    dt = range_end

end_time = perf_counter()

print(f'It took {end_time- start_time: 0.2f} second(s) to complete.')
