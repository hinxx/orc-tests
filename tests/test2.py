from time import sleep, perf_counter
from threading import Thread, Lock

import curses
import numpy as np
import pyarrow as pa
from pyarrow import orc
import pandas as pd
import json
import datetime
import os
import requests
import sys


# root data path
ROOT_PATH = './'
if len(sys.argv) > 1:
    ROOT_PATH = sys.argv[1]
if not os.path.exists(ROOT_PATH):
    os.makedirs(ROOT_PATH)
if not os.path.exists(ROOT_PATH+'aa-info'):
    os.makedirs(ROOT_PATH+'aa-info')

NUM_THREADS = 20
NUM_PV_PER_THREAD = 1000

threads = [None] * NUM_THREADS

orc_schema = pa.schema([
    ('pvname', pa.string()),
    ('timestamp', pa.timestamp('ns')),
    ('integer', pa.int64()),
    ('float', pa.float64()),
    ('string', pa.string())
])

stdscr = curses.initscr()



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
    local_info_file = ROOT_PATH+'aa-info/'+pvname+'.json'
    if not os.path.exists(local_info_file):
        params = {'pv': pvname}
        res = requests.get(url, params=params)
        # print(res.request.url)
        if res.status_code == requests.codes.ok:
            with open(local_info_file, 'w') as fp:
                json.dump(res.json(), fp, indent=2)
            return res.json()
    else:
        with open(local_info_file, 'r') as fp:
            return json.load(fp)
    return None


def get_dbr(pvname):
    infos = get_info(pvname)
    for info in infos:
        if info['name'] == 'Archiver DBR type (from typeinfo):':
            return info['value']
    return None


def task(work, id):
    # print('Starting the task %d: %s ... %s, size %d' % (id, work['start'], work['end'], len(work['payload'])))

    pvnames = pd.Series(dtype='string', name='pvname')
    timestamps = pd.Series(dtype='datetime64[ns]', name='timestamp')
    integers = pd.Series(dtype='Int64', name='integer')
    floats = pd.Series(dtype='float64', name='float')
    strings = pd.Series(dtype='string', name='string')

    for pvname in work['payload']:
        # print('pvname', pvname)
        work['pvname'] = pvname
        work['count'] += 1
        dbr = get_dbr(pvname)
        data = get_data(pvname, work['start'], work['end'] )
        if dbr and data and len(data) and 'data' in data[0] and len(data[0]['data']):
            if pvname in work['last']:
                # remove the duplicate (first) event; already in the previous request
                if work['last'][pvname] == data[0]['data'][0]:
                    del data[0]['data'][0]

            df = pd.DataFrame(data[0]['data'])
            num = len(df['val'])

            if dbr in  ['DBR_SCALAR_INT', 'DBR_SCALAR_SHORT', 'DBR_SCALAR_BYTE', 'DBR_SCALAR_ENUM']:
                integers = pd.concat([integers, df['val']], ignore_index=True)
                floats = pd.concat([floats, pd.Series(num * [None], dtype='float64')], ignore_index=True)
                strings = pd.concat([strings, pd.Series(num * [None], dtype='string')], ignore_index=True)
            elif dbr in ['DBR_SCALAR_DOUBLE', 'DBR_SCALAR_FLOAT']:
                floats = pd.concat([floats, df['val']], ignore_index=True)
                integers = pd.concat([integers, pd.Series(num * [None], dtype='Int64')], ignore_index=True)
                strings = pd.concat([strings, pd.Series(num * [None], dtype='string')], ignore_index=True)
            elif dbr in ['DBR_SCALAR_STRING']:
                strings = pd.concat([strings, df['val']], ignore_index=True)
                integers = pd.concat([integers, pd.Series(num * [None], dtype='Int64')], ignore_index=True)
                floats = pd.concat([floats, pd.Series(num * [None], dtype='float64')], ignore_index=True)
            else:
                # print('unhandled DBR:', dbr)
                continue

            pvnames = pd.concat([pvnames, pd.Series(num * [pvname], dtype='string')], ignore_index=True)
            timestamps = pd.concat([timestamps, pd.to_datetime(df["secs"] + df["nanos"] * 1e-9, unit="s")], ignore_index=True)

            # save the last event; to check for duplicate in next request
            work['last'] = data[0]['data'][-1]

    work['result'] = pd.DataFrame({
        'pvname': pvnames,
        'timestamp': timestamps,
        'integer': integers,
        'float': floats,
        'string': strings
    })

    # print(f'The task {id} completed')
    work['done'] = True


def mon_task(work):
    run = True
    while run:
        done = 0
        for n in range(len(work)):
            stdscr.addstr(n, 0, '%d: %10d  %s\n' % (n, work[n]['count'], work[n]['pvname']))
            if work[n]['done']:
                done += 1
        stdscr.refresh()

        if done >= len(work):
            run = False
        else:
            sleep(1)

def main():
    jdata = None
    with open('PBI-archiver-01.json') as fp:
        jdata = json.load(fp)
    # print('handling %d pvs' % len(jdata))

    start_time = perf_counter()

    # start_date = '2021-01-01'
    # end_date = '2023-03-01'
    start_date = '2022-07-11'
    end_date = '2022-07-12'

    total = len(jdata)
    # total = 100

    # Clear screen
    stdscr.clear()

    writer = orc.ORCWriter('demo2.orc', dictionary_key_size_threshold=1)

    last = {}
    dt = datetime.datetime.fromisoformat(start_date)
    end_dt = datetime.datetime.fromisoformat(end_date)
    work = [None] * NUM_THREADS
    # go over the desired timeframe
    while dt < end_dt:
        range_start = dt
        range_end = dt + datetime.timedelta(hours=1)
        # range_end = dt + datetime.timedelta(days=1)

        # go over all the pv names and start N threads
        offset = 0
        while offset < total:
            threads = [None] * NUM_THREADS
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
                    'pvname': None,
                    'count': 0,
                    'done': False
                }
                work[n] = w

                threads[n] = Thread(target=task, args=(w, n))
                threads[n].start()

                if offset == total:
                    break

            mon_thread = Thread(target=mon_task, args=(work, ))
            mon_thread.start()

            # wait for the threads to complete
            for n in range(len(threads)):
                if threads[n] is not None:
                    threads[n].join()

            mon_thread.join()

            # handle the results
            dfs = []
            for n in range(len(threads)):
                if threads[n] is not None:
                    # print(n, 'results, shape', work[n]['result'].shape)
                    # print(work[n]['result'])
                    dfs.append(work[n]['result'])

            df = pd.concat(dfs, ignore_index=True)
            df = df.set_index('timestamp')

            table = pa.table(df, schema=orc_schema)
            table.sort_by('timestamp')

            writer.write(table)

        dt = range_end

    end_time = perf_counter()

    # print(f'It took {end_time- start_time: 0.2f} second(s) to complete.')


main()
curses.endwin()
