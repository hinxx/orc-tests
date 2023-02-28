from time import sleep, perf_counter
import json
import pandas as pd
import random
import os
from threading import Thread
from datetime import datetime, timedelta
import pyarrow as pa
from pyarrow import parquet
import argparse


# This script will generate parquet files containing defined amount
# of data with varying:
#  - pv names
#  - time range
#  - number of rows
#  - file size
#
# These files will then be used with the processing frameworks.
# Goal is to a) evaluate these frameworks and b) determine best
# practices for file contents/sizes/..

# wget https://www.mit.edu/~ecprice/wordlist.10000
wordlist = 'wordlist.10000'
ess_wordlist = 'ess-prefixes.txt'


def generate_pv_names(filename):
    if os.path.exists(filename):
        print('filename', filename, 'exists')
        with open(filename, 'r') as fp:
            return json.load(fp)
        return False

    # take some random words to be used as pv parameters
    # first line in the file is CSV header: 'a'
    raw = pd.read_csv(wordlist)
    # get the words that are between 4 and 10 chars long
    mask = (raw['a'].str.len() > 3) & (raw['a'].str.len() < 10)
    raw = raw.loc[mask]
    params_all = raw['a'].to_list()

    # take ess prefixes
    raw = pd.read_csv(ess_wordlist)
    # get the words that are longer than 8
    mask = (raw['a'].str.len() > 8)
    raw = raw.loc[mask]
    prefixes_all = raw['a'].to_list()

    random.shuffle(prefixes_all)
    random.shuffle(params_all)

    # take 50 prefixes; system names
    prefixes = random.choices(prefixes_all, k=50)
    # take 1000 parameters; pv attributes
    params = random.choices(params_all, k=1000)

    prefix_series = pd.Series(prefixes)
    pv_series = pd.Series([], dtype='str')
    for i in range(0, 1000):
        param_series = pd.Series(random.choices(params, k=50))
        pv_series = pd.concat([pv_series, prefix_series + ':' + param_series], ignore_index=True)

    # print(pv_series)
    # print('len:', len(pv_series))

    with open(filename, 'w') as fp:
        json.dump(pv_series.to_list(), fp, indent=2)

    print('filename', filename, 'created')
    return pv_series.to_list()


def task1(work, id):
    print('thread %d starting..' % (id))

    ts = work['ts']
    num = len(work['pvs'])
    filename = work['filestub'] % id
    final_rows = work['rows_per_day'] * work['days']

    writer = parquet.ParquetWriter(filename, schema=work['schema'], version='2.6')
    pvnames = []
    timestamps = []
    integers = []
    
    total_rows = 0
    day_rows = 0
    start_time = perf_counter()
    while True:
        timestamps += num * [ts]
        tss = int(datetime.timestamp(ts)*1e6)
        integers += [tss + n for n in range(num)]

        day_rows += num
        total_rows += num

        # next time slice
        ts = ts + timedelta(milliseconds=72)

        # print('\rthread %d %d / %d' % (id, total_rows, final_rows), end='')

        if day_rows >= work['rows_per_day']:
            print('\rthread %d WRITING %d / %d' % (id, total_rows, final_rows))
            pvnames = int(day_rows / num) * work['pvs']
            table = pa.table([pvnames, timestamps, integers], schema=work['schema'])
            # table.sort_by('timestamp')
            writer.write(table)
            timestamps = []
            integers = []
            day_rows = 0

        if total_rows >= final_rows:
            break

    print('\rthread %d DONE %d / %d' % (id, total_rows, final_rows))
    end_time = perf_counter()
    print('thread %d took %.2f second(s) to complete.' % (id, end_time - start_time))

    writer.close()


def work1(pvs, nthreads=1, npvs=200, ndays=1):
    work_name = 'work1'
    start_date = '2023-02-11'
    work = [None] * nthreads
    threads = [None] * nthreads

    schema = pa.schema([
        ('pvname', pa.string()),
        # ('part1', pa.string()),
        ('timestamp', pa.timestamp('ns')),
        ('integer', pa.int64()),
        # ('float', pa.float64()),
        # ('string', pa.string()),
        # ('binary', pa.binary())
    ])

    # start the threads
    for n in range(len(threads)):
        s = n * npvs
        e = (n + 1) * npvs
        w = {
            'ts': datetime.fromisoformat(start_date),
            'pvs': pvs[s:e],
            'filestub': work_name+'-%d.parquet',
            'schema': schema,
            # 14 * 60 * 60 * 24 = 1209600 entries in a day @ 14 Hz
            'rows_per_day': 14 * 60 * 60 * 24,
            'days': ndays
        }
        work[n] = w
        threads[n] = Thread(target=task1, args=(w, n))
        threads[n].start()

    # wait for the threads to complete
    for n in range(len(threads)):
        threads[n].join()


################################################################################################

parser = argparse.ArgumentParser()
parser.add_argument('work', action='store')
parser.add_argument('-d', '--days', action='store', default='1', type=int)
parser.add_argument('-p', '--pv_count', action='store', default='200', type=int)
parser.add_argument('-t', '--threads', action='store', default='1', type=int)
args = parser.parse_args()
print('args:', args)

pvs = generate_pv_names('fake-pvs.json')
print('have %d fake PVs..' % len(pvs))
if args.work == 'work1':
    work1(pvs, nthreads=args.threads, npvs=args.pv_count, ndays=args.days)
else:
    print('work function %s not found!' % args.work)
