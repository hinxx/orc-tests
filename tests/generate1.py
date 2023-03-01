from time import sleep, perf_counter
import json
import pandas as pd
import random
import os
from multiprocessing import Process
from datetime import datetime, timedelta
import pyarrow as pa
from pyarrow import parquet
import argparse
import itertools


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

prefix_file = 'ess-prefixes.txt'
param_file = 'ess-params.txt'

def generate_pv_names(filename):
    if os.path.exists(filename):
        print('filename', filename, 'exists')
        with open(filename, 'r') as fp:
            return json.load(fp)
        return False

    # take ess parameters
    # first line in the file is CSV header: 'a'
    raw = pd.read_csv(param_file)
    # get the words that are longer than 4
    mask = (raw['a'].str.len() > 3)
    raw = raw.loc[mask]
    params_all = raw['a'].to_list()

    # take ess prefixes
    raw = pd.read_csv(prefix_file)
    # get the words that are longer than 8
    mask = (raw['a'].str.len() > 12)
    raw = raw.loc[mask]
    prefixes_all = raw['a'].to_list()

    random.shuffle(prefixes_all)
    random.shuffle(params_all)

    # take 50 prefixes; system names
    prefixes = random.sample(prefixes_all, k=50)
    # take 1000 parameters; pv attributes
    params = random.sample(params_all, k=1000)

    prefix_series = pd.Series(prefixes)
    param_series = pd.Series(params)

    pvs = [r[0] + ':' + r[1] for r in itertools.product(prefixes, params)]
    random.shuffle(pvs)

    # final list of pvs
    pv_series = pd.Series(pvs, dtype='str')
    if pv_series.value_counts().max() == 1:
        with open(filename, 'w') as fp:
            json.dump(pvs, fp, indent=2)

        print('filename', filename, 'created')
        return pvs
    else:
        print('pv_series value count max is not 1 !!!')
        print('pv_series counts:\n', pv_series.value_counts())
        return None


def task1(work, id):
    print('worker %d starting..' % (id))

    ts = work['ts']
    num = len(work['pvs'])
    filename = work['filestub'] % id
    res = work['result']

    writer = parquet.ParquetWriter(filename, schema=work['schema'], version='2.6')
    pvnames = []

    num_events = 0
    row_count = num * work['event_count']
    num_rows = 0
    batch_max_size = work['batch_size']
    # print('orig batch_max_size', batch_max_size)
    if batch_max_size > row_count:
        batch_max_size = row_count
    # print('1 batch_max_size', batch_max_size)
    # work in batches that are multiples of number of pvs
    if (batch_max_size % num) != 0:
        batch_max_size = int((batch_max_size / num)) * num
    # print('2 batch_max_size', batch_max_size)

    start_time = perf_counter()

    res['integer_range'] = [int(datetime.timestamp(ts)*1e6)]
    res['timestamp_range'] = [str(ts)]

    # generate desired amount of rows
    while num_rows < row_count:
        batch_size = min(batch_max_size, row_count - num_rows)
        batch_rows = 0
        timestamps = []
        integers = []
        # handle rows in batches that do not clog the system
        while batch_rows < batch_size:
            timestamps += num * [ts]
            tss = int(datetime.timestamp(ts)*1e6)
            integers += [tss + n for n in range(num)]

            # next event / time slice
            ts = ts + timedelta(milliseconds=72)
            num_events += 1
            batch_rows += num
            num_rows += num

        print('\rworker %2d %15s %15d / %-15d events %15d / %-15d rows' % (id, 'WRITING', num_events, work['event_count'], num_rows, row_count), end='')
        pvnames = int(batch_rows / num) * work['pvs']
        table = pa.table([pvnames, timestamps, integers], schema=work['schema'])
        # table.sort_by('timestamp')
        writer.write(table)

    res['integer_range'].append(integers[-1])
    res['timestamp_range'].append(str(timestamps[-1]))

    print('\rworker %2d %15s %15d / %-15d events %15d / %-15d rows' % (id, 'DONE', num_events, work['event_count'], num_rows, row_count))
    end_time = perf_counter()
    print('worker %d took %.2f second(s) to complete.' % (id, end_time - start_time))

    writer.close()


def work1(pvs, args):
    start_date = '2023-02-11'
    work = [None] * args.workers
    workers = [None] * args.workers

    schema = pa.schema([
        ('pvname', pa.string()),
        # ('part1', pa.string()),
        ('timestamp', pa.timestamp('ns')),
        ('integer', pa.int64()),
        # ('float', pa.float64()),
        # ('string', pa.string()),
        # ('binary', pa.binary())
    ])

    if args.pv_sort:
        pvs.sort()

    path = args.path + '/' + args.run_name

    # start the workers
    for n in range(len(workers)):
        s = n * args.pv_count
        e = (n + 1) * args.pv_count
        w = {
            'id': n,
            'ts': datetime.fromisoformat(start_date),
            'pvs': pvs[s:e],
            'filestub': path + '/' + args.work + '-%d.parquet',
            'schema': schema,
            'event_count': args.events,
            'batch_size': args.batch_size,
            'result': {
                'timestamp_range': None,
                'integer_range': None
            }
        }
        work[n] = w
        workers[n] = Process(target=task1, args=(w, n))
        workers[n].start()

    # wait for the workers to complete
    for n in range(len(workers)):
        workers[n].join()

    # save the work parameters and results in a single json file
    results = {
        'schema': schema.to_string(),
        'args': str(args),
        'data': []
    }
    for w in work:
        if w is not None:
            r = {
                'id': w['id'],
                'filename': w['filestub'] % w['id'],
                'pvs': w['pvs'],
                'timestamp_range': w['result']['timestamp_range'],
                'integer_range': w['result']['integer_range']
            }
            results['data'].append(r)
    with open(path + '/' + args.work + '-report.json', 'w') as fp:
        json.dump(results, fp, indent=2)


################################################################################################

parser = argparse.ArgumentParser()
parser.add_argument('work', action='store')
parser.add_argument('-b', '--batch_size', action='store', default='1000000', type=int)
parser.add_argument('-p', '--pv_count', action='store', default='200', type=int)
parser.add_argument('-w', '--workers', action='store', default='1', type=int)
parser.add_argument('-e', '--events', action='store', default='10000', type=int)
parser.add_argument('-P', '--path', action='store', default='pq-data', type=str)
parser.add_argument('-s', '--pv_sort', action="store_true", default=False)
parser.add_argument('-n', '--run_name', action='store', default='', type=str)
args = parser.parse_args()
print('args:', args)

if args.run_name == '':
    now = datetime.now()
    args.run_name = now.strftime('%Y-%m-%d-%H-%M-%S')

path = args.path + '/' + args.run_name
if not os.path.exists(path):
    os.makedirs(path)

pvs = generate_pv_names('fake-pvs.json')
if pvs is None:
    exit(1)

print('have %d fake PVs..' % len(pvs))

if args.work == 'work1':
    work1(pvs, args)
else:
    print('work function %s not found!' % args.work)
