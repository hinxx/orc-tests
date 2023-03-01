from time import sleep, perf_counter
import json
import pandas as pd
import random
import os
from multiprocessing import Process, Manager
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



class Generator(object):
    def __init__(self):
        self.args = None

    def setup_(self, args):
        self.args = args
        self.all_pvs = generate_pv_names('fake-pvs.json')
        if args.pv_sort:
            self.all_pvs.sort()

        self.initial_timestamp = datetime.fromisoformat('2023-02-11')
        
        self.schema = None

        self.pv_count = args.pv_count
        self.event_count = args.event_count
        self.row_count = self.pv_count * self.event_count
        self.batch_size = args.batch_size
        # print('orig self.batch_size', self.batch_size)
        if self.batch_size > self.row_count:
            self.batch_size = self.row_count
        # print('1 self.batch_size', self.batch_size)
        # work in batches that are multiples of number of pvs
        if (self.batch_size % self.pv_count) != 0:
            self.batch_size = int((self.batch_size / self.pv_count)) * self.pv_count
        # print('final self.batch_size', self.batch_size)

        self.worker_count = args.worker_count
        self.workers = []

        self.task_name = args.work
        self.run_name = args.run_name
        if self.run_name == '':
            now = datetime.now()
            args.run_name = now.strftime('%Y-%m-%d-%H-%M-%S')

        self.path = args.path + '/' + args.run_name
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        self.filestub = self.path + '/' + self.task_name + '-%d.parquet'

    def start(self):
        # use manager to get back some results
        with Manager() as manager:
            # start the workers
            for n in range(self.worker_count):
                result = manager.dict()
                result['done'] = False
                worker = {
                    'id': n,
                    'result': result,
                    'process': Process(target=self.task, args=(n, result))
                }
                self.workers.append(worker)
                worker['process'].start()

            # wait for the workers to complete
            for worker in self.workers:
                worker['process'].join()
                # print('result', worker['result'])

            # save the work parameters and results in a single json file
            results = {
                'schema': self.schema.to_string(),
                'args': str(self.args),
                'data': []
            }
            for worker in self.workers:
                r = {
                    'id': worker['id'],
                    'filename': self.filestub % worker['id'],
                    'pvs': worker['result']['pvs'],
                    # 'timestamp_range': w['result']['timestamp_range'],
                    # 'integer_range': w['result']['integer_range']
                }
                results['data'].append(r)
            with open(self.path + '/' + self.task_name + '-report.json', 'w') as fp:
                json.dump(results, fp, indent=2)

    def task(self, wid, result):
        # print('wid', wid)
        # print('self path', self.path)
        # print('len pvs', len(pvs))
        # result['done'] = True

        print('worker %d starting..' % wid)
        s = wid * self.pv_count
        e = (wid + 1) * self.pv_count
        pvs = self.all_pvs[s:e]

        filename = self.filestub % wid
        writer = parquet.ParquetWriter(filename, schema=self.schema, version='2.6')

        pvnames = []
        num_events = 0
        num_rows = 0
        ts = self.initial_timestamp
        schema_len = len(self.schema.names) - 2

        start_time = perf_counter()

        # res['integer_range'] = [int(datetime.timestamp(ts)*1e6)]
        # res['timestamp_range'] = [str(ts)]

        # generate desired amount of rows
        while num_rows < self.row_count:
            batch_size = min(self.batch_size, self.row_count - num_rows)
            num_batch_rows = 0
            timestamps = []

            # integers = []
            values = [[] for _ in range(schema_len)]

            # handle rows in batches that do not clog the system
            while num_batch_rows < batch_size:
                timestamps += self.pv_count * [ts]

                # tss = int(datetime.timestamp(ts)*1e6)
                # integers += [tss + n for n in range(self.pv_count)]
                # integers += self.gen_values(ts)
                values = self.gen_values(ts, values)

                # next event / time slice
                ts = ts + timedelta(milliseconds=72)
                num_events += 1
                num_batch_rows += self.pv_count
                num_rows += self.pv_count

            print('\rworker %2d %15s %15d / %-15d events %15d / %-15d rows' % (wid, 'WRITING', num_events, self.event_count, num_rows, self.row_count), end='')
            pvnames = int(num_batch_rows / self.pv_count) * pvs
            # table = pa.table([pvnames, timestamps, integers], schema=self.schema)
            table = pa.table([pvnames, timestamps] + values, schema=self.schema)
            # table.sort_by('timestamp')
            writer.write(table)

        # res['integer_range'].append(integers[-1])
        # res['timestamp_range'].append(str(timestamps[-1]))

        print('\rworker %2d %15s %15d / %-15d events %15d / %-15d rows' % (wid, 'DONE', num_events, self.event_count, num_rows, self.row_count))
        end_time = perf_counter()
        print('worker %d took %.2f second(s) to complete.' % (wid, end_time - start_time))

        writer.close()

        result['pvs'] = pvs
        result['filename'] = filename


class GenerateScalarInteger(Generator):
    def __init__(self):
        super().__init__()

    def setup(self, args):
        self.setup_(args)
        self.schema = pa.schema([
                                ('pvname', pa.string()),
                                ('timestamp', pa.timestamp('ns')),
                                ('integer', pa.int64())
                            ])

    def gen_values(self, ts, values):
        # values is a list of nested lists each corresponding to a schema field
        # this function needs to add new values to each of the nested list
        # NOTE: the pvname and timestamp are not included!
        tss = int(datetime.timestamp(ts)*1e6)
        # we know our schema contains only single value field
        values[0] += [tss + n for n in range(self.pv_count)]
        return values


class GenerateArrayInteger(Generator):
    def __init__(self):
        super().__init__()

    def setup(self, args):
        self.setup_(args)
        self.schema = pa.schema([
                                ('pvname', pa.string()),
                                ('timestamp', pa.timestamp('ns')),
                                ('integer', pa.list_(pa.int64()))
                            ])
        self.array_size = 1000

    def gen_values(self, ts, values):
        # values is a list of nested lists each corresponding to a schema field
        # this function needs to add new values to each of the nested list
        # NOTE: the pvname and timestamp are not included!
        tss = int(datetime.timestamp(ts)*1e6)
        # we know our schema contains only single value field
        values[0] += [self.pv_count * [tss + n for n in range(self.array_size)]]
        return values


class GenerateScalarFloat(Generator):
    def __init__(self):
        super().__init__()

    def setup(self, args):
        self.setup_(args)
        self.schema = pa.schema([
                                ('pvname', pa.string()),
                                ('timestamp', pa.timestamp('ns')),
                                ('float', pa.float64())
                            ])

    def gen_values(self, ts, values):
        # values is a list of nested lists each corresponding to a schema field
        # this function needs to add new values to each of the nested list
        # NOTE: the pvname and timestamp are not included!
        tss = float(datetime.timestamp(ts)*1e6)
        # we know our schema contains only single value field
        values[0] += [tss + n for n in range(self.pv_count)]
        return values



'''
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
'''

################################################################################################

parser = argparse.ArgumentParser()
parser.add_argument('work', action='store')
parser.add_argument('-b', '--batch_size', action='store', default='1000000', type=int)
parser.add_argument('-p', '--pv_count', action='store', default='200', type=int)
parser.add_argument('-w', '--worker_count', action='store', default='1', type=int)
parser.add_argument('-e', '--event_count', action='store', default='10000', type=int)
parser.add_argument('-P', '--path', action='store', default='pq-data', type=str)
parser.add_argument('-s', '--pv_sort', action="store_true", default=False)
parser.add_argument('-n', '--run_name', action='store', default='', type=str)
args = parser.parse_args()
print('args:', args)

# gen = GenerateScalarInteger()
# gen = GenerateScalarFloat()
gen = GenerateArrayInteger()
gen.setup(args)
gen.start()

exit(2)


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
