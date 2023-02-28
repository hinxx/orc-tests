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
    # get the words that are between 4 and 9 chars long
    mask = (raw['a'].str.len() > 3) & (raw['a'].str.len() < 10)
    raw = raw.loc[mask]
    params_all = raw['a'].to_list()

    # take ess prefixes
    raw = pd.read_csv(ess_wordlist)
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
    print('thread %d starting..' % (id))

    ts = work['ts']
    num = len(work['pvs'])
    filename = work['filestub'] % id
    final_rows = work['event_count'] * work['day_count']

    writer = parquet.ParquetWriter(filename, schema=work['schema'], version='2.6')
    pvnames = []
    timestamps = []
    integers = []

    num_events = 0
    num_days = 0
    start_time = perf_counter()

    while num_days < work['day_count']:
        timestamps += num * [ts]
        tss = int(datetime.timestamp(ts)*1e6)
        integers += [tss + n for n in range(num)]

        # next event / time slice
        ts = ts + timedelta(milliseconds=72)
        num_events += 1

        if (num_events % 1000) == 0:
            print('\rthread %d event %d / %d' % (id, num_events , work['event_count']), end='')

        if num_events == work['event_count']:
            print('\rthread %d event %d / %d' % (id, num_events , work['event_count']))
            num_days += 1
            print('\rthread %d WRITING day %d / %d                 ' % (id, num_days, work['day_count']))
            pvnames = num_events * work['pvs']
            table = pa.table([pvnames, timestamps, integers], schema=work['schema'])
            # table.sort_by('timestamp')
            writer.write(table)
            timestamps = []
            integers = []
            num_events = 0

    print('\rthread %d DONE day %d / %d                 ' % (id, num_days, work['day_count']))
    end_time = perf_counter()
    print('thread %d took %.2f second(s) to complete.' % (id, end_time - start_time))

    writer.close()


def work1(pvs, args):
    work_name = 'work1'
    start_date = '2023-02-11'
    work = [None] * args.threads
    threads = [None] * args.threads

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
        s = n * args.pv_count
        e = (n + 1) * args.pv_count
        w = {
            'ts': datetime.fromisoformat(start_date),
            'pvs': pvs[s:e],
            'filestub': work_name+'-%d.parquet',
            'schema': schema,
            'event_count': args.events,
            'day_count': args.days
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
parser.add_argument('-e', '--events', action='store', default='10000', type=int)
args = parser.parse_args()
print('args:', args)

pvs = generate_pv_names('fake-pvs.json')
if pvs is None:
    exit(1)

print('have %d fake PVs..' % len(pvs))

if args.work == 'work1':
    work1(pvs, args)
else:
    print('work function %s not found!' % args.work)
