import pyarrow as pa
from pyarrow import orc
import pandas as pd
import json
import requests
import datetime
import stat
import os

# Some preliminary code for working with the archiver
# and creating ORC files..


def convert_single(filename):
    '''
    $ ll ArrayCounter_RBV*
    308151 Feb 17 10:56 ArrayCounter_RBV.json
    188053 Feb 17 10:56 ArrayCounter_RBV.orc
     29661 Feb 17 10:56 ArrayCounter_RBV-dict.orc
    $ ../install-1.8.2/bin/orc-scan ArrayCounter_RBV-dict.orc 
    $ ../install-1.8.2/bin/orc-scan ArrayCounter_RBV.orc 
    Rows: 3601
    Batches: 4
    '''
    with open(filename, 'r') as fp:
        data = json.load(fp)

        # df = pa.array(data[0]["data"])
        df = pd.DataFrame(data[0]["data"])
        df["timestamp"] = pd.to_datetime(df["secs"] + df["nanos"] * 1e-9, unit="s")
        df = df[["timestamp", "val"]]
        df["pvname"] = data[0]["meta"]["name"]
        df = df.set_index("timestamp")
        print(df)
        table = pa.table(df)
        print(table)

        # dictionary encoding is off by default
        orc.write_table(table, 'ArrayCounter_RBV.orc')
        # enable dictionary encoding for strings
        orc.write_table(table, 'ArrayCounter_RBV-dict.orc', dictionary_key_size_threshold=1)


def load_from_archiver(pvname):
    '''
    if an event (E0) is present in the archiver and we request events from any point in time
    after that where there are no events, we still get that single E0 in response data
    effectively storing duplicates of event E0 for each such request
    code below handles this case and removes the duplicate
    '''
    archiver = 'archiver-01'
    url_stub = 'http://%s.tn.esss.lu.se:17668/retrieval/data/getData.json'
    url = url_stub % archiver
    print('contacting', archiver)

    start_date = '2021-01-01'
    end_date = '2023-03-01'

    last = None
    dt = datetime.datetime.fromisoformat(start_date)
    end_dt = datetime.datetime.fromisoformat(end_date)
    while dt < end_dt:
        range_start = dt
        range_end = dt + datetime.timedelta(days=7)

        filename = './aa-data/' + pvname + '-' + range_start.strftime('%Y-%m-%d') + '.json'
        if not os.path.exists(filename):
            params = {'pv': pvname, 
                    'from': range_start.strftime('%Y-%m-%d') + 'T00:00:00.000000Z',
                    'to': range_end.strftime('%Y-%m-%d') + 'T00:00:00.000000Z'}

            res = requests.get(url, params=params)
            if res.status_code == requests.codes.ok:
                jdata = res.json()
                if last and len(jdata[0]['data']):
                    # remove the duplicate (first) event; already in the previous request
                    if last == jdata[0]['data'][0]:
                        # print('removed duplicate:', last)
                        del jdata[0]['data'][0]
                with open(filename, 'w') as fp:
                    json.dump(jdata, fp, indent=2)
                if len(jdata[0]['data']):
                    # save the last event; to check for duplicate in next request
                    last = jdata[0]['data'][-1]
                print('saved', filename, len(str(jdata)), 'bytes')
            else:
                print('failed', url)
        else:
            print('exists', filename)

        dt = range_end


def convert_multiple(pvname):
    '''
    $ ll MEBT-010:PBI-BPM-002:SM-TR1-ArrayCounter_RBV-*.json | awk '{print $5}' | paste -sd+ | bc
    3156233074
    $ ll all-ArrayCounter_RBV*
     127151861 Feb 17 11:35 all-ArrayCounter_RBV-dict.orc
    1122561380 Feb 17 11:21 all-ArrayCounter_RBV.orc
    $ ../install-1.8.2/bin/orc-scan all-ArrayCounter_RBV.orc 
    Rows: 22622316
    Batches: 22100
    $ ../install-1.8.2/bin/orc-scan all-ArrayCounter_RBV-dict.orc 
    Rows: 22622316
    Batches: 22094
    '''
    g = os.walk('./aa-data')
    _, _, files = next(g)
    files.sort()
    
    writer1 = orc.ORCWriter('all-ArrayCounter_RBV.orc')
    writer2 = orc.ORCWriter('all-ArrayCounter_RBV-dict.orc', dictionary_key_size_threshold=1)

    for filename in files:
        if filename.startswith(pvname):
            print('handling', filename)
            with open(filename, 'r') as fp:
                data = json.load(fp)
                if len(data[0]["data"]):
                    df = pd.DataFrame(data[0]["data"])
                    df["timestamp"] = pd.to_datetime(df["secs"] + df["nanos"] * 1e-9, unit="s")
                    df = df[["timestamp", "val"]]
                    df["pvname"] = data[0]["meta"]["name"]
                    df = df.set_index("timestamp")
                    # print(df)
                    table = pa.table(df)
                    # print(table)
            
                    # no string dictionary
                    writer1.write(table)
                    # with string dictionary
                    writer2.write(table)



def archiver_to_orc(archiver, pvnames, orcname):
    writer = orc.ORCWriter(orcname + '.orc', dictionary_key_size_threshold=1)
    



def get_pv_info(pvname):
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
    return None


def test1(filter_str, orcname):
    filename = './PBI-archiver-01.json'
    pvnames = []
    print('PV filter', filter_str)
    with open(filename, 'r') as fp:
        pvs = json.load(fp)
        for pv in pvs:
            if filter_str in pv:
                pvnames.append(pv)
    
    # print('number of Pvs to handle', len(pvnames))
    # archiver_to_orc('archiver-01', pvnames, 'test1')

    writer_int = orc.ORCWriter(orcname + '-int.orc', dictionary_key_size_threshold=1)
    writer_int_count = 100
    writer_float = orc.ORCWriter(orcname + '-float.orc', dictionary_key_size_threshold=1)
    writer_str = orc.ORCWriter(orcname + '-str.orc', dictionary_key_size_threshold=1)
    
    archiver = 'archiver-01'
    url_stub = 'http://%s.tn.esss.lu.se:17668/retrieval/data/getData.json'
    url = url_stub % archiver
    print('using archiver', archiver)

    # start_date = '2021-01-01'
    # end_date = '2023-03-01'
    start_date = '2022-07-11'
    end_date = '2022-07-12'

    last = {}
    dt = datetime.datetime.fromisoformat(start_date)
    end_dt = datetime.datetime.fromisoformat(end_date)
    while dt < end_dt:
        range_start = dt
        # range_end = dt + datetime.timedelta(hours=1)
        range_end = dt + datetime.timedelta(days=1)

        for pvname in pvnames:
            # DEBUG
            if writer_int_count <= 0:
                dt = end_dt
                break

            params = {'pv': pvname, 
                    'from': range_start.isoformat() + 'Z',
                    'to': range_end.isoformat() + 'Z'}
            # print('handle params', params)
            res = requests.get(url, params=params)
            print(res.request.url)
            if res.status_code == requests.codes.ok:
                # print('sucess', url)
                jdata = res.json()
                if len(jdata) and 'data' in jdata[0] and len(jdata[0]['data']):
                    if pvname in last:
                        # remove the duplicate (first) event; already in the previous request
                        if last[pvname] == jdata[0]['data'][0]:
                            # print('removed duplicate:', last)
                            del jdata[0]['data'][0]

                    df = pd.DataFrame(jdata[0]["data"])
                    df["timestamp"] = pd.to_datetime(df["secs"] + df["nanos"] * 1e-9, unit="s")
                    df = df[["timestamp", "val"]]
                    df["pvname"] = jdata[0]["meta"]["name"]
                    df = df.set_index("timestamp")
                    # print(df)
                    table = pa.table(df)
                    # print(table)


                    # "value": "DBR_SCALAR_BYTE"
                    # "value": "DBR_SCALAR_DOUBLE"
                    # "value": "DBR_SCALAR_ENUM"
                    # "value": "DBR_SCALAR_INT"
                    # "value": "DBR_SCALAR_SHORT"
                    # "value": "DBR_SCALAR_STRING"
                    # "value": "DBR_WAVEFORM_BYTE"
                    # "value": "DBR_WAVEFORM_DOUBLE"
                    # "value": "DBR_WAVEFORM_FLOAT"
                    # "value": "DBR_WAVEFORM_INT"
                    # "value": "DBR_WAVEFORM_STRING"

                    pv_infos = get_pv_info(pvname)
                    pv_type = None
                    for pv_info in pv_infos:
                        if pv_info['name'] == 'Archiver DBR type (from typeinfo):':
                            pv_type = pv_info['value']
                    
                    if pv_type == 'DBR_SCALAR_INT' or pv_type == 'DBR_SCALAR_SHORT' or pv_type == 'DBR_SCALAR_BYTE' or pv_type == 'DBR_SCALAR_ENUM':
                        writer_int.write(table)
                        # writer_int_count -= 1
                    elif pv_type == 'DBR_SCALAR_DOUBLE' or pv_type == 'DBR_SCALAR_FLOAT':
                        writer_float.write(table)
                    elif pv_type == 'DBR_SCALAR_STRING':
                        writer_str.write(table)
                    else:
                        print('unhandled DBR:', pv_type)

                    # save the last event; to check for duplicate in next request
                    last = jdata[0]['data'][-1]

                    print(params, 'got', len(str(jdata)), 'bytes')
                else:
                    print(params, 'no data')
            else:
                print(params, 'failed')

        dt = range_end

    writer_int.close()
    # writer_float.close()
    # writer_str.close()


#################################################################################

# test single json file conversion to ORC
# filename = 'ArrayCounter_RBV.json'
# convert_single(filename)

# get all the data from archiver into local json files
# load_from_archiver('MEBT-010:PBI-BPM-002:SM-TR1-ArrayCounter_RBV')
# load_from_archiver('MEBT-010:PBI-BPM-002:PhaseUnwrapR')

# convert_multiple('MEBT-010:PBI-BPM-002:SM-TR1-ArrayCounter_RBV')

test1('PBI-BPM01:Ctrl-AMC-110', 'test1')
