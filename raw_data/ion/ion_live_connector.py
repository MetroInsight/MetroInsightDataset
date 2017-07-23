import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

import json
import time
import pdb
import os

import arrow
import gwd

from citadel_helper import *
from building_depot import DataService
import ion_connector as client_lib

init_flag = True 
interval =  15 * 60 # seconds
start_time = arrow.get().shift(seconds=-interval)

wd_name = 'ucsd.metroinsight.ion'
gwd.auth(wd_name)

raw_metadatafile = 'metadata/raw_ion_metadata.json'
with open(raw_metadatafile, 'r') as fp:
    raw_metadata_dict = json.load(fp)
metadatafile = 'metadata/ion_metadata.json'
if os.path.isfile(metadatafile):
    with open(metadatafile, 'r') as fp:
        metadata_dict = json.load(fp)
else:
    metadata_dict = dict()

with open("config/ion_config.json", "r") as fp:
    configs = json.load(fp)
    base_url = configs['base_url']
with open("config/bd2_3secrets.json", "r") as fp:
    cred = json.load(fp)
    apikey = cred['api_key']
    username = cred['username']
bd = DataService(base_url, apikey, username)

if init_flag:
    for name, raw_metadata in raw_metadata_dict.items():
        # Init datapoints
        if name not in metadata_dict:
            if 'longitude' not in raw_metadata:
                continue
            metadata = {
                    'pointType': raw_metadata['point_type'],
                    'unit': raw_metadata['unit'],
                    'name': raw_metadata['name'],
                    'geometryType': 'point',
                    'coordinates': [[raw_metadata['longitude'],
                                     raw_metadata['latitude']]]
                    }
            metadata_dict[name] = metadata
        if not metadata_dict[name].get('uuid'):
            points = find_points({'name': metadata['name']})
            if points:
                uuid = points[0]['uuid']
            else:
                point = subset_dict(metadata, ['name', 'unit', 'pointType'])
                uuid = create_point(point)
            metadata_dict[name]['uuid'] = uuid
    with open(metadatafile, 'w') as fp:
        json.dump(metadata_dict, fp, indent=2)

while True:
    end_time = arrow.get()
    for srcid, metadata in metadata_dict.items():
        raw_metadata = raw_metadata_dict[srcid]
        # Read Data
        bd_uuid = raw_metadata['bd_uuid']
        ts_data = bd.get_timeseries_datapoints(bd_uuid, 'PresentValue', \
                start_time.datetime, end_time.datetime)['timeseries']
        uuid = metadata['uuid']
        if not ts_data:
            print("No data, {0}".format(metadata['name']))
            no_data_cnt += 1
            continue
        data = client_lib.normalize_data(uuid, ts_data, metadata)
        if not post_data(data):
            gwd.fault(wd_name, 'Failed at posting data of {0} to Citadel.'\
                               .format(name))
    start_time = end_time
    print('no data: ', no_data_cnt)
    print('data: ', data_cnt)
    gwd.kick(wd_name, interval + 5*60)
    time.sleep(interval)
