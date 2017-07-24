import json
import requests
import time
import arrow
import gwd
import pdb

import get_openaq_data as client_lib 
from citadel_helper import *

init_flag = True 
interval =  10 * 60 # seconds
start_time = arrow.get().shift(hours=-24)

wd_name = 'ucsd.metroinsight.openaq'
gwd.auth(wd_name)
gwd.kick(wd_name)

with open('config/openaq_config.json', 'r') as fp:
    src_config = json.load(fp)
src_hostname = src_config['hostname']
with open('metadata/unit_map.json', 'r') as fp:
    unit_map = json.load(fp)
with open('metadata/pointtype_map.json', 'r') as fp:
    pointtype_map = json.load(fp)
with open('metadata/openaq_metadata.json', 'r') as fp:
    raw_metadata_dict = json.load(fp)


## src-specific configs
city = src_config['city']
staitions = client_lib.get_measuring_stations()['results']

metadata_dict = dict()
uuid_dict = dict()

if init_flag:
    for name, raw_metadata in raw_metadata_dict.items():
        # Init datapoints
        metadata = {
                'pointType': pointtype_map[raw_metadata['parameter']],
                'unit': raw_metadata['unit'],
                'name': raw_metadata['name'],
                'geometryType': 'point',
                'coordinates': raw_metadata['coordinates']
                }
        metadata_dict[name] = metadata
        points = find_points({'name': metadata['name']})
        if points:
            uuid = points[0]['uuid']
        else:
            point = subset_dict(metadata, ['name', 'unit', 'pointType'])
            uuid = create_point(point)
        uuid_dict[name] = uuid

while True:
    for name, metadata in metadata_dict.items():
        end_time = arrow.get()
        raw_metadata = raw_metadata_dict[name]
        # Read Data
        params = {
                'city': city,
                'parameter': raw_metadata['parameter'],
                'location': raw_metadata['location'],
                }
        results = client_lib.get_parameter_data(params, start_time, end_time)
        uuid = uuid_dict[name]
        data = client_lib.normalize_data(uuid, results, metadata)
        if not results:
            continue
        if not post_data(data):
            gwd.fault(wd_name, 'Failed at posting data of {0} to Citadel.'\
                               .format(name))
            pdb.set_trace()
        resp = requests.post(data_url, json={'data': data}, headers=headers)
        start_time = end_time
    gwd.kick(wd_name, interval + 5*60)
    time.sleep(interval)
