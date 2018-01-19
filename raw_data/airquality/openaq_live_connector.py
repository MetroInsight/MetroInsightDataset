import json
import requests
import time
import arrow
import gwd
import pdb

import get_openaq_data as client_lib 
from citadel import Citadel

init_flag = True 
interval =  10 * 60 # seconds
start_time = arrow.get().shift(hours=-24)

with open('config/citadel_config.json', 'r') as fp:
    citadel_config = json.load(fp)

citadel = Citadel(citadel_config['hostname'],
                  citadel_config['apikey'])

with open('config/openaq_config.json', 'r') as fp:
    src_config = json.load(fp)
src_hostname = src_config['hostname']
with open('metadata/unit_map.json', 'r') as fp:
    unit_map = json.load(fp)
with open('metadata/pointtype_map.json', 'r') as fp:
    pointtype_map = json.load(fp)
with open('metadata/openaq_metadata.json', 'r') as fp:
    raw_metadata_dict = json.load(fp)

wd_name = src_config['wd_name']


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
                'name': client_lib.custom_url_encode(raw_metadata['name']),
                'geometryType': 'point',
                'coordinates': raw_metadata['coordinates']
                }
        metadata_dict[name] = metadata
        points = citadel.query_points({'name': metadata['name']})
        if points:
            uuid = points[0]
        else:
            point = subset_dict(metadata, ['name', 'unit', 'pointType'])
            uuid = citadel.create_point(point)
        uuid_dict[name] = uuid

while True:
    valid_cnt = 0
    invalid_cnt = 0
    end_time = arrow.get()
    for name, metadata in metadata_dict.items():
        raw_metadata = raw_metadata_dict[name]
        # Read Data
        params = {
                'city': city,
                'parameter': raw_metadata['parameter'],
                'location': raw_metadata['location'],
                }
        try:
            results = client_lib.get_parameter_data(params, start_time, end_time)
        except Exception as e:
            print('Did not get result: \n')
            print(e)
            continue
        uuid = uuid_dict[name]
        data = client_lib.normalize_data(uuid, results, metadata)
        if not results:
            invalid_cnt += 1
            continue
        if not citadel.post_data(data):
            try:
                gwd.fault(wd_name, 'Failed at posting data of {0} to Citadel.'\
                        .format(name))
            except:
                print(arrow.get(), "gwd not accessible")
            pdb.set_trace()
        valid_cnt += 1
    start_time = end_time
    try:
        gwd.kick(wd_name, interval + 15*60)
    except:
        print(arrow.get(), "gwd not accessible")
    time.sleep(interval)
