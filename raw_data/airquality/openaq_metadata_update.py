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
        point = citadel.get_point(uuid)
    else:
        pdb.set_trace()
        point = subset_dict(metadata, ['name', 'unit', 'pointType'])
        uuid = citadel.create_point(point)
    point['owner'] = 'jbkoh@eng.ucsd.edu'
    res = citadel.upsert_metadata(uuid, point)
    if not res:
        pdb.set_trace()
