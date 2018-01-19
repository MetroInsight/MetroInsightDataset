import json
import requests
import time
import arrow
import sys

from citadel import Citadel
from citadel.util import *

init_flag = True 
interval =  5 * 60 # seconds

with open('config/citadel_config.json', 'r') as fp:
    citadel_config = json.load(fp)

citadel = Citadel(citadel_config['hostname'], 
                  citadel_config['apikey'],
                  verify=False)

with open('config/wifire_config.json', 'r') as fp:
    wifire_config = json.load(fp)

wifire_hostname = wifire_config['hostname']
#wifire_apikey = wifire_config['apikey']
min_lat = wifire_config['min_lat']
min_lng = wifire_config['min_lng']
max_lat = wifire_config['max_lat']
max_lng = wifire_config['max_lng']
selection_type = wifire_config['selection']
observable_set = wifire_config['observable_set']

with open('metadata/wifire_unit_map.json', 'r') as fp:
    unit_map = json.load(fp)
with open('metadata/wifire_point_map.json', 'r') as fp:
    point_map = json.load(fp)
with open('metadata/wifire_metadata.json', 'r') as fp:
    raw_metadata_dict = json.load(fp)

headers = {
        'content-type': 'application/json'
        }

metadata_dict = dict()
uuid_dict = dict()

if init_flag:
    for srcid, raw_metadata in raw_metadata_dict.items():
        # Init datapoints
        metadata = {
                'pointType': raw_metadata['point_type'],
                'unit': raw_metadata['unit'],
                'name': encode_name(raw_metadata['name']),
                'geometryType': 'point',
                'coordinates': [[raw_metadata['longitude'],
                                 raw_metadata['latitude']]]
                }
        metadata_dict[srcid] = metadata
        uuids = citadel.query_points({'name': metadata['name']})
        point = {
                'name': metadata['name'],
                'pointType': metadata['pointType'],
                'unit': metadata['unit'],
                }
        if uuids:
            uuid = uuids[0]
        else:
            uuid = citadel.create_point(point)
        point['owner'] = 'jbkoh@eng.ucsd.edu'
        res = citadel.upsert_metadata(uuid, point)
        if not res:
            pdb.set_trace()
