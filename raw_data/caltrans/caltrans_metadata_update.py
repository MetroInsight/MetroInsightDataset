
import json
import requests
import time
import arrow
import gwd
import pdb

from main import init_metadata, get_ids
from citadel import Citadel

data_types = ["FLOW", "OCCUPANCY", "SPEED", "VMT", "VHT", "DELAY"] # Alter this and dict to modify data types used.
uuid_dict = dict()
raw_metadata_dict = {}
metadata_dict = {}

init_flag = True 
interval =  10 * 60 # seconds
start_time = arrow.get().shift(hours=-24)

with open('config/citadel_config.json', 'r') as fp:
    citadel_config = json.load(fp)

citadel = Citadel(citadel_config['hostname'],
                  citadel_config['apikey'])

# Declare static variables
raw_metadata_dict, metadata_dict = init_metadata()
filename = "5minagg_latest.txt"  # Altered to work with 5 minute aggregate data
# Get VDS IDs
IDs = get_ids()

tot_latency = arrow.get() - arrow.get()
cnt = 0

for srcid, metadata in metadata_dict.items():
    name = metadata['name']
    points = citadel.query_points({'name': name})
    if not points:
        pdb.set_trace()
        print(name, 'not found')
        uuid = citadel.create_point(subset_dict(metadata, ['name', 'unit', 'pointType']))
    else:
        uuid = points[0]
        point = citadel.get_point(uuid)
    point['owner'] = 'jbkoh@eng.ucsd.edu'
    uuid_dict[name] = uuid
    begin_time = arrow.get()
    citadel.upsert_metadata(uuid, point)
    end_time = arrow.get()
    tot_latency += end_time - begin_time
    cnt += 1

print('Avg latency: ', tot_latency / cnt)
