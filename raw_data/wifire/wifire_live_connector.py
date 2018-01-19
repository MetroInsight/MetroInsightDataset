import json
import requests
import time
import arrow
import gwd

from citadel import Citadel
from citadel.util import *

init_flag = True 
interval =  5 * 60 # seconds

with open('config/citadel_config.json', 'r') as fp:
    citadel_config = json.load(fp)
#citadel_base_host = citadel_secret['hostname']
#citadel_host = citadel_base_host + '/api'

citadel = Citadel(citadel_config['hostname'])

with open('config/wifire_config.json', 'r') as fp:
    wifire_config = json.load(fp)
begin_time = arrow.get().now().shift(days=-10)
wd_name = wifire_config['wd_name']

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
        if uuids:
            uuid = uuids[0]
        else:
            point = {
                    'name': metadata['name'],
                    'pointType': metadata['pointType'],
                    'unit': metadata['unit']
                    }
            uuid = citadel.create_point(point)
        uuid_dict[srcid] = uuid

while True:
    for observable in observable_set:
        # Read Data
        wifire_data_url = wifire_hostname + '/stations/data?'
        end_time = arrow.Arrow.now()

        selectionType = "selection=boundingBox"
        selectionParameters = "&minLat=%s&minLon=%s&maxLat=%s&maxLon=%s" \
                % ( min_lat, min_lng, max_lat, max_lng)
        urlDateTime = "&from=%s&to=%s" % ( str(begin_time) , str(end_time) )
        observableInfo = "&observable=" + observable
        urlPlot = wifire_data_url\
                + selectionType \
                + selectionParameters \
                + observableInfo \
                + urlDateTime
        """
        params = {
                'selection': selection_type,
                'minLat': min_lat,
                'minLon': min_lng,
                'maxLat': max_lat,
                'maxLon': max_lng,
                'from': str(frm),
                'to': str(to),
                'observable': observable
                }
        data = requests.get(wifire_data_url, params=params)
        """
        raw_data = requests.get(urlPlot).json()
        if not raw_data.get('features'):
            continue
        features = raw_data['features']
        data = []
        for feature in features:
            # Reformat Data
            srcid = feature['properties']['description']['name'] + '_' + observable
            if metadata_dict.get(srcid):
                metadata = metadata_dict[srcid]
            else:
                metadata = {
                        'pointType': point_map[observable],
                        'unit': unit_map[feature['properties']['units'][observable]],
                        'name': encode_name(srcid),
                        'geometryType': 'point',
                        'coordinates': [[feature['geometry']['coordinates'][0],
                            feature['geometry']['coordinates'][1]]]
                        }
                point = {
                        'name': metadata['name'],
                        'pointType': metadata['pointType'],
                        'unit': metadata['unit']
                        }
                uuid_dict[srcid] = citadel.create_point(point)
                metadata_dict[srcid] = metadata
            value_list = feature['properties'][observable]
            time_list = feature['properties']['timestamp']
            time_list = [arrow.get(t).timestamp * 1000 for t in time_list]

            # Push Data
            for t, val in zip(time_list, value_list):
                if not val:
                    continue
                datum = {
                            'value': val,
                            'uuid': uuid_dict[srcid],
                            'geometryType': 'point',
                            'coordinates': metadata['coordinates'],
                            'timestamp': t
                        }
                data.append(datum)
        if not citadel.post_data(data):
            gwd.fault(wd_name, 'Failed at posting data of {0} to Citadel.'\
                               .format(srcid))
        begin_time = end_time
    gwd.kick(wd_name, 900)
    time.sleep(interval)
