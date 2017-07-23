import json
import requests
import time
import arrow
import gwd
import pdb

init_flag = True 
interval =  5 * 60 # seconds

wd_name = 'ucsd.metroinsight.wifire'
gwd.auth(wd_name)
gwd.kick(wd_name)

with open('config/citadel_config.json', 'r') as fp:
    citadel_secret = json.load(fp)
citadel_base_host = citadel_secret['hostname']
citadel_host = citadel_base_host + '/api'

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

query_url = citadel_host + '/query'
data_url = citadel_host + '/data'
point_url = citadel_host + '/point'
dataquery_url = citadel_host + '/querydata'

headers = {
        'content-type': 'application/json'
        }

def create_point(point):
    resp = requests.post(point_url, json=point, headers=headers)
    if not resp.json()['success']:
        pdb.set_trace()
    else:
        uuid = resp.json()['uuid']
    return uuid



metadata_dict = dict()
uuid_dict = dict()

if init_flag:
    for name, raw_metadata in raw_metadata_dict.items():
        # Init datapoints
        metadata = {
                'pointType': raw_metadata['point_type'],
                'unit': raw_metadata['unit'],
                'name': raw_metadata['name'],
                'geometryType': 'point',
                'coordinates': [[raw_metadata['longitude'],
                                 raw_metadata['latitude']]]
                }
        metadata_dict[name] = metadata
        query = {'query': {'name': metadata['name']}}
        resp = requests.post(query_url, json=query, headers=headers).json() #TODO: Citadel doesn't implement failure case yet.
        uuids = resp['results']
        success = resp['success']
        if not success:
            print("ERROR")
            pdb.set_trace()
        elif uuids:
            uuid = uuids[0]['uuid']
        else:
            point = {
                    'name': metadata['name'],
                    'pointType': metadata['pointType'],
                    'unit': metadata['unit']
                    }
            resp = requests.post(point_url, json=point, headers=headers)
            if not resp.json()['success']:
                pass
            else:
                uuid = resp.json()['uuid']
        uuid_dict[name] = uuid

while True:
    for observable in observable_set:
        # Read Data
        wifire_data_url = wifire_hostname + '/stations/data?'
        to = arrow.Arrow.now()
        frm = to.replace(seconds=-interval)
        selectionType = "selection=boundingBox"
        selectionParameters = "&minLat=%s&minLon=%s&maxLat=%s&maxLon=%s" \
                % ( min_lat, min_lng, max_lat, max_lng)
        urlDateTime = "&from=%s&to=%s" % ( str(frm) , str(to) )
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
            name = feature['properties']['description']['name'] + '_' + observable
            if metadata_dict.get(name):
                metadata = metadata_dict[name]
            else:
                metadata = {
                        'pointType': point_map[observable],
                        'unit': unit_map[feature['properties']['units'][observable]],
                        'name': name,
                        'geometryType': 'point',
                        'coordinates': [[feature['geometry']['coordinates'][0],
                            feature['geometry']['coordinates'][1]]]
                        }
                point = {
                        'name': metadata['name'],
                        'pointType': metadata['pointType'],
                        'unit': metadata['unit']
                        }
                uuid_dict[name] = create_point(point)
            value_list = feature['properties'][observable]
            time_list = feature['properties']['timestamp']
            time_list = [arrow.get(t).timestamp for t in time_list]

            # Push Data
            for t, val in zip(time_list, value_list):
                if not val:
                    continue
                datum = {
                            'value': val,
                            'uuid': uuid_dict[name],
                            'geometryType': 'point',
                            'coordinates': metadata['coordinates'],
                            'timestamp': t
                        }
                data.append(datum)
        resp = requests.post(data_url, json={'data': data}, headers=headers)
    gwd.kick(wd_name, 900)
    time.sleep(interval)
