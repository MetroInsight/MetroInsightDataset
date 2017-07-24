import requests
import json
import pdb
import arrow

with open('config/citadel_config.json', 'r') as fp:
    citadel_config = json.load(fp)
citadel_base_host = citadel_config['hostname']
citadel_host = citadel_base_host + '/api'

headers = {
        'content-type': 'application/json'
        }

query_url = citadel_host + '/query'
data_url = citadel_host + '/data'
point_url = citadel_host + '/point'
dataquery_url = citadel_host + '/querydata'

def create_point(point, headers=headers):
    resp = requests.post(point_url, json=point, headers=headers)
    if not resp.json()['success']:
        pdb.set_trace()
    else:
        uuid = resp.json()['uuid']
    return uuid

def find_points(query, headers=headers):
    query = {'query': query}
    resp = requests.post(query_url, json=query, headers=headers)
    try:
        points = resp.json()['results']
    except:
        pdb.set_trace()
    return points
    
def post_data(data, headers=headers):
    resp = requests.post(data_url, json={'data': data}, headers=headers).json()
    if not resp['success']:
        print(resp['reason'])
    return resp['success']

def get_data(query, headers=headers):
    resp = requests.get(data_url, json={'data': data}, headers=headers)
    return resp.json()['success']

def subset_dict(d, keys):
    return dict([(k, d[k]) for k in keys])

date_format = 'YYYY-MM-DDTHH:mm:ss'
