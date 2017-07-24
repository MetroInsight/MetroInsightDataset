# Get OpenAQ Air Quality Data for San Diego
# https://docs.openaq.org/

import requests
import json
import time
import os
import pdb

import arrow

data_dir = 'data/openaq/'
metadata_dir = 'metadata'
base_url = 'https://api.openaq.org/v1'
city = 'San Diego-Carlsbad-San Marcos'

date_format = 'YYYY-MM-DDTHH:mm:ss'

# Get all measuring stations located in San Diego
def get_measuring_stations():
    with open('metadata/unit_map.json', 'r') as fp:
        unit_map = json.load(fp)

    filename = metadata_dir + '/openaq_stations.json'
    if os.path.isfile(filename):
        f = open(filename, 'r')
        stations = json.load(f)
        f.close()
    else:
        url = base_url + '/locations'
        params = {
                'city': city
                }
        stations = requests.get(url, params=params).json()
        f = open(metadata_dir + '/openaq_stations.json', 'w')
        json.dump(stations, f)
        f.close()

    metadata_dict = dict()
    for station in stations['results']:
        for parameter in station['parameters']:
            name =  station['location'] + '_' + station['city'] + \
                    '_' + station['sourceName'] + '_' + parameter
            metadata = {
                    'name': name,
                    'city': station['city'],
                    'location': station['location'],
                    'coordinates': [[station['coordinates']['longitude'], \
                                    station['coordinates']['latitude']]],
                    'unit': unit_map.get(parameter),
                    'parameter': parameter
                    }
            metadata_dict[name] = metadata

    with open('metadata/openaq_metadata.json', 'w') as fp:
        json.dump(metadata_dict, fp, indent=2)
    
    return stations

def get_parameter_data(params, start_time=None, end_time=None):
# start_time and end_time are arrow.get()
    if start_time:
        params['date_from'] = start_time.format(date_format)
    if end_time:
        params['date_to'] = end_time.format(date_format)

    url = base_url + '/measurements'
    response = requests.get(url, params=params).json()
    num_pages = int(response['meta']['found'] / response['meta']['limit'] + 2)

    results = []
    results.extend(response['results'])

    for page in range(2, num_pages + 1):
        url = base_url + '/measurements'
        params['page'] = page
        response = requests.get(url, params=params).json()
        try:
            results.extend(response['results'])
        except:
            pdb.set_trace()
    return results


def normalize_data(uuid, data, metadata):
    return [{
                'uuid': uuid,
                'coordinates': metadata['coordinates'],
                'geometryType': 'point',
                'value': datum['value'],
                'timestamp': arrow.get(datum['date']['utc']).timestamp * 1000
            } for datum in data]
             


# Get observed measurements from the provided station.
def get_station_measurements(station_name, begin_time=None, end_time=None):
    params = {
            'city': city,
            'location': station_name
            }
    if begin_time:
        params['date_from'] = begin_time
    if end_time:
        params['data_to'] = end_time
    url = base_url + '/measurements'
    response = requests.get(url, params=params).json()
    num_pages = int(response['meta']['found'] / response['meta']['limit'] + 2)

    results = []
    results.extend(response['results'])

    for page in range(2, num_pages + 1):
        url = base_url + '/measurements'
        params = {
                'city': city,
                'location': station_name,
                'page': page
                }
        if begin_time:
            params['date_from'] = begin_time
        if end_time:
            params['data_to'] = end_time
        response = requests.get(url, params=params).json()
        try:
            results.extend(response['results'])
        except:
            pdb.set_trace()
        time.sleep(1)

    with open(data_dir + station_name + '.json', 'w') as fp:
        json.dump(results, fp, indent=2)

if __name__ == '__main__':
    stations = get_measuring_stations()
    begin_time = arrow.get().shift(days=-7).format(date_format)
    end_time = arrow.get().format(date_format)

    for i in range(len(stations['results'])):
        print(stations['results'][i]['location'])
        get_station_measurements(stations['results'][i]['location'], begin_time, end_time)
