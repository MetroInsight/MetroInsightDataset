# Get OpenAQ Air Quality Data for San Diego
# https://docs.openaq.org/

import requests
import json
import time
import os

data_dir = 'data/openaq/'
base_url = 'https://api.openaq.org/v1/'
city = 'San Diego-Carlsbad-San Marcos'

# Get all measuring stations located in San Diego
def get_measuring_stations():
    filename = data_dir + 'stations_metadata.json'
    if os.path.isfile(filename):
        f = open(filename, 'r')
        stations = json.load(f)
        f.close()
    else:
        url = base_url + 'locations?city=' + city
        url = url.replace(' ', '%20')
        stations = requests.get(url).json()
        f = open(data_dir + 'stations_metadata.json', 'w')
        json.dump(stations, f)
        f.close()
    
    return stations

# Get observed measurements from the provided station.
def get_station_measurements(station_name):
    url = base_url + 'measurements?city=' + city + '&location=' + station_name
    print url
    url = url.replace(' ', '%20')
    response = requests.get(url).json()
    num_pages = (response['meta']['found'] / response['meta']['limit']) + 2

    results = []
    results.extend(response['results'])

    for page in range(2, num_pages + 1):
        url = base_url + 'measurements?city=' + city + '&location=' + station_name
        url += '&page=' + str(page)
        print url
        url = url.replace(' ', '%20')

        response = requests.get(url).json()
        results.extend(response['results'])
        time.sleep(2)

    f = open(data_dir + station_name + '.csv', 'w')
    json.dump(results, f)
    f.close()

if __name__ == '__main__':
    stations = get_measuring_stations()

    for i in range(len(stations['results'])):
        print stations['results'][i]['location']
        get_station_measurements(stations['results'][i]['location'])
