# Download weather data using the Mesowest REST API.
# Link to API: https://synopticlabs.org/api/

import requests
import json
import os
from pprint import pprint

with open('config/mesowest_secret.json', 'r') as fp:
    api_key = json.load(fp)['apikey']

api_key = 'api_key'
token = 'token'

# Use the Mesowest API to get a token for the provided API key.
def get_token(api_key):
    url = "https://api.synopticlabs.org/v2/auth?"
    url += "apikey=" + api_key
    response = requests.get(url)
    print response
    r = json.loads(response.text)
    token = r['TOKEN']
    print "Token: " + token
    return token

# List all the tokens associated with the given API key.
def list_tokens(api_key):
    url = "https://api.synopticlabs.org/v2/auth?"
    url += "apikey=" + api_key
    url += "&list=1"
    response = requests.get(url)
    r = json.loads(response.text)
    pprint(r)

# Disable a particular token associated with an API key.
def disable_token(api_key, token):
    url = "https://api.synopticlabs.org/v2/auth?"
    url += "apikey=" + api_key
    url += "&disableToken=" + token
    response = requests.get(url)
    r = json.loads(response.text)
    pprint(r)

# Use the Mesowest API to retrieve metadata for stations within a given radius
# of a given latitude and longitude point.
# Example: get_stations_within_radius(token, 32.8801, -117.234, 1.5)
def get_stations_within_radius(token, lat, lon, dist):
    url = "https://api.synopticlabs.org/v2/stations/metadata?"
    url += "token=" + token
    url += "&complete=1"
    url += "&sensorvars=1"
    url += "&radius=%s,%s,%s" % (lat, lon, dist)
    print url
    response = requests.get(url)
    r = json.loads(response.text)
    return r

# Process the metadata returned by get_stations_within_radius() and add the
# updated metadata to a metadata.json file, replacing old records from the
# same stations if present.
def add_stations_to_metadata_file(st):
    station_metadata = {}
    try:
        f = open('metadata/metadata.json', 'r')
        station_metadata = json.load(f)
        f.close()
    except (IOError, ValueError): # No metadata file exists
        station_metadata = {}

    for station in st['STATION']:
        station_metadata[station['STID']] = station

    f = open('metadata/metadata.json', 'w')
    json.dump(station_metadata, f)
    f.close()

# Retrieve sensor data for a particular station, restricted by the given start
# and end times, and store the result into a csv file.
# Example: get_station_timeseries(token, "AP907", "201701300000", "201701300100")
def get_station_timeseries(token, station_id, start_time, end_time):
    url = "https://api.synopticlabs.org/v2/stations/timeseries?"
    url += "token=" + token
    url += "&stid=" + station_id
    url += "&start=" + start_time
    url += "&end=" + end_time
    url += "&output=csv"
    print url
    response = requests.get(url, stream=True)

    with open('data/' + station_id + '.csv', 'wb') as f:
        for chunk in response.iter_content(chunk_size=2**20):
            f.write(chunk)

# Download all sensor data for the weather sensors in the metadata file,
# with the option to skip repeat downloads for sensors that already exist
# in the data directory.
def download_all_data(token, start_time, end_time, skip_existing=True):
    f = open('metadata/metadata.json', 'r')
    station_metadata = json.load(f)
    f.close()

    for station in station_metadata.keys():
        filename = 'data/' + station + '.csv'
        if skip_existing and os.path.isfile(filename):
            continue
        else:
            get_station_timeseries(token, station, start_time, end_time)

if __name__ == '__main__':
    # stations = get_stations_within_radius(token, 32.8801, -117.234, 15)
    # add_stations_to_metadata_file(stations)
    download_all_data(token, "199701010000", "201702010000")

