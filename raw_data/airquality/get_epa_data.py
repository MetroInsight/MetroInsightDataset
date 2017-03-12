# Get EPA Air Quality data and metadata
# Link to API: https://aqs.epa.gov/api

import pandas as pd
import requests
import os.path
import sys
import glob

# Get yearly measurements for all air quality parameters between the
# dates specified.
def get_yearly_readings(start_year, end_year):
    data_dir = 'data/epa_yearly_readings/'
    years = range(start_year, end_year+1)
    param_codes = pd.read_csv(data_dir + 'param_codes.tsv', sep='\t')
    base_url = 'https://aqs.epa.gov/api/rawData'

    request_params = {}
    request_params['user']   = 'email'
    request_params['pw']     = 'password'
    request_params['format'] = 'DMCSV'
    request_params['state']  = '06' # California
    request_params['county'] = '073' # San Diego

    for year in years:
        request_params['bdate'] = str(year) + '0101'
        request_params['edate'] = str(year) + '1231'

        for code in param_codes['code']:
            request_params['param'] = str(code)
            print year,code

            r = requests.get(base_url, params=request_params)

            # Skip empty files: No readings in SD County for this year and parameter
            num_lines = r.text.count('\r\n')
            if num_lines == 2:
                continue

            with open(data_dir + str(year) + '-' + str(code) + '.csv', 'w') as f:
                f.write(r.text)

# Get all distinct pairs of (year, station id) in the yearly readings gathered
# above. Used to collect daily summaries and metadata about the observed stations.
def get_stations_per_year():
    data_dir = 'data/epa_yearly_readings/'
    files = sorted(glob.glob(data_dir + '*-*.csv'))
    stations = set()

    for filename in files:
        print(filename)
        year = filename[-14:-10]
        try:
            df = pd.read_csv(filename, engine='python', skipfooter=1)
        except ValueError:
            continue

        if 'Site Num' in df:
            for site_num in df['Site Num'].unique():
                stations.add((year, str(site_num).zfill(4)))

    return stations

# Get daily summaries for the provided measurement stations and years.
# Also provides metadata about the readings made and the stations themselves.
def get_daily_summaries(stations_per_year):
    data_dir = 'data/epa_daily_summaries/'
    base_url = 'https://www3.epa.gov/cgi-bin/broker?_service=data'
    base_url += '&_program=dataprog.Daily.sas&check=void&polname=Lead&debug=0'

    for (year, station) in stations_per_year:
        url = base_url + '&year=' + year + '&site=06-073-' + station

        print year,station
        r = requests.get(url)
        with open(data_dir + year + '-' + station + '.csv', 'w') as f:
            f.write(r.text)

if __name__ == '__main__':
    # get_yearly_readings(1980, 2017)

    stations = get_stations_per_year()
    print stations
    get_daily_summaries(stations)
