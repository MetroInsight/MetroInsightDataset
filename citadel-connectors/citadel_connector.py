import json
import pdb
from os import walk
import argparse

import arrow
import pandas as pd

from citadel import Citadel
import config



parser = argparse.ArgumentParser(description='Run data dump uploader')
parser.add_argument('-dataset', dest='dataset_name', type=str)
parser.add_argument('-datasource', dest='source_reference', type=str)

args = parser.parse_args()
dataset_name = args.dataset_name
source_reference = args.source_reference

citadel = Citadel(config.CITADEL_URL)

base_dir = config.BASE_DIR + '/raw_data/' + dataset_name
data_dir = base_dir + '/data'

# Load predefined metadata
with open(base_dir + '/metadata/%s_metadata.json'%dataset_name, 'r') as fp:
    metadata_dict = json.load(fp)

# Find all data files pre-downloaded by a script.
filenames = []
for (dirpath, dirname, filename) in walk(data_dir):
    filenames.extend(filename)
    break


failed_point_list = list()
for filename in filenames:
    srcid = filename[:-4]
    filename = data_dir + '/' + filename
    if not 'csv' in filename:
        continue
    # Load the metadata file.
    metadata = metadata_dict[srcid]
    # Load time series data
    data = pd.Series.from_csv(filename, header=0)
    try:
        # Skip a data if the data does not have location information
        assert(isinstance(metadata['latitude'], float))

        # Make a data based on our schema. Please do follow this structure.
        # A request may be rejected if it doesn't follow this schema.
        new_metadata = {
                'name': metadata['name'],
                'tags':{
                    'point_type':metadata['point_type'],
                    'unit': metadata['unit'],
                    'source_reference': source_reference
                    },
                'geometry':{
                    'coordinates': [metadata['longitude'], metadata['latitude']],
                    'type': 'Point'
                    }
                }
        for tag, value in metadata.items():
            if tag in ['name', 'point_type', 'longitude', 'latitude', 'unit']:
                continue
            new_metadata['tags'][tag] = value


        # Structure the timeseries data in this way.
        ts_data = dict((int(arrow.get(k).timestamp),float(v)) for k,v in data.iteritems())
    except:
        failed_point_list.append(srcid)
        continue

    try:
        # Create a point at Citadel
        res = citadel.create_point(new_metadata)
        if not res['success']:
            # Skip if the point is already there.
            if json.loads(res['result']['reason'])['reason'] == 'Given name already exists':
                continue
        # Parse UUID from the Citadel's response
        # TODO: It'd better use query API other than using UUID directly.
        uuid = res['result']['uuid']
        # Put time series data
        res = citadel.put_timeseries(uuid, ts_data)
        if not res['success']:
            failed_point_list.append(srcid)
    except:
        pdb.set_trace()

# Store failed points for future review
with open('%s_failed_points.json'%dataset_name, 'w') as fp:
    json.dump(failed_point_list, fp)
