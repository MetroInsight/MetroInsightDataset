import json
import pdb
from os import walk

import arrow
import pandas as pd

from citadel import Citadel # Citadel python client library
    # https://github.com/MetroInsight/citadel-python-client
import config

# Instantiate a Citadel connector.
citadel = Citadel(config.CITADEL_URL)

# configuration
ion_dir = config.BASE_DIR + '/raw_data/ion_data'
data_dir = ion_dir + '/data'
base_url = config.CITADEL_URL

# Load predefined metadata
with open(ion_dir + '/metadata/ion_metadata.json', 'r') as fp:
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
    data = pd.Series.from_csv(filename)
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
                    'source_reference': 'ucsd_ion'
                    },
                'geometry':{
                    'coordinates': [metadata['latitude'], metadata['longitude']],
                    'type': 'Point'
                    }
                }
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
with open('ion_failed_points.json', 'w') as fp:
    json.dump(failed_point_list, fp)
