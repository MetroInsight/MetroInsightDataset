import json
import pdb
from os import walk

import arrow
import pandas as pd

from building_depot import DataService
from citadel import Citadel
import config

citadel = Citadel(config.CITADEL_URL)
ion_dir = config.BASE_DIR + '/raw_data/ion_data'
data_dir = ion_dir + '/data'
base_url = config.CITADEL_URL

with open(ion_dir + '/metadata/ion_metadata.json', 'r') as fp:
    metadata_dict = json.load(fp)

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
    metadata = metadata_dict[srcid]
    data = pd.Series.from_csv(filename)
    try:
        assert(isinstance(metadata['latitude'], float))
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
        ts_data = dict((int(arrow.get(k).timestamp),float(v)) for k,v in data.iteritems())
    except:
        failed_point_list.append(srcid)
        continue
    try:
        res = citadel.create_point(new_metadata)
        if not res['success']:
            if json.loads(res['result']['reason'])['reason'] == 'Given name already exists':
                continue
        uuid = res['result']['uuid']
        res = citadel.put_timeseries(uuid, ts_data)
        if not res['success']:
            failed_point_list.append(srcid)
    except:
        pdb.set_trace()

with open('ion_failed_points.json', 'w') as fp:
    json.dump(failed_point_list, fp)
