
# coding: utf-8

# In[18]:

from building_depot import DataService, BDError
import json
import pandas as pd
import arrow
from collections import defaultdict
import re
from copy import deepcopy
import pdb


# In[2]:

with open("config/ion_config.json", "r") as fp:
    configs = json.load(fp)
    base_url = configs['base_url']
    #start_time = arrow.get(configs['start_time']).datetime
    #end_time = arrow.get(configs['end_time']).datetime
    start_time = arrow.get().shift(hours=-24).datetime
    end_time = arrow.get().datetime
with open("config/bd2_3secrets.json", "r") as fp:
    cred = json.load(fp)
    apikey = cred['api_key']
    username = cred['username']
bd = DataService(base_url, apikey, username)


# In[3]:

def make_series(bd_data):
    time_list = list()
    value_list = list()
    for datum in data:
        time_list.append(datum.keys()[0])
        value_list.append(datum.values()[0])
    return pd.Series(index=time_list, data=value_list)


# In[4]:

#bd = DataService(base_url, apikey, username)
orig_metadata_df = pd.DataFrame.from_csv('metadata/ion.txt', )


# In[5]:

srcid_dict = dict()
metadata_dict = defaultdict(dict)
for srcid, row in orig_metadata_df.iterrows():
    uuid = row['uuid']
    data = bd.get_timeseries_datapoints(uuid, "PresentValue", start_time, end_time)['timeseries']
    if len(data)>1:
        sensor = bd.view_sensor(uuid)
        metadata_dict[srcid]['bd_uuid'] = uuid
        metadata_dict[srcid]['name'] = sensor['source_identifier']
        #make_series(data).to_csv(datadir+uuid+'.csv')
metadata_dict = dict(metadata_dict)


# In[6]:

metadata_dict_backup = deepcopy(metadata_dict)


# In[7]:

def building_id_extractor(name):
    campus_name = name.split('.')[0]
    building_name =  '_'.join(name.split('.')[1].split(':')[0].split('_')[:-1])
    return campus_name + '_' + building_name

def point_type_extractor(name):
    return name.split(':')[-1]


# In[8]:

#building_name_set = set()
#for srcid, metadata in metadata_dict.items():
#    name = metadata['name']
#    building_name_set.add(building_id_extractor(name))
#with open('./metadata/ion_building_set.json', 'w') as fp:
#    json.dump(
#        dict([(building_name, "") for building_name in building_name_set]), fp, indent=2, sort_keys=True)    


# In[9]:

with open('metadata/building_info.json' ,'r') as fp:
    building_info = json.load(fp)
with open('metadata/ion_unit_map.json', 'r') as fp:
    unit_map = json.load(fp)
with open('metadata/ion_point_map.json', 'r') as fp:
    point_map = json.load(fp)
with open('metadata/ion_building_mapping.json', 'r') as fp:
    building_map = json.load(fp)


# In[23]:

for srcid, metadata in metadata_dict.items():
    name = metadata['name']
    building_id = building_id_extractor(name)
    building_name = building_map[building_id]
    if len(building_name)>0:
        building = building_info[building_name]
        metadata['latitude'] = building['location']['latitude']
        metadata['longitude'] = building['location']['longitude']
    point_id = point_type_extractor(name)
    point_type = point_map[point_id]
    unit = unit_map[point_id]
    metadata['point_type'] = point_type
    metadata['unit'] = unit
    #metadata_dict[srcid] = metadata
    
with open('metadata/raw_ion_metadata.json', 'w') as fp:
    json.dump(metadata_dict, fp, indent=4)


# In[ ]:



