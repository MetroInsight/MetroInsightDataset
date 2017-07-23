import json
import pdb
import arrow



def normalize_data(uuid, results, metadata):
    uuid = metadata['uuid']
    geometryType = metadata['geometryType']
    coordinates = metadata['coordinates']
    try:
        data = [{
                    'uuid': uuid,
                    'value': list(result.values())[0],
                    'timestamp': arrow.get(list(result.keys())[0]).timestamp,
                    'geometryType': geometryType,
                    'coordinates': coordinates
                } for result in results]
    except:
        pdb.set_trace()
    return data


