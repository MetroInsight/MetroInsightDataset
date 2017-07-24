import sys
import os.path
import gwd  # Watchdog timer
from ftplib import FTP  # For communicating with FTP server
import xml.etree.ElementTree as ET  # For reading XML file for metadata
import json
import pdb
import os

from influxdb import InfluxDBClient  # For writing to influxdb database
import arrow  # For formatting the time format
import pandas as pd  # For reading and interpreting the data

from citadel_helper import *

data_types = ["FLOW", "OCCUPANCY", "SPEED", "VMT", "VHT", "DELAY"] # Alter this and dict to modify data types used.
uuid_dict = dict()
raw_metadata_dict = {}
metadata_dict = {}



def make_name(vds_name, data_type):
    return vds_name + '_' + data_type

def init_points(metadata_dict):
    for name, metadata in metadata_dict.items():
        if metadata.get('uuid'):
            continue
        points = find_points({'name': name})
        if not points:
            print(name, 'not found')
            uuid = create_point(subset_dict(metadata, ['name', 'unit', 'pointType']))
        else:
            uuid = points[0]['uuid']
        uuid_dict[name] = uuid
        metadata_dict[name]['uuid'] = uuid
    with open('metadata/caltrans_metadata.json', 'w') as fp:
        json.dump(metadata_dict, fp, indent=2)

def main(argv):
    # Declare static variables
    global raw_metadata_dict
    global metadata_dict 
    raw_metadata_dict, metadata_dict = init_metadata()
    filename = "5minagg_latest.txt"  # Altered to work with 5 minute aggregate data
    # Get VDS IDs
    IDs = get_ids()

    # Initiate watchdog
    wd_name = "ucsd.caltrans"
    gwd.auth(wd_name)

    init_points(metadata_dict)

    # Establish FTP connection and get necessary file
    try:
        ftp = FTP("pems.dot.ca.gov")
        ftp.login(argv[0], argv[1])
        ftp.cwd("D11/Data/5min")  # This is to work with District 11's 5 minute raw data
        ftp.retrbinary("RETR " + filename, open(filename, "wb").write)
        ftp.quit()
    except:  # TODO: Add specific exceptions for each type of error (see ftplib). Can also do this for other exceptions.
        error_log("FTP Error. Please make sure you entered the username and password for the FTP server as arguments and are able to connect to the server. Exited script")
        sys.exit(1)

    # Open raw data file and get timestamp
    with open(filename, "r") as data_file:
        timestamp = data_file.readline()
    # Convert timestamp to ISO time format and in UTC-0:00 time zone
    iso_timestamp = arrow.get(timestamp, "MM/DD/YYYY HH:mm:ss").replace(tzinfo="US/Pacific")

    # Parse data file using Pandas API
    df = pd.read_csv("5minagg_latest.txt", skiprows = 1, header = None)  # Parses data into Pandas dataframe. Skips first line (timestamp).
    df.columns = ["VDS_ID", "FLOW", "OCCUPANCY", "SPEED", "VMT", "VHT", "Q", "TRAVEL_TIME", "DELAY", "NUM_SAMPLES", "PCT_OBSERVED"]  # Renames headers for the columns. This is based off of provided documentation.

    df = df.set_index("VDS_ID")  #Sets VDS_ID to index for efficient use

    # Get data (flow and occupancy) from file and write to influxdb for all the IDs.
    error_flag = False
    for ID in IDs:
        try:
            data = get_data(df, data_types, int(ID))
        except:
            error_log("Data for %s could not be found. Will not be written to database." % (ID))
            data = {"FLOW": None, "OCCUPANCY": None, "SPEED": None, "VMT": None, "VHT": None, "DELAY": None}

        if data:  # Checks to see if any data was received
            write_influxdb(ID, data, data_types, iso_timestamp.timestamp)
        else:  # Watchdog
            gwd.fault(wd_name, "No data found at {0}".format(ID))  # Notify that there is a fault
            error_flag = True
    if not error_flag:  # Will run if error flag is false (positive). This means that data for all the IDs was found.
        gwd.kick(wd_name, 600)  # Notify that this is running correctly. This signal is valid for 600 seconds.


def init_metadata():
    ids = get_ids()
    raw_metadata_file = 'metadata/raw_caltrans_metadata.json'
    if os.path.isfile(raw_metadata_file):
        with open(raw_metadata_file, 'r') as fp:
            raw_metadata_dict = json.load(fp)
    else:
        raw_metadata_dict = dict()
    metadata_file = 'metadata/caltrans_metadata.json'
    if os.path.isfile(metadata_file):
        with open(metadata_file, 'r') as fp:
            metadata_dict = json.load(fp)
    else:
        metadata_dict = dict()

    for i in ids:
        if i not in raw_metadata_dict:
            raw_metadata = get_metadata(i)
            for data_type in data_types:
                name = make_name(raw_metadata['name'], data_type)
                try:
                    metadata = {
                        'name': name,
                        'geometryType': 'point',
                        'coordinates': [[float(raw_metadata['longitude']),
                                         float(raw_metadata['latitude'])]],
                        'unit': 'test_unit',
                        #'unit': unit_map[data_type], #TODO: IMPLEMENT
                        'pointType': data_type
                        }
                except:
                    pdb.set_trace()
                metadata_dict[name] = metadata
            raw_metadata_dict[i] = raw_metadata

    with open(raw_metadata_file, 'w') as fp:
        json.dump(raw_metadata_dict, fp, indent=2)
    with open(metadata_file, 'w') as fp:
        json.dump(metadata_dict, fp, indent=2)


    return raw_metadata_dict, metadata_dict

def get_ids():
    """
    Returns VDS IDs that's data will be extracted from the file and posted to the influxdb server.
    To use the IDs.txt file method, use the vds_discovery.py script or enter specified IDs into the file manually (use line breaks).
    """
    if os.path.isfile("IDs.txt"): # If IDs.txt exists, use the IDs in the file
        with open("IDs.txt") as IDs_file:
            IDs = IDs_file.readlines()  # Read all IDs into list
            IDs = [ID.strip() for ID in IDs]  # Removes whitespace, notably the newline
    else: # Otherwise use hardcoded pre-determined IDs
        IDs = ["1108498", "1108719", "1123087", "1123086", "1108452", "1118544", "1125911", "1123072", "1123081",
               "1123064"]
    return IDs


def get_data(data_df, data_types, VDS_ID):
    """
    Returns the data for a specified VDS (based upon ID passed) using the passed Pandas Dataframe.
    Note that VDS_ID must be passed as an integer variable (typecasted from string).
    """
    # Declare data dictionary. Must match data_types.
    data = {"FLOW": None, "OCCUPANCY": None, "SPEED": None, "VMT": None, "VHT": None, "DELAY": None}

    # Get data for each data type
    for data_type in data_types:
        data[data_type] = data_df.loc[VDS_ID].loc[data_type]

    # Return the values
    return data


def write_influxdb(VDS_ID, data, data_types, timestamp):
    """
    Gets metadata and writes data (along with metadata) to influxdb database.
    Data values being stored is based upon data_type dictionary.
    Gets metadata using get_metadata() function.
    Writes each data value using write_point() function.   
    """
    # Get metadata for writing to database as tags
    #metadata = get_metadata(VDS_ID)
    metadata = raw_metadata_dict[VDS_ID]

    citadel_data = list()

    # Write measurements to database.
    for data_type in data_types:
        if data[data_type]:  # Checks if data type was found
            data_point = write_point(data_type, VDS_ID, metadata, data[data_type], timestamp)
            citadel_data.append(data_point)
        else:  # Writes error if a value was not found for a specific VDS
            error_log("Could not retrieve data type %s's value for VDS %s. This is a common issue." % (data_type, VDS_ID))
    if not post_data(citadel_data):
        print('Failed at posting: {0}'.format(VDS_ID))
        pdb.set_trace()


def get_metadata(identifier):
    """Gets metadata from XML file for use when writing data to database."""
    # Declare metadata dictionary
    metadata = {"name": None, "type": None, "country_id": None, "city_id": None, "freeway_id": None, "freeway_dir": None, "lanes": None, "cal_pm": None, "abs_pm": None, "latitude": None, "longitude": None, "last_modified": None}

    # Parse XML file for metadata
    tree = ET.parse("vds_config.xml")
    root = tree.getroot()
    stations = root[11][1]  # Gets detector_stations child tag in XML file for District 11

    # Finds correct VDS based upon identifier and stores its metadata to dictionary
    for vds in stations.findall("vds"):
        if identifier == vds.get("id"):
            metadata["name"] = vds.get("name")
            metadata["type"] = vds.get("type")
            metadata["county_id"] = vds.get("county_id")
            metadata["city_id"] = vds.get("city_id")
            metadata["freeway_id"] = vds.get("freeway_id")
            metadata["freeway_dir"] = vds.get("freeway_dir")
            metadata["lanes"] = vds.get("lanes")
            metadata["cal_pm"] = vds.get("cal_pm")
            metadata["abs_pm"] = vds.get("abs_pm")
            metadata["latitude"] = vds.get("latitude")
            metadata["longitude"] = vds.get("longitude")
            metadata["last_modified"] = vds.get("last_modified")
            return metadata


def write_point(data_type, identifier, metadata_tags, value, timestamp):
    """
    Writes data point to database.
    Documentation used: http://influxdb-python.readthedocs.io/en/latest/include-readme.html#documentation
    """
    # Creating json body for measurement.
    data_point = [
        {
            "measurement": data_type,
            "tags": {
                "ID": identifier,
                "name": metadata_tags["name"],
                "type": metadata_tags["type"],
                "county_id": metadata_tags["county_id"],
                "city_id": metadata_tags["city_id"],
                "freeway_id": metadata_tags["freeway_id"],
                "freeway_dir": metadata_tags["freeway_dir"],
                "lanes": metadata_tags["lanes"],
                "cal_pm": metadata_tags["cal_pm"],
                "abs_pm": metadata_tags["abs_pm"],
                "latitude": metadata_tags["latitude"],
                "longitude": metadata_tags["longitude"],
                "last_modified": metadata_tags["last_modified"]
            },
            "time": timestamp,
            "fields": {
                "value": value
            }
        }
    ]

    # Write to database
    try:
        client = InfluxDBClient('localhost', 8086, 'root', 'root', 'caltran_traffic')  # caltran_traffic is given name of database
        client.create_database("caltran_traffic")  # Will create database if it does not exist. Otherwise, does not modify database.
        client.write_points(data_point)
    except:
        error_log("Could not write data for {} due to error with InfluxDB database".format(identifier))

    # Write to Citadel
    name = make_name(metadata_tags['name'], data_type)
    uuid = metadata_dict[name]['uuid']
    data_point = subset_dict(metadata_dict[name], ['geometryType', 'uuid', 'coordinates'])
    data_point['value'] = value
    data_point['timestamp'] = timestamp * 1000
    return data_point


def error_log(error):
    # TODO: Replace this with the logging API.
    """For logging errors that occur to the log file."""
    with open("error.log", "a") as file:
        file.write("%s\n" % (error))


# Executes from here
if __name__ == "__main__":
    main(sys.argv[1:])




