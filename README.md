This repository contains the dataset used to MetroInsight Project. It may be deprecated after stabilizing the webservice.

# Datsets

In each Dataset, you can find data in ./data and metadata in ./metadata.
UPDATE: You cannot find data here to avoid privacy leak. Please contact Jason Koh to get an access to Googld Drive data folder.

### Summary
| Data Source   |# Data Points | Granularity | Etc.  |   
|       ---     |     ---      |     ---     |  ---  |
| BuildingDepot |      7296    |    2 mins   | 5 buildings  |   
|  ION Dataset  |      955     |    2 mins   |       |   
| Google Traffic|      10      |   10 mins   | 5 routes     |   
- Data Periods: 2017.1.18 ~ 2017.1.25


## BuildingDepot

1. BuildingDepot contains building sensors. Currently a couple of building are included.
2. raw\_data/buildingdepot/metadata has all the metadata related to building sensors.
 1. building\_info.json has metadata related to buildings especially location.

## ION Dataset

 1. ION Dataset has energy consumption of multiple buildings. Energy consumption varies from electricity to thermal energy in water circulation. Its metadata will be augmented.
 2. Raw simple metadata is found in raw\_data/ion\_data/metadata/ion\_metadata.csv However, this is not machine redable. TODO: Need to parse it.

## Google Traffic

 1. It is collected from Google Travel Time API. Several routes in UCSD campus are being observed.
 2. Routes' metadata can be found in raw\_data/google\_traffic/metadata/routes\_metadata.json

## Mesowest Weather Data

 1. Contains weather sensor data collected via the Mesowest / SynopticLabs API.
 2. Metadata contains information about each sensor and the metrics collected, which are stored as csv files per-sensor in the data directory.

## Wifier data

 1. Cotnains Wifier project data consisting of MesoWest, NWS Digistal Forecast and HRRRX forcast.



# For developers,

## Directory structure
 - *_citadel_dump_uploader.py is a file to upload a bunch of static data of the dataset.
 - *_citadel_connector.py is a file to upload live data of the dataset continuously.

## Running instruction
 - Install citadel-python-client in citadel-python-client/ directory by the following steps:
   1. cd citadel-python-client
     2. python setup.py install
