This repository contains the dataset used to MetroInsight Project. It may be deprecated after stabilizing the webservice.


### Datsets

In each Dataset, you can find data in ./data and metadata in ./metadata.
UPDATE: You cannot find data here to avoid privacy leak. Please contact Jason Koh to get an access to Googld Drive data folder.

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

## More to come
1. Weather center data
2. UCSD weather measurement
3. Air pollution data
