import geopandas as gpd
import pandas as pd
import requests
import boto3
import os
from osmtogeojson import osmtogeojson

# Create an S3 connection
s3_client = boto3.client('s3')

# Retrieve golf course polygon data from the Overpass API
# using a query and save it as geodataframe
query = """
[out:json];

(
area[name="Cook County"][admin_level=6];
node["leisure"="golf_course"]["golf"!="driving_range"]["name"!="Mini-Golf"](area);

area[name="Cook County"][admin_level=6];
way["leisure"="golf_course"]["golf"!="driving_range"]["name"!="Mini-Golf"](area);

area[name="Cook County"][admin_level=6];
relation["leisure"="golf_course"]["golf"!="driving_range"]["name"!="Mini-Golf"](area);
);


out body;
>;
out skel qt;
"""

url = "http://overpass-api.de/api/interpreter"
r = requests.get(url, params={'data': query})

result = osmtogeojson.process_osm_json(r.json())
osm_df = gpd.GeoDataFrame.from_features(result)
osm_df = osm_df.set_crs(4326)
osm_df = osm_df.to_crs(3435)

# Get rid of indoor golf courses and rename the id column
osm_df = osm_df[~osm_df['description'].str.contains('indoor', na=False)]
osm_df = osm_df.rename(columns={"@id": "id"})

# Universe of PINs which have a golf course class (535) from the "Assessor - Parcel Universe", year 2021
parcel_uni_res = requests.get("https://datacatalog.cookcountyil.gov/resource/tx2p-k2g9.json?class=535&year=2021&$select=pin10")
parcel_uni_df = pd.DataFrame.from_dict(parcel_uni_res.json())

# Use the list of class 535 PINs from the parcel universe to get the parcel polygons from
# Cook County's parcel shapefile
query_string = "https://datacatalog.cookcountyil.gov/resource/77tz-riq7.geojson?$where=PIN10 in" + \
  str(tuple(parcel_uni_df.pin10.tolist()))

parcel_shp_res = requests.get(query_string)
parcel_shp_df = gpd.GeoDataFrame.from_features(parcel_shp_res.json())
parcel_shp_df = parcel_shp_df.set_crs(4326)
parcel_shp_df = parcel_shp_df.to_crs(3435)

# Keep any polygons that exist in both OSM and the Assessor parcel shapefile
overlap_osm_df = gpd.sjoin(osm_df, parcel_shp_df, how="inner", predicate="contains")

# Rename the column and drop the duplicate to merge with overlap_osm_df
overlap_osm_df = overlap_osm_df.rename(columns={"@id": "id"})
overlap_osm_df = overlap_osm_df["id"].drop_duplicates()

# Keep anything in OSM that is NOT in the parcel shapefile
no_overlap_osm_df = pd.merge(osm_df, overlap_osm_df, how='outer', left_on='id', right_on='id', indicator=True)
no_overlap_osm_df = no_overlap_osm_df.loc[no_overlap_osm_df['_merge']=='left_only']
no_overlap_osm_df = no_overlap_osm_df.loc[~no_overlap_osm_df.id.isin(["way/231037058", "way/1025843449"])]["id"]

# Anything in the parcel shapefile and NOT in OSM is reviewed manually
parcel_shp_df_no_osm = gpd.sjoin(parcel_shp_df, osm_df[["id", "geometry"]], how="left", predicate="within")
parcel_shp_df_no_osm = parcel_shp_df_no_osm.loc[parcel_shp_df_no_osm["id"].isnull()]
parcel_shp_df_no_osm = parcel_shp_df_no_osm.drop_duplicates(subset=['pin10'], keep=False)

# Manually reviewed list of parcels to keep
parcel_shp_pins_to_keep = [
    '0205300002','0206400011','0208100002','0216103003','0216300001',
    '0216302002','0217201002','0217201008','0217400026','0402300046','0402301027','0414400021',
    '0414400022','0414400023','0414405003','0414406004','0415200017','0423301007','0423301008',
    '0423302009','0423302011','0426100038','0427300009','0427302007','0435405005','0436103003',
    '0507306001','0529204003','0708101010','0532308001','0532402001','0532308001','0532402001',
    '0708101010','0708101018','0708200007','0708200011','0926111001','1008300007','1022101016',
    '1022200058','1022200059','1034400001','1235202023','1236300003','1318200002','1318201002',
    '1318201003','1519301004','1519301006','1519400007','1519400009','1519400011','1519401005',
    '1519401007','1526200001','1526201008','1526204003','1526204004','1526204009','1526204010',
    '1530301021','1808206001','1808402001','1808402002','1829101007','1829102001','1829300005',
    '1829301001','2215201001','2222201002','2222201005','2222302001','2222400001','2223200002',
    '2223201009','2223201011','2223201016','2223201018','2223300006','2223301008','2223302003',
    '2226100013','2226401008','2227101003','2227200013','2234103011','2234106001','2234110002',
    '2235201014','2334302002','2334410001','2401212001','2401212003','2413116001','2413209001',
    '2413218001','2708206010','2708405005','2711201006','2711201011','2711300001','2711301002',
    '2711301003','2711400008','2711400010','2711400017','2711400018','2711402002','2711402003',
    '2733400001','2809101001','2809202006','2836413013','2930300103','2930300005','3113303043',
    '3207201003','3207300001','3207300002','3207301001','3207301002'
]

parcel_shp_df_no_osm_file = parcel_shp_df_no_osm.loc[parcel_shp_df_no_osm.pin10.isin(parcel_shp_pins_to_keep)]

# Concatenate 3 data sources (OSM overlapping with parcels, OSM not overlapping with parcels,
# and manually reviewed parcels)
osm_df_concat = pd.concat([overlap_osm_df, no_overlap_osm_df])
osm_df_concat = gpd.GeoDataFrame(osm_df_concat)

parcel_shp_fil = parcel_shp_df_no_osm_file[["geometry", "pin10"]].rename(columns={"pin10": "id"})
osm_df_concat = pd.DataFrame(osm_df_concat).merge(osm_df[['id', 'geometry']], on='id', how='left')
df_final = pd.concat([osm_df_concat, parcel_shp_fil])

# Save concatenated data to a single geojson file with 1 column (ID)
gdf_final = gpd.GeoDataFrame(df_final, geometry="geometry", crs=3435).to_crs(4326)
gdf_final.to_file('/tmp/golf_course.geojson', driver='GeoJSON')

# Write file to S3
AWS_S3_RAW_BUCKET = os.environ.get("AWS_S3_RAW_BUCKET")
output_path = os.path.join("spatial", "environment", "golf_course", "2022.geojson")
s3_client.upload_file("/tmp/golf_course.geojson", AWS_S3_RAW_BUCKET, output_path)
