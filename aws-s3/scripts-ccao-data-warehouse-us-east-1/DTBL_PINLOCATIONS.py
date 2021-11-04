# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
##### Import Packages #####
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

import boto3
import geopandas as gpd
import pandas as pd
import s3fs
import os
from sklearn.neighbors import BallTree
import gzip

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
##### Connect to AWS #####
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

paths = {
    'parcels': 'spatial/tax/parcel/',
    'pumas': 'spatial/census/puma/',
    'tracts': 'spatial/census/tract/',
    'ssas': 'spatial/tax/ssa/',
    'roads': 'spatial/environment/major_road/',
    'ohare': 'spatial/environment/ohare_flight_path/',
    'fema_floodplains': 'spatial/environment/flood_fema/',
    'fs_floodplains': 'environment/flood_first_street/',
    'fips': 'spatial/census/place/',
    'municipality':'spatial/political/municipality/',
    'township':'spatial/political/township/',
    'commissioner':'spatial/political/commissioner/',
    'congressional':'spatial/political/congressional_district/',
    'rep':'spatial/political/state_representative/',
    'senate':'spatial/political/state_senate/',
    'ward':'spatial/political/ward/',
    'tif':'spatial/tax/tif/',
    'school_elem':'spatial/school/school_district_elementary/',
    'school_hs':'spatial/school/school_district_secondary/',
    'school_uni':'spatial/school/school_district_unified/'
    }

# create connection
token = input('one time use MFA token: ')

client = boto3.client(
    service_name='s3',
    region_name='us-east-1',
    aws_session_token=token
    )

# declare location of data
root = 's3://ccao-data-raw-us-east-1/'
bucket = 'ccao-data-raw-us-east-1'

# files should be named after the year they pertain to, we want the most recent file
paths = {key: root + max([(a['Key']) for a in client.list_objects(Bucket = bucket, Prefix = val)['Contents']]) for key, val in paths.items()}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
##### Gather Data #####
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

data = {
    key:
    gpd.read_file(val) if 'spatial' in val
    else pd.read_parquet(val, engine = 'pyarrow')
    for key, val in paths.items()
    }

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
##### Gather Old Parcels #####
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# grab parcel files older than what's been detected as the most recent
old_parcel_paths = [root + "spatial/tax/parcel/" + x for x in os.listdir(root + "spatial/tax/parcel/")
if x.endswith(".geojson") and x != os.path.basename(paths['parcels'])]

current_pins = data['parcels']['pin10']

def import_old_pins(x):
    temp = gpd.read_file(x)
    temp = temp[~temp['pin10'].isin(current_pins)]
    temp['taxyr'] = os.path.splitext(os.path.basename(x))[0]
    return temp

old_parcels = {os.path.splitext(os.path.basename(x))[0]: import_old_pins(x) for x in old_parcel_paths}

# parcel shapefiles from before 2019 don't include political boundaries and so must be treated differently
old_parcels_combined = {}
old_parcels_combined = {key: (value.rename(columns = {'SHAPE__Area':'shape_star'}) if 'SHAPE__Area' in value.columns else value) for key, value in old_parcels.items() if int(key) < 2019}
for i in old_parcels_combined:

    # only keep the bar minimum for shapefiles before 2019, what's needed will be merged on below
    old_parcels_combined[i] = old_parcels_combined[i][['pin10', 'shape_star', 'geometry', 'taxyr']]

old_parcels_combined = pd.concat(old_parcels_combined.values(), ignore_index=True)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
##### Add Boundaries to Parcels Prior to 2019 Before Appending  #####
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# remove observations with no geometry
old_parcels_combined = old_parcels_combined[~old_parcels_combined.geometry.is_empty]

# generate lat and long, convert to centroid geometry
old_parcels_combined['latitude'] = old_parcels_combined.centroid.y
old_parcels_combined['longitude'] = old_parcels_combined.centroid.x
old_parcels_combined = old_parcels_combined.to_crs(3435)
old_parcels_combined['geometry'] = old_parcels_combined.centroid

# Perform spatial joins with all boundary datasets
for shape in ('tracts', 'fips', 'municipality', 'township', 'commissioner', 'rep', 'senate', 'ward', 'tif', 'congressional', 'school_elem', 'school_hs', 'school_uni'):

  df = data[shape]

  # rename critical columns before joining to avoid generic prefixes/suffixes as only identifiers
  if shape == 'tracts':
    df = df.rename(columns = {'GEOID':'censustract_geoid'})

  if shape == 'school_elem':
    df = df.rename(columns = {'AGENCY_DESCRIPTION':'elemschltaxdist'})

  if shape == 'school_hs':
    df = df.rename(columns = {'AGENCY_DESC':'highschltaxdist'})

  if shape == 'school_uni':
    df = df.rename(columns = {'MAX_AGENCY_DESC':'unitschltaxdist'})

  if shape == 'tif':
    df = df.rename(columns = {'AGENCY_DESCRIPTION':'tifdistrict'})

  if shape == 'congressional':
    df = df.rename(columns = {'district_1':'congressionaldistrict'})
    df = df[['geometry', 'congressionaldistrict']]

  if shape == 'commissioner':
    df['commissionerdistrict'] = df['district'].str.replace('st|nd|rd|th', '')

  if shape == 'township':
    df = df.rename(columns = {'NAME_TMP':'politicaltownship'})

  if shape == 'fips':
    df = df.rename(columns = {'PLACEFP':'municipalityfips'})

  if shape == 'municipality':
    df = df.rename(columns = {'municipali':'municipality'})

  if shape == 'rep':
    df = df.rename(columns = {'district_1':'staterepresentativedistrict'})

  if shape == 'senate':
    df = df.rename(columns = {'senatedist':'statesenatedistrict'})

  if shape == 'ward':
    df = df.rename(columns = {'ward':'chicagoward'})

  # perform joins
  old_parcels_combined = gpd.sjoin(old_parcels_combined, df.to_crs(3435), how = 'left', op = 'intersects')
  old_parcels_combined = old_parcels_combined.drop(['index_right'], axis = 1)

# add empty columns that exist in data we're appending to
for i in ['pinu', 'pinb', 'taxcode', 'pinp', 'pinsa', 'pina', 'assessornbhd', 'assessorbldgclass', 'parceltype']:
  old_parcels_combined[i] = ''

# only keep columns necessary for appending, re-project to same crs as main parcel file
keep = data['parcels'].columns.values.tolist()
keep.append('taxyr')
old_parcels_combined = old_parcels_combined[keep].to_crs(4326)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
##### Clean Parcel Data #####
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# this shapefile defines the universe of PINs we'll be working with, as well as their spatial locations

# parcels should load as 4326 CRS
if data['parcels'].crs != 4326:
    raise Exception("parcel CRS not as expected")

# parcels should load as 4326 CRS
if data['parcels'].crs != 4326:
    raise Exception("parcel CRS not as expected")

parcels = data['parcels']

# add tax year
parcels['taxyr'] = os.path.splitext(os.path.basename(paths['parcels']))[0]

# append old parcels
for i in old_parcels:
    if int(i) >= 2019:
        parcels = parcels.append(old_parcels[i])
parcels.reset_index()

# only use most recent parcel version of parcel
parcels['taxyr'] = pd.to_numeric(parcels['taxyr'])
parcels['most_recent'] = parcels.groupby('pin10', sort = False)['taxyr'].transform('max')
parcels = parcels[parcels['taxyr'] == parcels['most_recent']]

# drop rows not associated with a PIN
parcels = data['parcels'].dropna(subset=['pin10'])
parcels.index = parcels.reset_index().index

# format township
parcels['township_name'] = parcels['politicaltownship'].str.replace('Town of ', '')

# format tract
parcels['TRACTCE'] = parcels['censustract_geoid'].str[-6:]

# calculate polygon size using projected CRS, not geographic
parcels = parcels.to_crs(3435)
parcels['area'] = parcels['geometry'].area
parcels['point_parcel'] = (parcels['area'] < 0.5).astype(int)

# construct multiple polygon indicator (within pin10)
parcels['multiple_geographies'] = parcels.duplicated(subset = ['pin10'], keep = False)
parcels['multiple_geographies'] = parcels['multiple_geographies'].astype(int)

# construct indicator for largest polygons associated with each PIN
parcels['primary_polygon'] = 0
parcels.loc[parcels.groupby('pin10')['area'].idxmax(), 'primary_polygon'] = 1

# convert parcel polygons to centroids
parcels['geometry'] = parcels.centroid

# format and rename PIN column
parcels['PIN'] = parcels['pin10'] + '0000'
parcels = parcels.rename(columns={'pin10':'PIN10'})

# drop unneeded columns
parcels = parcels.drop(['pinu', 'pinb', 'pinp', 'pinsa', 'pina', 'area', 'most_recent'], axis = 1)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
##### Clean Data for Joins #####
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# PUMAS

# re-project, only keep geometry column and identiefier columns
pumas = data['pumas'].to_crs(3435)
pumas['PUMA'] = pumas['GEOID10'].str[2:7]
pumas = pumas[['PUMA', 'geometry']]

# CHICAGO SPECIAL SERVICE AREAS

ssas = data['ssas'].to_crs(3435)

# rename columns and only keep those of interest
ssas = ssas.rename(columns = {'name':'ssa_name', 'ref':'ssa_no'})
ssas = ssas[['ssa_name', 'ssa_no', 'geometry']]

# GEOGRAPHIC BOUNDARIES

# roads shapefile
roads = data['roads'].to_crs(3435)

# O'Hare noise zone boundary (derived from buffering O'Hare boundary data)
ohare = data['ohare']
ohare = ohare.rename(columns = {'AIRPORT':'ohare_noise'})

# FEMA flood data
fema_floodplains = data['fema_floodplains'].to_crs(3435)
fema_floodplains = fema_floodplains[['SFHA_TF', 'geometry']]
fema_floodplains = fema_floodplains.rename(columns = {'SFHA_TF':'floodplain'})
fema_floodplains['floodplain'] = fema_floodplains['floodplain'].replace({'T': 1, 'F': 0})

# clean third-party flood data
fs_floodplains = data['fs_floodplains']
fs_floodplains['PIN10'] = fs_floodplains['PIN'].str[0:10]
fs_floodplains = fs_floodplains.drop(columns = 'PIN')
fs_floodplains = fs_floodplains.drop_duplicates(subset=['PIN10'])

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
##### Join Data to PINs #####
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# get unique PINs inside of 100ft buffer
roads_100 = roads
roads_100['geometry'] = roads_100['geometry'].buffer(100)
roads_100 = gpd.overlay(parcels, roads_100, how = "intersection")['PIN']
roads_100 = roads_100.drop_duplicates()

# get unique PINs inside of 300ft buffer
roads_300 = roads
roads_300['geometry'] = roads_300['geometry'].buffer(300)
roads_300 = gpd.overlay(parcels, roads_300, how = "intersection")['PIN']
roads_300 = roads_300.drop_duplicates()
roads_300 = roads_300[~roads_300.isin(roads_100)] # Only need PINs outside of 100 and within 300

# generate road distance indicators for parcels file
parcels['withinmr100'] = parcels['PIN'].isin(roads_100).astype(int)
parcels['withinmr101300'] = parcels['PIN'].isin(roads_300).astype(int)

# create way to dedupe after possible duplication during spatial joins (spatial
# joins with overlapping boundaries such as TIFs will create duplicate rows)
parcels['row_num'] = parcels.index
parcels_joined = parcels

# perform spatial joins with all boundary datasets
for df in (ohare, pumas, ssas, fema_floodplains):

    parcels_joined = gpd.sjoin(parcels_joined, df, how = 'left', op = 'intersects')
    parcels_joined = parcels_joined.drop(['index_right'], axis = 1)

# join third-party floodplains data by PIN
parcels_joined = parcels_joined.merge(fs_floodplains, how = 'left', on = 'PIN10')

# drop any dupluicated rows due to joins
parcels_joined = parcels_joined.drop_duplicates()

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
##### Fill Missing Values #####
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# cleaning column names
parcels_filled = parcels_joined.rename(columns = {
  'commissionerdistrict':'commissioner_dist',
  'staterepresentativedistrict':'reps_dist',
  'statesenatedistrict':'senate_dist',
  'censustract_geoid':'GEOID',
  'elemschltaxdist':'elem_district',
  'highschltaxdist':'hs_district',
  'municipalityfips':'FIPS',
  'chicagoward':'ward',
  'tifdistrict':'tif_dist'
  })

# fill empty o'hare values with 0
parcels_filled['ohare_noise'] = parcels_filled['ohare_noise'].fillna(0)

# fill empty tif districts with NA
parcels_filled['tif_dist'].replace(0, pd.NA)

# fill missing elemntary and hs districts with unified districts when available
parcels_filled['elem_district'] = parcels_filled['elem_district'].fillna(parcels_filled['unitschltaxdist'])
parcels_filled['hs_district'] = parcels_filled['hs_district'].fillna(parcels_filled['unitschltaxdist'])

# goal here is to fill missing values for certain columns with the value of their
# nearest non-NA neighbor. Missingness here is usually caused by imperfect
# shapefiles (boundaries that don't cover 100% of PINs)
cols_to_fill_with_nn = [
  'withinmr100', 'withinmr101300', 'ohare_noise',
  'floodplain', 'fs_flood_factor', 'fs_flood_risk_direction',
  'commissioner_dist', 'reps_dist', 'senate_dist', 'PUMA',
  'elem_district', 'hs_district',
  'GEOID', 'TRACTCE'
]

# split parcels into missing and non-missing data frames
parcels_filled['has_na'] = parcels_filled[cols_to_fill_with_nn].isna().any(axis = 1)
parsplit = parcels_filled[parcels_filled['has_na'] == True]
parcels_filled = parcels_filled[parcels_filled['has_na'] == False]

# find the nearest point with non-missing data from the primary data frame

# create a BallTree
tree = BallTree(parcels_filled[['latitude', 'longitude']].values, leaf_size=2)

# query the BallTree on each feature from 'appart' to find the distance
# to the nearest 'pharma' and its id
parsplit['id_nearest'] = tree.query(
    parsplit[['latitude', 'longitude']].values, # The input array for the query
    k=1, # The number of nearest neighbors
    return_distance = False
)

# add nearest pin with no NAs as nearest neighbor for each PIN with NAs
parsplit['nearest_PIN'] = parcels_filled['PIN10'].iloc[parsplit['id_nearest']].tolist()
parsplit = parsplit.drop(columns='id_nearest')

# loop through columns and if there's an NA, replace it with value from nearest neighbor
for col in cols_to_fill_with_nn:
    for i in parsplit['PIN10']:

        nearest_PIN = parsplit[parsplit['PIN10'] == i]['nearest_PIN'].to_string(index = False)

        parsplit[parsplit['PIN10'] == i][col] = parcels_filled[parcels_filled['PIN10'] == nearest_PIN][col] if bool(pd.isna(parsplit[parsplit['PIN10'] == i][col].to_string(index = False))) else parsplit[parsplit['PIN10'] == i][col]

parsplit = parsplit.drop(columns='nearest_PIN')

# recombine and remove unnecessary geometry column
parcels_filled_no_missing = parcels_filled.append(parsplit)

# sort columns for readability
parcels_filled_no_missing = parcels_filled_no_missing[[
  'PIN10', 'latitude', 'longitude', 'point_parcel', 'multiple_geographies', 'primary_polygon',
  'GEOID', 'TRACTCE', 'PUMA', 'FIPS',
  'municipality', 'township_name', 'commissioner_dist', 'reps_dist', 'senate_dist', 'ward', 'ssa_name', 'ssa_no', 'tif_dist',
  'ohare_noise', 'floodplain', 'fs_flood_factor', 'fs_flood_risk_direction', 'withinmr100', 'withinmr101300',
  'elem_district', 'hs_district', 'taxyr', 'geometry'
]]

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
##### Upload to S3 #####
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# to_parquet is finicky about column types, lat and long can't upload as 'object' type
parcels_filled_no_missing['latitude'] = parcels_filled_no_missing['latitude'].astype('string')
parcels_filled_no_missing['longitude'] = parcels_filled_no_missing['longitude'].astype('string')

parcels_filled_no_missing.to_parquet('s3://ccao-data-warehouse-us-east-1/modeling/DTBL_PINLOCATIONS/DTBL_PINLOCATIONS.parquet', index = False)
