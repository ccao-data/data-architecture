# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
##### Import Packages #####
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

import boto3
import geopandas as gpd
import pandas as pd
import s3fs
from os import listdir
from sklearn.neighbors import BallTree

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
##### Connect to AWS #####
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

paths = {
    'parcels': 'stable/spatial/tax/parcel/',
    'pumas': 'stable/spatial/census/puma/',
    'tracts': 'stable/census/acs5/tract/',
    'ssas': 'stable/spatial/tax/ssa/',
    'roads': 'stable/spatial/environment/major_road/',
    'ohare': 'stable/spatial/environment/ohare_flight_path/',
    'fema_floodplains': 'stable/spatial/environment/flood_fema/',
    'fs_floodplains': 'stable/environment/flood_first_street/'
    }

# create connection
token = input('one time use MFA token: ')

client = boto3.client(
    service_name='s3',
    region_name='us-east-1',
    aws_session_token=token
    )

# declare location of data
root = 's3://ccao-landing-us-east-1/'
bucket = 'ccao-landing-us-east-1'

# files should be named after the year they pertain to, we want the most recent file
paths = {key: root + val + max([(a['Key']) for a in client.list_objects(Bucket = bucket, Prefix = val)['Contents']]) for key, val in paths.items()}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
##### Gather Data #####
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

data = {
    key:
    gpd.read_file(val, rows = 100000) if 'spatial' in val
    #gpd.read_file(val) if 'spatial' in val
    else pd.read_parquet(val,
    engine = 'pyarrow')
    for key, val in paths.items()
    }

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
##### Clean Parcel Data #####
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# this shapefile defines the universe of PINs we'll be working with, as well as their spatial locations

# parcels should load as 4326 CRS
if data['parcels'].crs != 4326:
    raise Exception("parcel CRS not as expected")

# drop rows not associated with a PIN
parcels = data['parcels'].dropna(subset=['pin10'])
parcels.index = parcels.reset_index().index

# rename lat/long
parcels = parcels.rename(columns={'longitude':'centroid_x', 'latitude':'centroid_y'})

# format township
parcels['township_name'] = parcels['politicaltownship'].str.replace('Town of ', '')

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
parcels['geometry'] = parcels['geometry'].centroid

# format and rename PIN column
parcels['PIN'] = parcels['pin10'] + '0000'
parcels = parcels.rename(columns={'pin10':'PIN10'})

# drop unneeded columns
parcels = parcels.drop(['pinu', 'pinb', 'pinp', 'pinsa', 'pina', 'area'], axis = 1)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
##### Clean Data for Joins #####
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# PUMAS

# re-project, only keep geometry column and identiefier columns
pumas = data['pumas'].to_crs(3435)
pumas['PUMA'] = pumas['GEOID10'].str[2:7]
pumas = pumas[['PUMA', 'geometry']]

# TRACTS

# drop margin of error columns and suffix on estimate columns
tracts = data['tracts'].drop(data['tracts'].filter(regex='E$').columns, axis = 1)
tracts.columns = tracts.columns.str.rstrip('M')

# combine other races into one category (same technique as racial dot map)
other = ['o1', 'o2', 'o3', 'o4', 'am_ind', 'pac_isl']
tracts['other'] = tracts[other].sum(axis = 1)
tracts = tracts.drop(columns = other)

# convert race pops to percentage
to_perc = ['white', 'black', 'asian', 'his', 'other']
tracts = tracts.rename(columns = {'tot_pop':'tract_pop', 'geoid':'censustract_geoid'})
tracts[to_perc] = tracts[to_perc].divide(tracts['tract_pop'], axis = 'rows')
tracts = tracts.rename(columns = dict(zip(to_perc, [suf + '_perc' for suf in to_perc])))

# create column to merge tract geometry and demographics, merge
tracts['TRACTCE'] = tracts['censustract_geoid'].str[-6:]

# only keep columns of interest
tracts = tracts[['censustract_geoid', 'midincome', 'tract_pop'] + [suf + '_perc' for suf in to_perc] + ['TRACTCE']]

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

# join tracts and third-party floodplains data by PIN
parcels_joined = parcels_joined.merge(tracts, how = 'left', on = 'censustract_geoid')
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
  'midincome', 'tract_pop', 'white_perc', 'black_perc',
  'asian_perc', 'his_perc', 'other_perc', 'elem_district', 'hs_district',
  'GEOID', 'TRACTCE'
]

# split parcels into missing and non-missing data frames
parcels_filled['centroid'] = parcels_filled.centroid
parcels_filled['has_na'] = parcels_filled[cols_to_fill_with_nn].isna().any(axis = 1)
parsplit = parcels_filled[parcels_filled['has_na'] == True]
parcels_filled = parcels_filled[parcels_filled['has_na'] == False]

# find the nearest point with non-missing data from the primary data frame

# create a BallTree
tree = BallTree(parcels_filled[['centroid_x', 'centroid_y']].values, leaf_size=2)

# query the BallTree on each feature from 'appart' to find the distance
# to the nearest 'pharma' and its id
parsplit['id_nearest'] = tree.query(
    parsplit[['centroid_x', 'centroid_y']].values, # The input array for the query
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
  'PIN10', 'centroid_x', 'centroid_y', 'point_parcel', 'multiple_geographies', 'primary_polygon',
  'GEOID', 'TRACTCE', 'tract_pop', 'white_perc', 'black_perc', 'asian_perc', 'his_perc', 'other_perc', 'midincome',
  'PUMA', 'FIPS', 'municipality', 'township_name', 'commissioner_dist', 'reps_dist', 'senate_dist', 'ward', 'ssa_name', 'ssa_no', 'tif_dist',
  'ohare_noise', 'floodplain', 'fs_flood_factor', 'fs_flood_risk_direction', 'withinmr100', 'withinmr101300',
'elem_district', 'hs_district'
]]

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
##### Upload to S3 #####
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

parcels_filled_no_missing.to_parquet('s3://ccao-staging-us-east-1/modeling/DTBL_PINLOCATIONS/DTBL_PINLOCATIONS.parquet', index = False)