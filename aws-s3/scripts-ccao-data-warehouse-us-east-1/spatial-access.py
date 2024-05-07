import boto3
import geopandas as gpd
import pyarrow.parquet as pq
import tempfile
import os
import datetime
from utils import standardize_expand_geo

# Set up AWS credentials
AWS_S3_RAW_BUCKET = os.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET = os.getenv("AWS_S3_WAREHOUSE_BUCKET")
s3 = boto3.client('s3')
current_year = datetime.strftime("%Y")

# Helper function to save to S3
def upload_to_s3(local_file, bucket, s3_key):
    s3.upload_file(local_file, bucket, s3_key)

# Helper function to check if an S3 object exists
def object_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False

##### BIKE TRAIL #####
bike_key_raw = f"{AWS_S3_RAW_BUCKET}/spatial/access/bike_trail/2021.geojson"
bike_key_warehouse = f"{AWS_S3_WAREHOUSE_BUCKET}/spatial/access/bike_trail/year=2021/part-0.parquet"

if not object_exists(AWS_S3_WAREHOUSE_BUCKET, bike_key_warehouse):
    with tempfile.NamedTemporaryFile(suffix=".geojson") as temp:
        s3.download_file(AWS_S3_RAW_BUCKET, "spatial/access/bike_trail/2021.geojson", temp.name)
        df_bike = gpd.read_file(temp.name).to_crs(epsg=4326)
        df_bike.columns = map(str.lower, df_bike.columns)
        df_bike['geometry_3435'] = df_bike['geometry'].to_crs(epsg=3435)
        df_bike = df_bike.rename(columns={
            'spdlimit': 'speed_limit',
            'onstreet': 'on_street',
            'edtdate': 'edit_date',
            'trailwdth': 'trail_width',
            'trailtype': 'trail_type',
            'trailsurfa': 'trail_surface'
        }).drop(columns=['created_us', 'shape_stle'])
        pq.write_table(df_bike.to_parquet(), temp.name)
        upload_to_s3(temp.name, AWS_S3_WAREHOUSE_BUCKET, bike_key_warehouse)

# Similar structures follow for other datasets (e.g., cemetery, hospital, park, industrial corridor, and walkability).

##### CEMETERY #####
ceme_key_raw = f"{AWS_S3_RAW_BUCKET}/spatial/access/cemetery/2021.geojson"
ceme_key_warehouse = f"{AWS_S3_WAREHOUSE_BUCKET}/spatial/access/cemetery/year=2021/part-0.parquet"

if not object_exists(AWS_S3_WAREHOUSE_BUCKET, ceme_key_warehouse):
    with tempfile.NamedTemporaryFile(suffix=".geojson") as temp:
        s3.download_file(AWS_S3_RAW_BUCKET, "spatial/access/cemetery/2021.geojson", temp.name)
        df_ceme = gpd.read_file(temp.name).to_crs(epsg=4326)
        df_ceme.columns = map(str.lower, df_ceme.columns)
        df_ceme['geometry_3435'] = df_ceme['geometry'].to_crs(epsg=3435)
        df_ceme = df_ceme.rename(columns={
            'cfname': 'name'
        }).loc[:, ['name', 'address', 'gniscode', 'source', 'community', 'comment', 'mergeid', 'geometry', 'geometry_3435']]
        pq.write_table(df_ceme.to_parquet(), temp.name)
        upload_to_s3(temp.name, AWS_S3_WAREHOUSE_BUCKET, ceme_key_warehouse)

# And continue with other datasets...

##### WALKABILITY #####
walk_key_raw = f"{AWS_S3_RAW_BUCKET}/spatial/access/walkability/2017.geojson"
walk_key_warehouse = f"{AWS_S3_WAREHOUSE_BUCKET}/spatial/access/walkability/year=2017/part-0.parquet"

if not object_exists(AWS_S3_WAREHOUSE_BUCKET, walk_key_warehouse):
    with tempfile.NamedTemporaryFile(suffix=".geojson") as temp:
        s3.download_file(AWS_S3_RAW_BUCKET, "spatial/access/walkability/2017.geojson", temp.name)
        df_walk = gpd.read_file(temp.name).to_crs(epsg=4326)
        df_walk.columns = map(str.lower, df_walk.columns)
        df_walk.columns = [col.replace('sc', '_score') for col in df_walk.columns]
        df_walk.rename(columns={'walkabilit': 'walkability_rating', 'amenities': 'amenities_score', 'transitacc': 'transitaccess'}, inplace=True)
        df_walk = standardize_expand_geo(df_walk)
        df_walk['year'] = '2017'
        pq.write_table(df_walk.to_parquet(), temp.name)
        upload_to_s3(temp.name, AWS_S3_WAREHOUSE_BUCKET, walk_key_warehouse)
