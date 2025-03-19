import datetime
import os
from datetime import datetime
import tempfile
import boto3
import geopandas as gpd
import pyarrow.parquet as pq

# Set up AWS credentials
AWS_S3_RAW_BUCKET = os.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET = os.getenv("AWS_S3_WAREHOUSE_BUCKET")
s3 = boto3.client("s3")

current_year = datetime.now().strftime("%Y")


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

# Bike Trail Data Processing
def process_bike_trail(spark_session):
    bike_key_raw = f"{AWS_S3_RAW_BUCKET}/spatial/access/bike_trail/2021.geojson"
    bike_key_warehouse = f"{AWS_S3_WAREHOUSE_BUCKET}/spatial/access/bike_trail/year=2021/part-0.parquet"

    if not object_exists(AWS_S3_WAREHOUSE_BUCKET, bike_key_warehouse):
        with tempfile.NamedTemporaryFile(suffix=".geojson") as temp:
            s3.download_file(AWS_S3_RAW_BUCKET, "spatial/access/bike_trail/2021.geojson", temp.name)
            df_bike = gpd.read_file(temp.name).to_crs(epsg=4326)
            df_bike.columns = map(str.lower, df_bike.columns)
            df_bike["geometry_3435"] = df_bike["geometry"].to_crs(epsg=3435)
            df_bike = df_bike.rename(
                columns={
                    "spdlimit": "speed_limit",
                    "onstreet": "on_street",
                    "edtdate": "edit_date",
                    "trailwdth": "trail_width",
                    "trailtype": "trail_type",
                    "trailsurfa": "trail_surface",
                }
            ).drop(columns=["created_us", "shape_stle"])

            spark_df = spark_session.createDataFrame(df_bike)
            spark_df.write.parquet(bike_key_warehouse)

# Park Data Processing
def process_parks(spark_session):
    park_key_warehouse = f"{AWS_S3_WAREHOUSE_BUCKET}/spatial/access/park/year=2021/part-0.parquet"

    if not object_exists(AWS_S3_WAREHOUSE_BUCKET, park_key_warehouse):
        with tempfile.NamedTemporaryFile(suffix=".geojson") as temp:
            cook_boundary_key = "spatial/ccao/county/2019.parquet"
            s3.download_file(AWS_S3_WAREHOUSE_BUCKET, cook_boundary_key, temp.name)
            cook_boundary = gpd.read_parquet(temp.name).to_crs(epsg=4326)

            parks_df = gpd.read_file("path/to/local/park_data.geojson").to_crs(epsg=4326)
            parks_df["geometry_3435"] = parks_df["geometry"].to_crs(epsg=3435)
            parks_df_filtered = parks_df.loc[parks_df.intersects(cook_boundary.unary_union)]

            spark_df = spark_session.createDataFrame(parks_df_filtered)
            spark_df.write.parquet(park_key_warehouse)

# Industrial Corridor Data Processing
def process_industrial_corridor(spark_session):
    indc_key_raw = f"{AWS_S3_RAW_BUCKET}/spatial/access/industrial_corridor/2013.geojson"
    indc_key_warehouse = f"{AWS_S3_WAREHOUSE_BUCKET}/spatial/access/industrial_corridor/year=2013/part-0.parquet"

    if not object_exists(AWS_S3_WAREHOUSE_BUCKET, indc_key_warehouse):
        with tempfile.NamedTemporaryFile(suffix=".geojson") as temp:
            s3.download_file(AWS_S3_RAW_BUCKET, "spatial/access/industrial_corridor/2013.geojson", temp.name)
            df_indc = gpd.read_file(temp.name).to_crs(epsg=4326)
            df_indc.columns = map(str.lower, df_indc.columns)
            df_indc["geometry_3435"] = df_indc["geometry"].to_crs(epsg=3435)
            df_indc = df_indc.rename(
                columns={
                    "name": "name",
                    "region": "region",
                    "no": "num",
                    "hud_qualif": "hud_qualif",
                }
            ).loc[:, ["name", "region", "num", "hud_qualif", "acres", "geometry", "geometry_3435"]]

            spark_df = spark_session.createDataFrame(df_indc)
            spark_df.write.parquet(indc_key_warehouse)

# Cemetery Data Processing
def process_cemetery(spark_session):
    ceme_key_raw = f"{AWS_S3_RAW_BUCKET}/spatial/access/cemetery/2021.geojson"
    ceme_key_warehouse = f"{AWS_S3_WAREHOUSE_BUCKET}/spatial/access/cemetery/year=2021/part-0.parquet"

    if not object_exists(AWS_S3_WAREHOUSE_BUCKET, ceme_key_warehouse):
        with tempfile.NamedTemporaryFile(suffix=".geojson") as temp:
            s3.download_file(AWS_S3_RAW_BUCKET, "spatial/access/cemetery/2021.geojson", temp.name)
            df_ceme = gpd.read_file(temp.name).to_crs(epsg=4326)
            df_ceme.columns = map(str.lower, df_ceme.columns)
            df_ceme["geometry_3435"] = df_ceme["geometry"].to_crs(epsg=3435)
            df_ceme = df_ceme.rename(columns={"cfname": "name"}).loc[
                :, ["name", "address", "gniscode", "source", "community", "comment", "mergeid", "geometry", "geometry_3435"]
            ]

            spark_df = spark_session.createDataFrame(df_ceme)
            spark_df.write.parquet(ceme_key_warehouse)

# Walkability Data Processing
def process_walkability(spark_session):
    walk_key_raw = f"{AWS_S3_RAW_BUCKET}/spatial/access/walkability/2017.geojson"
    walk_key_warehouse = f"{AWS_S3_WAREHOUSE_BUCKET}/spatial/access/walkability/year=2017/part-0.parquet"

    if not object_exists(AWS_S3_WAREHOUSE_BUCKET, walk_key_warehouse):
        with tempfile.NamedTemporaryFile(suffix=".geojson") as temp:
            s3.download_file(AWS_S3_RAW_BUCKET, "spatial/access/walkability/2017.geojson", temp.name)
            df_walk = gpd.read_file(temp.name).to_crs(epsg=4326)
            df_walk.columns = map(str.lower, df_walk.columns)
            df_walk.columns = [col.replace("sc", "_score") for col in df_walk.columns]
            df_walk.rename(
                columns={
                    "walkabilit": "walkability_rating",
                    "amenities": "amenities_score",
                    "transitacc": "transitaccess",
                },
                inplace=True,
            )
            df_walk = gpd.standardize_expand_geo(df_walk)
            df_walk["year"] = "2017"

            spark_df = spark_session.createDataFrame(df_walk)
            spark_df.write.parquet(walk_key_warehouse)
