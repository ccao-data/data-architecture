import os
from datetime import datetime

import boto3
import osmnx as ox
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv


def s3_object_exists(bucket, key):
    s3 = boto3.client("s3")
    s3.head_object(Bucket=bucket, Key=key)


def write_geoparquet(gdf, remote_file):
    table = pa.Table.from_pandas(gdf)
    pq.write_table(table, remote_file)


# Set up environment variables and paths
load_dotenv("etl/.Renviron")
AWS_S3_WAREHOUSE_BUCKET = os.environ.get("AWS_S3_WAREHOUSE_BUCKET")[5:]
output_bucket = os.path.join(
    AWS_S3_WAREHOUSE_BUCKET, "spatial", "environment"
)  # flake8: noqa
current_year = datetime.now().year


# Function to query OpenStreetMap for grocery stores in Cook County
def query_osm_for_grocery_stores():
    # Query for grocery stores in Cook County
    tags = {"shop": ["supermarket", "greengrocer"]}
    gdf = ox.geometries_from_place("Cook County, Illinois, USA", tags)
    return gdf


# Iterate over the years and process data
for year in range(2014, current_year + 1):
    remote_file = os.path.join(
        output_bucket,
        "grocery_store",
        f"year={year}",
        f"grocery_store-{year}.parquet",
    )

    if not s3_object_exists(AWS_S3_WAREHOUSE_BUCKET, remote_file):
        supermarkets = query_osm_for_grocery_stores()
        write_geoparquet(supermarkets, remote_file)
