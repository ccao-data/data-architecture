import os
import re

import boto3
import pandas as pd
import pyarrow.parquet as pq

# Load environment variables
AWS_S3_RAW_BUCKET = os.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET = os.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket = f"{AWS_S3_WAREHOUSE_BUCKET}/housing/ihs_index"

# Initialize S3 client
s3 = boto3.client("s3")


# Get the list of files from the raw S3 bucket
def get_bucket_keys(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in response:
        return [content["Key"] for content in response["Contents"]]
    return []


# Find the raw file
raw_keys = get_bucket_keys(AWS_S3_RAW_BUCKET, "housing/ihs_index/")
raw_file_key = raw_keys[0] if raw_keys else None

if raw_file_key:
    # Download the raw file locally
    local_raw_file = "/tmp/ihs_index.parquet"
    s3.download_file(AWS_S3_RAW_BUCKET, raw_file_key, local_raw_file)

    # Read the parquet file
    df = pd.read_parquet(local_raw_file)

    # Rename columns and modify geoid
    df.rename(columns={"puma": "geoid"}, inplace=True)
    df["geoid"] = df["geoid"].apply(lambda x: re.sub(r"p", "170", x))

    # Convert from wide to long
    df_long = df.melt(
        id_vars=["geoid", "name"], var_name="time", value_name="ihs_index"
    )

    # Split 'time' column into 'year' and 'quarter'
    df_long[["year", "quarter"]] = df_long["time"].str.split("Q", expand=True)
    df_long.drop(columns=["time"], inplace=True)

    # Reorder columns
    df_long = df_long[["geoid", "name", "ihs_index", "quarter", "year"]]


def model(dbt, spark_session):
    dbt.config(materialized="table")

    input = dbt.ref("housing/ihs_index")

    spark_df = spark_session.createDataFrame(df)

    return spark_df
