import os
import tempfile
from io import BytesIO

import boto3
import pandas as pd
from dotenv import load_dotenv

# Set up the S3 client
s3 = boto3.client("s3")
temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)

# Download the file from S3 to your local system
load_dotenv("etl/.Renviron")
AWS_S3_RAW_BUCKET = os.getenv("AWS_S3_RAW_BUCKET")[5:]
file_key = os.path.join("housing", "dci_index", "dci_index.csv")

s3.download_file(AWS_S3_RAW_BUCKET, file_key, temp_file.name)

# Use pandas to read the Excel file, skipping the first two rows
data = pd.read_csv(temp_file.name)

data = data[data["County"] == "Cook County, Illinois"]

data = data[
    [
        "Zip Code",
        "2017-2021 Final Distress Score",
        "Quintile (5=Distressed)",
        "DCI Decile",
        "Rank of Zip w/in County (1=Most Prosperous)",
        "Number of Zips in County",
        "Rank of Zip w/in State (1=Most Prosperous)",
    ]
]

file_key = os.path.join("housing", "dci_index", "dci_index.parquet")

# Save the DataFrame to a Parquet file locally.
data.to_parquet("temp_file.parquet")


def upload_df_to_s3_as_parquet(df, bucket, file_name):
    """Uploads a DataFrame to S3 as a Parquet file."""
    # Get an S3 client
    s3 = boto3.client("s3")
    # Create a buffer
    parquet_buffer = BytesIO()
    # Write DataFrame to buffer in Parquet format
    df.to_parquet(parquet_buffer, index=False)
    # Upload buffer content to S3
    s3.put_object(Bucket=bucket, Key=file_name, Body=parquet_buffer.getvalue())


AWS_S3_WAREHOUSE_BUCKET = os.getenv("AWS_S3_WAREHOUSE_BUCKET")[5:]

# Upload the Parquet file to S3
upload_df_to_s3_as_parquet(data, AWS_S3_RAW_BUCKET, file_key)
