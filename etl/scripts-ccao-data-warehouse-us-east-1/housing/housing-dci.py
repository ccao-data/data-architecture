import os
import tempfile
from datetime import datetime

import boto3
import pandas as pd
from dotenv import load_dotenv

# This script cleans the Distressed Communities Index data, mostly standardizing
# column names to our conventions, and filtering data to IL.

# Set up the S3 client
s3 = boto3.client("s3")
temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)

# Download the file from S3 to your local system
load_dotenv(".Renviron")
AWS_S3_WAREHOUSE_BUCKET = os.getenv("AWS_S3_WAREHOUSE_BUCKET")
AWS_S3_RAW_BUCKET = os.getenv("AWS_S3_RAW_BUCKET")[5:]  # type: ignore
file_key = os.path.join("housing", "dci", "dci.csv")

s3.download_file(AWS_S3_RAW_BUCKET, file_key, temp_file.name)

# Use pandas to read the Excel file, skipping the first two rows
data = pd.read_csv(temp_file.name)

data = data[data["State"] == "Illinois"]

# Current year of data construction is 2024. This column
# should be updated if https://eig.org/distressed-communities
#  has an updated dataset.

data["year"] = str(2021)
data["data_year"] = str(2024)

data = data[
    ["Zip Code", "2017-2021 Final Distress Score", "year", "data_year"]
].rename(
    columns={
        "Zip Code": "geoid",
        "2017-2021 Final Distress Score": "dci",
    }
)
data["geoid"] = data["geoid"].astype(str)

# Upload the Parquet file to S3
data["loaded_at"] = str(datetime.now())
data.to_parquet(
    os.path.join(
        os.environ["AWS_S3_WAREHOUSE_BUCKET"],
        "housing",
        "dci",
        "dci.parquet",
    ),
    index=False,
)
