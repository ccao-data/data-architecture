import os
import tempfile
from datetime import datetime

import boto3
import pandas as pd
from dotenv import load_dotenv

# Set up the S3 client
s3 = boto3.client("s3")
temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)

# Download the file from S3 to your local system
load_dotenv("etl/.Renviron")
AWS_S3_RAW_BUCKET = os.getenv("AWS_S3_RAW_BUCKET")[5:]  # type: ignore
file_key = os.path.join("housing", "dci", "dci.csv")

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
].rename(
    columns={
        "Zip Code": "geoid",
        "2017-2021 Final Distress Score": "dci",
    }
)

AWS_S3_WAREHOUSE_BUCKET = os.getenv("AWS_S3_WAREHOUSE_BUCKET")[5:]  # type: ignore  # noqa: E501

current_year = datetime.now().year

data.to_parquet(
    file_key=os.path.join(
        AWS_S3_WAREHOUSE_BUCKET,
        "housing",
        "dci",
        f"{current_year}.parquet",  # noqa: E501
    ),
    index=False,
)
