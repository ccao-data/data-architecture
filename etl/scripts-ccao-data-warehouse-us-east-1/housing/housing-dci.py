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

data = data[data["State"] == "Illinois"]

current_year = datetime.now().year
data["year"] = current_year

data = data[["Zip Code", "2017-2021 Final Distress Score", "year"]].rename(
    columns={
        "Zip Code": "geoid",
        "2017-2021 Final Distress Score": "dci",
    }
)

AWS_S3_WAREHOUSE_BUCKET = os.getenv("AWS_S3_WAREHOUSE_BUCKET")  # type: ignore  # noqa: E501

current_year = datetime.now().year

data.to_parquet(
    os.path.join(
        AWS_S3_WAREHOUSE_BUCKET,
        "housing",
        "dci",
        "dci.parquet",
    ),
    index=False,
)
