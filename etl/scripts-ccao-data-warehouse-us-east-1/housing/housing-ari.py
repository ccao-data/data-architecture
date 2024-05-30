import datetime
import os
import tempfile

import boto3
import pandas as pd
from dotenv import load_dotenv

# Set up the S3 client
s3 = boto3.client("s3")
temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)

current_year = datetime.datetime.now().year
load_dotenv("etl/.Renviron")
AWS_S3_RAW_BUCKET = os.getenv("AWS_S3_RAW_BUCKET")[5:]  # type: ignore
file_key = os.path.join("housing", "ari", f"ari_{current_year}.xlsx")

s3.download_file(AWS_S3_RAW_BUCKET, file_key, temp_file.name)

# Use pandas to read the Excel file, skipping the first two rows
data = pd.read_excel(temp_file.name, skiprows=2, engine="openpyxl")

data["year"] = str(current_year)

data = data[["Census Tract", "Total ARI Score", "year"]].rename(
    columns={"Census Tract": "geoid", "Total ARI Score": "ari_score"}
)
data["geoid"] = data["geoid"].astype(str)
temp_file.close()

AWS_S3_WAREHOUSE_BUCKET = os.getenv("AWS_S3_WAREHOUSE_BUCKET")

# Upload the Parquet file to S3
data.to_parquet(
    os.path.join(
        AWS_S3_WAREHOUSE_BUCKET,  # type: ignore
        "housing",  # type: ignore
        "ari",  # type: ignore
        f"ari_{current_year}.parquet",  # type: ignore
    ),
    index=False,
)
