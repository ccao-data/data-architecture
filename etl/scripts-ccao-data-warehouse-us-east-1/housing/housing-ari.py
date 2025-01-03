import os
import tempfile
from datetime import datetime

import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv(".Renviron")
AWS_S3_WAREHOUSE_BUCKET = os.getenv("AWS_S3_WAREHOUSE_BUCKET")
AWS_S3_RAW_BUCKET = os.getenv("AWS_S3_RAW_BUCKET")[5:]  # type: ignore

# Set up the S3 client
s3 = boto3.client("s3")
temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)

# Manually include the year of file construction.
file_key = os.path.join("housing", "ari", "2023-ARI.xlsx")

s3.download_file(AWS_S3_RAW_BUCKET, file_key, temp_file.name)

# Use pandas to read the Excel file, skipping the first two rows
data = pd.read_excel(temp_file.name, skiprows=2, engine="openpyxl")

# Year refers to final year of census data.
# Data_year refers to year of data construction.
data["data_year"] = str(2023)
data["year"] = str(2021)

data = data[["Census Tract", "Total ARI Score", "year", "data_year"]].rename(
    columns={"Census Tract": "geoid", "Total ARI Score": "ari_score"}
)
data["geoid"] = data["geoid"].astype(str)
temp_file.close()

# Upload the Parquet file to S3
data["loaded_at"] = str(datetime.now())
data.to_parquet(
    os.path.join(
        os.environ["AWS_S3_WAREHOUSE_BUCKET"],
        "housing",
        "ari",
        "ari.parquet",
    ),
    index=False,
)
