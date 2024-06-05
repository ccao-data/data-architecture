import os
import tempfile

import boto3
import pandas as pd
from dotenv import load_dotenv

AWS_S3_WAREHOUSE_BUCKET = os.environ["AWS_S3_WAREHOUSE_BUCKET"]
AWS_S3_RAW_BUCKET = os.environ("AWS_S3_RAW_BUCKET")[5:]  # type: ignore

# Set up the S3 client
s3 = boto3.client("s3")
temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)

load_dotenv("etl/.Renviron")
# Manually include the year of file construction.
file_key = os.path.join("housing", "ari", "2023-ARI.xlsx")

s3.download_file(AWS_S3_RAW_BUCKET, file_key, temp_file.name)

# Use pandas to read the Excel file, skipping the first two rows
data = pd.read_excel(temp_file.name, skiprows=2, engine="openpyxl")

# Include a year column for the year of file construction.
# This should be changed if a more recent data file is uploaded.
data["year"] = str(2023)

data = data[["Census Tract", "Total ARI Score", "year"]].rename(
    columns={"Census Tract": "geoid", "Total ARI Score": "ari_score"}
)
data["geoid"] = data["geoid"].astype(str)
temp_file.close()


# Upload the Parquet file to S3
data.to_parquet(
    os.path.join(
        AWS_S3_WAREHOUSE_BUCKET,  # type: ignore
        "housing",  # type: ignore
        "ari",  # type: ignore
        "2023.parquet",  # type: ignore
    ),
    index=False,
)
