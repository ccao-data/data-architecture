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
file_keys = {
    "2023": "housing/ari/ARI-2023.csv",
    "2025": "housing/ari/ARI-2025.csv",
}

data_frames = []

for year, file_key in file_keys.items():
    s3.download_file(AWS_S3_RAW_BUCKET, file_key, temp_file.name)

    # Use pandas to read the Excel file, skipping the first two rows
    # Add year column based on the file_keys
    temp_data = pd.read_csv(temp_file.name, skiprows=2)
    temp_data["year"] = year  # Add the year column based on the file key
    data_frames.append(temp_data)

# Combine all data frames into one
data = pd.concat(data_frames, ignore_index=True)

# Year refers to final year of census data.
# Data_year refers to year of data construction.
# For ARI 2023, census data is from 2021.
# For ARI 2025, census data is from 2023.
data_year_mapping = {"2023": 2021, "2025": 2023}
data["data_year"] = data.apply(
    lambda row: data_year_mapping[str(row["year"])], axis=1
)

data = data[["Census Tract", "Total ARI Score", "data_year", "year"]].rename(
    columns={"Census Tract": "geoid", "Total ARI Score": "ari_score"}
)
data["geoid"] = data["geoid"].astype(str)
temp_file.close()

# Upload the Parquet file to S3 partitioned by year
data["loaded_at"] = str(datetime.now())
data.to_parquet(
    os.path.join(
        os.environ["AWS_S3_WAREHOUSE_BUCKET"],
        "housing",
        "ari",
        "ari.parquet",
    ),
    index=False,
    partition_cols=["year"],
)
