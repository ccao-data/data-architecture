import os
import tempfile
from datetime import datetime
from io import BytesIO

import boto3
import pandas as pd
from dotenv import load_dotenv

# Set up the S3 client
s3 = boto3.client("s3")
temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)

load_dotenv("etl/.Renviron")
AWS_S3_RAW_BUCKET = os.getenv("AWS_S3_RAW_BUCKET")[5:]
file_key = os.path.join("housing", "ari_index", "2023-ARI.xlsx")

s3.download_file(AWS_S3_RAW_BUCKET, file_key, temp_file.name)

# Use pandas to read the Excel file, skipping the first two rows
data = pd.read_excel(temp_file.name, skiprows=2, engine="openpyxl")
current_year = datetime.now().year
data["year"] = current_year
data = data[["Census Tract", "Total ARI Score", "year"]].rename(
    columns={"Census Tract": "geoid", "Total ARI Score": "ari_score"}
)
temp_file.close()

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
upload_df_to_s3_as_parquet(data, AWS_S3_WAREHOUSE_BUCKET, file_key)
