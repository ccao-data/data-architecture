# type: ignore
# pylint: skip-file
# sc.addPyFile(  # noqa: F821
#     "s3://ccao-dbt-athena-dev-us-east-1/packages/spark-packages.zip"
# )

import os
import tempfile

import boto3
import pandas as pd

# Set up the S3 client
s3 = boto3.client("s3")
temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)

# Download the file from S3 to your local system
AWS_S3_RAW_BUCKET = "s3://ccao-data-raw-us-east-1/"
file_key = os.path.join("housing", "ari_index", "2023-ARI.xlsx")

s3.download_file(AWS_S3_RAW_BUCKET, file_key, temp_file.name)

# Use pandas to read the Excel file, skipping the first two rows
data = pd.read_excel(temp_file.name, skiprows=2, engine="openpyxl")

temp_file.close()
