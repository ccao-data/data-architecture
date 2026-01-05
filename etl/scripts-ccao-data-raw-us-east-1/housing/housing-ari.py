import io
import os

import boto3
import pandas as pd
import requests
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

# Define the two known ARI sources
file_urls = {
    "2023": "https://www.ihda.org/wp-content/uploads/2023/07/2023-ARI.xlsx",
    "2025": "https://www.ihda.org/wp-content/uploads/2025/08/Final_ARI_2025.csv",
}


def download_file(file_url: str) -> bytes:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        )
    }
    response = requests.get(file_url, headers=headers)
    response.raise_for_status()
    return response.content


def upload_to_s3(file_content, bucket, key_prefix, file_name):
    s3_client = boto3.client("s3")
    object_key = f"{key_prefix}/{file_name}"
    s3_client.put_object(Bucket=bucket, Key=object_key, Body=file_content)
    return f"s3://{bucket}/{object_key}"

load_dotenv(".Renviron")
AWS_S3_RAW_BUCKET = os.environ.get("AWS_S3_RAW_BUCKET")[5:]  # type: ignore
key_prefix = os.path.join("housing", "ari")

for data_year, file_url in file_urls.items():
    data = download_file(file_url)
    file_name = file_url.split("/")[-1]

    # Read source file (they uploaded with both csv and xlsx extensions)
    if file_name.endswith(".xlsx"):
        df = pd.read_excel(io.BytesIO(data), engine="openpyxl")
    elif file_name.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(data))
    else:
        raise Exception(f"Unsupported file format: {file_name}")

    # Write CSV to memory
    output = io.StringIO()
    df.to_csv(output, index=False)

    # Upload
    s3_key_name = f"ARI-{data_year}.csv"
    result = upload_to_s3(
        file_content=output.getvalue().encode("utf-8"),
        bucket=AWS_S3_RAW_BUCKET,
        key_prefix=key_prefix,
        file_name=s3_key_name,
    )

    print(f"Uploaded {s3_key_name} → {result}")
