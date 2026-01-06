import os
from io import StringIO

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv

# This script downloads Distressed Communities Index data
# from Economic Innovation Group. It is a zip-code level
# measure of economic well-being.

def load_csv_from_url(url):
    # Headers to simulate a browser request
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/58.0.3029.110 Safari/537.3"
        )
    }
    # Send a GET request to the URL with the headers
    response = requests.get(url, headers=headers)
    # Raise an exception if the request was unsuccessful
    response.raise_for_status()

    data = StringIO(response.text)
    # Read the data into a DataFrame
    df = pd.read_csv(data)

    return df


def upload_df_to_s3(df, bucket, file_name):
    """Uploads a DataFrame to S3 as a CSV file."""
    # Get an S3 client
    s3 = boto3.client("s3")
    # Create a buffer
    csv_buffer = StringIO()
    # Write DataFrame to buffer
    df.to_csv(csv_buffer, index=False)
    # Upload buffer content to S3
    s3.put_object(Bucket=bucket, Key=file_name, Body=csv_buffer.getvalue())


# URL of the file we want to upload.
# This should be manually checked to see if data more recent
# than 2024 is included.
file_url = "https://eig.org/dci-maps-2023/data/1cd12716-de4a-4ef6-884b-af6e1066b581.csv"  # noqa: E501

# Load the data
df = load_csv_from_url(file_url)

load_dotenv(".Renviron")
AWS_S3_RAW_BUCKET = os.getenv("AWS_S3_RAW_BUCKET")[5:]  # type: ignore
file_name = os.path.join("housing", "dci", "dci.csv")


# Upload the DataFrame to S3
upload_df_to_s3(df, AWS_S3_RAW_BUCKET, file_name)
