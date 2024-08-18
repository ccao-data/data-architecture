import os

import boto3
import requests
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

file_url = "https://www.ihda.org/wp-content/uploads/2023/07/2023-ARI.xlsx"


def download_excel(file_url):
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
            )
        }
        response = requests.get(file_url, headers=headers)
        response.raise_for_status()
        return response.content
    except requests.exceptions.HTTPError as e:
        raise Exception(f"Failed to download the file: {e}")


def upload_to_s3(file_content, bucket, key_prefix, file_name):
    try:
        s3 = boto3.resource("s3")
        object_key = f"{key_prefix}/{file_name}"
        s3.Bucket(bucket).put_object(Key=object_key, Body=file_content)
        return f"File uploaded to {bucket}/{object_key}"
    except NoCredentialsError:
        raise Exception("Credentials not available for AWS S3")
    except Exception as e:
        raise Exception(f"An error occurred: {e}")


load_dotenv(".Renviron")
AWS_S3_RAW_BUCKET = os.environ.get("AWS_S3_RAW_BUCKET")[5:]  # type: ignore
key_prefix = os.path.join("housing", "ari")

data = download_excel(file_url)

file_name = file_url.split("/")[-1]

result = upload_to_s3(data, AWS_S3_RAW_BUCKET, key_prefix, file_name)
