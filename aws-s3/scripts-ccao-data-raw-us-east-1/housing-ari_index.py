import os

import boto3
import requests
from botocore.exceptions import NoCredentialsError
from bs4 import BeautifulSoup


def get_most_recent_ihs_data_url(base_url):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        )
    }
    response = requests.get(base_url, headers=headers)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    links = soup.find_all("a", href=True)

    xlsx_links = [
        link["href"] for link in links if link["href"].endswith(".xlsx")
    ]
    if xlsx_links:
        most_recent_xlsx = xlsx_links[0]
        if not most_recent_xlsx.startswith("http"):
            most_recent_xlsx = requests.compat.urljoin(
                base_url, most_recent_xlsx
            )
        return most_recent_xlsx
    else:
        return None


def download_and_upload_excel(file_url, bucket, key_prefix):
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
            )
        }
        response = requests.get(file_url, headers=headers)
        response.raise_for_status()

        # Initialize a session using boto3
        session = boto3.Session()
        s3 = session.resource("s3")

        # Upload directly from memory
        object_key = f"{key_prefix}/{file_url.split('/')[-1]}"
        s3.Bucket(bucket).put_object(Key=object_key, Body=response.content)

        return f"File uploaded to {bucket}/{object_key}"
    except requests.exceptions.HTTPError as e:
        return f"Failed to download the file: {e}"
    except NoCredentialsError:
        return "Credentials not available for AWS S3"
    except Exception as e:
        return f"An error occurred: {e}"


# URL of the webpage to scan for Excel files
base_url = (
    "https://www.ihda.org/developers/market-research/affordability-risk-index/"
)
most_recent_file_url = get_most_recent_ihs_data_url(base_url)

# S3 Bucket and Key Prefix
AWS_S3_RAW_BUCKET = os.environ.get("AWS_S3_RAW_BUCKET")
key_prefix = os.path.join("housing", "ari_index")

if most_recent_file_url:
    result = download_and_upload_excel(
        most_recent_file_url, AWS_S3_RAW_BUCKET, key_prefix
    )
    print(result)
else:
    print("No .xlsx file found on the page.")
