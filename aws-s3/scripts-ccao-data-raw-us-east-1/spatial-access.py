import os
import shutil
import tempfile
import urllib.request

import boto3
import geopandas as gpd
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv

dotenv_path = ".py.env"
load_result = load_dotenv(dotenv_path)

load_dotenv("./data-architecture/aws-s3/py.env")
# Set AWS S3 variables
AWS_S3_RAW_BUCKET = os.getenv("AWS_S3_RAW_BUCKET")
output_bucket = os.path.join(print(AWS_S3_RAW_BUCKET), "spatial", "access")

# List APIs from city site
sources_list = [
    # INDUSTRIAL CORRIDORS
    {
        "source": "https://data.cityofchicago.org/api/geospatial/",
        "api_url": "e6xh-nr8w?method=export&format=GeoJSON",
        "boundary": "industrial_corridor",
        "year": "2013",
    },
    # PARKS
    {
        "source": "https://opendata.arcgis.com/datasets/",
        "api_url": "74d19d6bd7f646ecb34c252ae17cd2f7_7.geojson",
        "boundary": "park",
        "year": "2021",
    },
]


# Function to call referenced API, pull requested data, and write it to S3
def get_data_to_s3(source, api_url, boundary, year):
    s3 = boto3.client("s3")

    response = urllib.request.urlopen(source + api_url)
    data = response.read().decode("utf-8")

    s3_path = os.path.join(output_bucket, boundary, f"{year}.geojson")

    try:
        s3.put_object(Bucket=os.environ["AWS_S3_RAW_BUCKET"], Key=s3_path, Body=data)
    except ClientError as e:
        print(e)


for source in sources_list:
    get_data_to_s3(**source)

# CMAP WALKABILITY
# 2017 Data is no longer available online
raw_walk = pd.DataFrame(
    {
        "url": [
            "https://services5.arcgis.com/LcMXE3TFhi1BSaCY/arcgis/rest/services/Walkability/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
        ],
        "year": ["2018"],
    }
)


def get_walkability(url, year):
    s3_uri = os.path.join(output_bucket, "walkability", f"{year}.geojson")

    if not os.path.exists(s3_uri):
        tmp_file = tempfile.NamedTemporaryFile(suffix=".geojson", delete=False)
        tmp_dir = os.path.join(tempfile.gettempdir(), "walkability")

        # Grab file from CTA, recompress without .htm file
        with urllib.request.urlopen(url) as response, open(
            tmp_file.name, "wb"
        ) as out_file:
            shutil.copyfileobj(response, out_file)

        s3 = boto3.client("s3")
        s3_path = os.path.join(output_bucket, "walkability", f"{year}.geojson")

        with open(tmp_file.name, "rb") as file:
            s3.upload_fileobj(file, os.environ["AWS_S3_RAW_BUCKET"], s3_path)

        os.unlink(tmp_file.name)
        shutil.rmtree(tmp_dir, ignore_errors=True)


for index, row in raw_walk.iterrows():
    get_walkability(row["url"], row["year"])
