# Glue setup
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

import io
import re
import time
import warnings

# Import necessary libraries
import boto3
import pandas as pd
import s3fs

# Define AWS boto3 clients
athena_client = boto3.client("athena")
glue_client = boto3.client("glue", region_name="us-east-1")
s3_client = boto3.client("s3")

# Define s3 and Athena paths
athena_db = "iasworld"

s3_bucket = "ccao-data-warehouse-us-east-1"
s3_prefix = "reporting/res_report_summary/"
s3_output = "s3://" + s3_bucket + "/" + s3_prefix
s3_res_report_summary = (
    "s3://" + s3_bucket + "/" + s3_prefix + "res_report_summary.parquet"
)


# Functions to help with Athena queries ----
def poll_status(athena_client, execution_id):
    """Checks the status of the a query using an incoming execution id and returns
    a 'pass' string value when the status is either SUCCEEDED, FAILED or CANCELLED."""

    result = athena_client.get_query_execution(QueryExecutionId=execution_id)
    state = result["QueryExecution"]["Status"]["State"]

    if state == "SUCCEEDED":
        return "pass"
    if state == "FAILED":
        return "pass"
    if state == "CANCELLED":
        return "pass"
    else:
        return "not pass"


def poll_result(athena_client, execution_id):
    """Gets the query result using an incoming execution id. This function is ran after the
    poll_status function and only if we are sure that the query was fully executed."""

    result = athena_client.get_query_execution(QueryExecutionId=execution_id)

    return result


def run_query_get_result(
    athena_client, s3_bucket, query, database, s3_output, s3_prefix
):
    """Runs an incoming query and returns the output as an s3 file like object."""

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={
            "OutputLocation": s3_output,
        },
    )

    QueryExecutionId = response.get("QueryExecutionId")

    # Wait until query is executed
    while poll_status(athena_client, QueryExecutionId) != "pass":
        time.sleep(2)
        pass

    result = poll_result(athena_client, QueryExecutionId)

    r_file_object = None

    # Only return file like object when the query succeeded
    if result["QueryExecution"]["Status"]["State"] == "SUCCEEDED":
        print("Query SUCCEEDED: {}".format(QueryExecutionId))

        s3_key = s3_prefix + QueryExecutionId + ".csv"

        r_file_object = boto3.resource("s3").Object(s3_bucket, s3_key)

    return r_file_object


# Athena query ----
SQL_QUERY = "SELECT * FROM reporting.vw_res_report_summary;"

# Run run_query_get_result to get file like object ----
r_file_object = run_query_get_result(
    athena_client, s3_bucket, SQL_QUERY, athena_db, s3_output, s3_prefix
)

# Retrieve s3 location of Athena query result and retrieve it
target = "s3://" + s3_bucket + "/" + r_file_object.key

pull = pd.read_csv(target)

# Delete all query results for this job from s3 bucket
response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

for object in response["Contents"]:
    if re.search("csv", object["Key"]):
        print("Deleting", object["Key"])
        s3_client.delete_object(Bucket=s3_bucket, Key=object["Key"])

# Append and write output to s3 bucket
pull.to_parquet(s3_res_report_summary)

# Trigger reporting glue crawler
glue_client.start_crawler(Name="ccao-data-warehouse-reporting-crawler")
