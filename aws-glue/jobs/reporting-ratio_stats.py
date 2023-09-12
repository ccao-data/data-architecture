from assesspy import (
    boot_ci,
    cod,
    cod_ci as cod_boot,
    cod_met,
    detect_chasing,
    mki,
    mki_met,
    prb,
    prb_met,
    prd,
    prd_met,
    prd_ci as prd_boot,
)

from assesspy.sales_chasing import detect_chasing_cdf, detect_chasing_dist

import warnings
import statsmodels.api as sm
import time
import s3fs
import re
import pandas as pd
import numpy as np
import math
import io
from statsmodels.distributions.empirical_distribution import ECDF
import boto3
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Define AWS boto3 clients
athena_client = boto3.client("athena")
glue_client = boto3.client("glue", region_name="us-east-1")
s3_client = boto3.client("s3")

# Define s3 and Athena paths
athena_db = "iasworld"

s3_bucket = "ccao-data-warehouse-us-east-1"
s3_prefix = "reporting/ratio_stats/"
s3_output = "s3://" + s3_bucket + "/" + s3_prefix
s3_ratio_stats = "s3://" + s3_bucket + "/" + s3_prefix + "ratio_stats.parquet"


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
SQL_QUERY = "SELECT * FROM reporting.vw_ratio_stats;"


# Run run_query_get_result to get file like object ----
r_file_object = run_query_get_result(
    athena_client, s3_bucket, SQL_QUERY, athena_db, s3_output, s3_prefix
)

# Retrieve s3 location of Athena query result and retrieve it
target = "s3://" + s3_bucket + "/" + r_file_object.key

pull = pd.read_csv(target)
pull = pull[pull.ratio > 0 & pull.ratio.notnull()]

# Delete all query results for this job from s3 bucket
response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

for object in response["Contents"]:
    if re.search("csv", object["Key"]):
        print("Deleting", object["Key"])
        s3_client.delete_object(Bucket=s3_bucket, Key=object["Key"])


def median_boot(ratio, nboot=100, alpha=0.05):
    return boot_ci(np.median, nboot=nboot, alpha=alpha, ratio=ratio)


def ccao_median(x):
    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = x.between(x.quantile(0.05), x.quantile(0.95), inclusive="neither")

    x_no_outliers = x[no_outliers]

    median_n = sum(no_outliers)

    median_val = np.median(x_no_outliers)
    median_ci = median_boot(x, nboot=1000)
    median_ci = f"{median_ci[0]}, {median_ci[1]}"

    out = [median_val, median_ci, median_n]

    return out


def ccao_mki(fmv, sale_price):
    ratio = fmv / sale_price

    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = ratio.between(
        ratio.quantile(0.05), ratio.quantile(0.95), inclusive="neither"
    )

    fmv_no_outliers = fmv[no_outliers]
    sale_price_no_outliers = sale_price[no_outliers]

    mki_n = sum(no_outliers)

    if mki_n >= 20:
        mki_val = mki(fmv_no_outliers, sale_price_no_outliers)
        met = mki_met(mki_val)

        out = [mki_val, met, mki_n]

    else:
        out = [None, None, mki_n]

    return out


def ccao_cod(ratio):
    """ """

    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = ratio[
        ratio.between(ratio.quantile(0.05), ratio.quantile(0.95), inclusive="neither")
    ]

    cod_n = no_outliers.size

    if cod_n >= 20:
        cod_val = cod(no_outliers)
        cod_ci = cod_boot(no_outliers, nboot=1000)
        cod_ci = f"{cod_ci[0]}, {cod_ci[1]}"
        met = cod_met(cod_val)
        out = [cod_val, cod_ci, met, cod_n]

    else:
        out = [None, None, None, cod_n]

    return out


def ccao_prd(fmv, sale_price):
    ratio = fmv / sale_price

    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = ratio.between(
        ratio.quantile(0.05), ratio.quantile(0.95), inclusive="neither"
    )

    fmv_no_outliers = fmv[no_outliers]
    sale_price_no_outliers = sale_price[no_outliers]

    prd_n = sum(no_outliers)

    if prd_n >= 20:
        prd_val = prd(fmv_no_outliers, sale_price_no_outliers)
        prd_ci = prd_boot(fmv_no_outliers, sale_price_no_outliers, nboot=1000)
        prd_ci = f"{prd_ci[0]}, {prd_ci[1]}"
        met = prd_met(prd_val)

        out = [prd_val, prd_ci, met, prd_n]

    else:
        out = [None, None, None, prd_n]

    return out


def ccao_prb(fmv, sale_price):
    ratio = fmv / sale_price

    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = ratio.between(
        ratio.quantile(0.05), ratio.quantile(0.95), inclusive="neither"
    )

    fmv_no_outliers = fmv[no_outliers]
    sale_price_no_outliers = sale_price[no_outliers]

    prb_n = sum(no_outliers)

    if prb_n >= 20:
        prb_model = prb(fmv_no_outliers, sale_price_no_outliers)
        prb_val = prb_model["prb"]
        prb_ci = prb_model["95% ci"]
        prb_ci = f"{prb_ci[0]}, {prb_ci[1]}"
        met = prb_met(prb_val)

        out = [prb_val, prb_ci, met, prb_n]

    else:
        out = [None, None, None, prb_n]

    return out


def report_summarise(df, geography_id, geography_type):
    # Aggregates data and calculates summary statistics for given groupings

    group_cols = [
        "year",
        "triad",
        "geography_type",
        "property_group",
        "assessment_stage",
        "geography_id",
        "sale_year",
    ]

    df["geography_id"] = pull[geography_id]
    df["geography_type"] = geography_type

    # Remove groups with less than three observations
    df["n"] = df.groupby(group_cols)["ratio"].transform("count")
    df = df[df["n"] > 2]
    df = df.groupby(group_cols).apply(
        lambda x: pd.Series(
            {
                "sale_n": np.size(x["triad"]),
                "ratio": ccao_median(x["ratio"]),
                "cod": ccao_cod(ratio=x["ratio"]),
                "mki": ccao_mki(fmv=x["fmv"], sale_price=x["sale_price"]),
                "prd": ccao_prd(fmv=x["fmv"], sale_price=x["sale_price"]),
                "prb": ccao_prb(fmv=x["fmv"], sale_price=x["sale_price"]),
                "detect_chasing": detect_chasing(ratio=x["ratio"]),
                "within_20_pct": sum(abs(1 - x["ratio"]) <= 0.20),
                "within_10_pct": sum(abs(1 - x["ratio"]) <= 0.10),
                "within_05_pct": sum(abs(1 - x["ratio"]) <= 0.05),
            }
        )
    )

    df[["median_ratio", "median_ratio_ci", "median_ratio_n"]] = pd.DataFrame(
        df.ratio.tolist(), index=df.index
    )
    df[["cod", "cod_ci", "cod_met", "cod_n"]] = pd.DataFrame(
        df.cod.tolist(), index=df.index
    )
    df[["mki", "mki_met", "mki_n"]] = pd.DataFrame(df.mki.tolist(), index=df.index)
    df[["prd", "prd_ci", "prd_met", "prd_n"]] = pd.DataFrame(
        df.prd.tolist(), index=df.index
    )
    df[["prb", "prb_ci", "prb_met", "prb_n"]] = pd.DataFrame(
        df.prb.tolist(), index=df.index
    )
    df["ratio_met"] = abs(1 - df["median_ratio"]) <= 0.05
    df["vertical_equity_met"] = df.prd_met | df.prb_met

    # Arrange output columns
    df = df[
        [
            "sale_n",
            "median_ratio",
            "median_ratio_ci",
            "cod",
            "cod_ci",
            "cod_n",
            "prd",
            "prd_ci",
            "prd_n",
            "prb",
            "prb_ci",
            "prb_n",
            "mki",
            "mki_n",
            "detect_chasing",
            "ratio_met",
            "cod_met",
            "prd_met",
            "prb_met",
            "mki_met",
            "vertical_equity_met",
            "within_20_pct",
            "within_10_pct",
            "within_05_pct",
        ]
    ].reset_index()

    return df


# Append and write output to s3 bucket
pd.concat(
    [
        report_summarise(pull, "triad", "Tri"),
        report_summarise(pull, "township_code", "Town"),
    ]
).to_parquet(s3_ratio_stats)

# Trigger reporting glue crawler
glue_client.start_crawler(Name="ccao-data-warehouse-reporting-crawler")
