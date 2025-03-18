# This script generates aggregated summary stats on sales across a number of
# geographies, class combinations, and time.

import statistics as stats

# Import libraries
import numpy as np
import pandas as pd

# Declare class groupings
groups = ["no_group", "class", "major_class", "modeling_group", "res_other"]


# Define aggregation functions. These are just wrappers for basic python
# functions that make using them easier to use with pandas.agg().
def q10(x):
    return x.quantile(0.1)


def q25(x):
    return x.quantile(0.25)


def q75(x):
    return x.quantile(0.75)


def q90(x):
    return x.quantile(0.9)


def first(x):
    return x.iloc[0]


more_stats = [
    "min",
    q10,
    q25,
    "median",
    q75,
    q90,
    "max",
    "mean",
    "sum",
]

agg_func_math = {
    "sale_price": ["size", "count"] + more_stats,
    "sale_price_per_sf": more_stats,
    "sale_char_bldg_sf": ["median"],
    "sale_char_land_sf": ["median"],
    "sale_char_yrblt": ["median"],
    "class": [stats.multimode],
    "geography_data_year": [first],
}


def aggregrate(data, geography_type, group_type):
    """
    Function to group a dataframe by whichever geography and group types it is
    passed and output aggregate stats for that grouping.
    """
    print(geography_type, group_type)

    group = [geography_type, group_type, "year"]
    summary = data.groupby(group).agg(agg_func_math).round(2)
    summary["geography_type"] = geography_type
    summary["group_type"] = group_type
    summary.index.names = ["geography_id", "group_id", "year"]
    summary = summary.reset_index().set_index(
        [
            "geography_type",
            "geography_id",
            "group_type",
            "group_id",
            "year",
        ]
    )

    return summary


def assemble(df, geos, groups):
    """
    Function that loops over predefined geography and class groups and passes
    them to the aggregate function. Returns stacked output from the aggregate
    function.
    """

    # Create an empty dataframe to fill with output
    output = pd.DataFrame()

    # Loop through group combinations and stack output
    for key, value in geos.items():
        df["geography_data_year"] = df[key]

        for x in value:
            for z in groups:
                output = pd.concat([output, aggregrate(df, x, z)])

    # Flatten multi-index
    output.columns = ["_".join(col) for col in output.columns]
    output = output.reset_index()

    # Create additional stat columns post-aggregation
    output["sale_price_sum"] = output["sale_price_sum"].replace(0, np.NaN)
    output["sale_price_per_sf_sum"] = output["sale_price_per_sf_sum"].replace(
        0, np.NaN
    )

    output = output.sort_values("year")

    diff_cols = [
        "geography_id",
        "group_id",
        "sale_price_median",
        "sale_price_mean",
        "sale_price_sum",
        "sale_price_per_sf_median",
        "sale_price_per_sf_mean",
        "sale_price_per_sf_sum",
    ]

    output[
        [
            "sale_price_delta_median",
            "sale_price_delta_mean",
            "sale_price_delta_sum",
            "sale_price_per_sf_delta_median",
            "sale_price_per_sf_delta_mean",
            "sale_price_per_sf_delta_sum",
        ]
    ] = output[diff_cols].groupby(["geography_id", "group_id"]).diff()

    output = clean_names(output)

    return output


def clean_names(x):
    """
    Function to rename and reorder columns.
    """

    output = x.rename(
        columns={
            "sale_price_size": "pin_n_tot",
            "year": "sale_year",
            "sale_price_count": "sale_n_tot",
            "class_multimode": "sale_class_mode",
            "geography_data_year_first": "geography_data_year",
        }
    )

    output = output[
        [
            "geography_type",
            "geography_id",
            "geography_data_year",
            "group_type",
            "group_id",
            "sale_year",
            "pin_n_tot",
            "sale_n_tot",
            "sale_price_min",
            "sale_price_q10",
            "sale_price_q25",
            "sale_price_median",
            "sale_price_q75",
            "sale_price_q90",
            "sale_price_max",
            "sale_price_mean",
            "sale_price_sum",
            "sale_price_delta_median",
            "sale_price_delta_mean",
            "sale_price_delta_sum",
            "sale_price_per_sf_min",
            "sale_price_per_sf_q10",
            "sale_price_per_sf_q25",
            "sale_price_per_sf_median",
            "sale_price_per_sf_q75",
            "sale_price_per_sf_q90",
            "sale_price_per_sf_max",
            "sale_price_per_sf_mean",
            "sale_price_per_sf_sum",
            "sale_price_per_sf_delta_median",
            "sale_price_per_sf_delta_mean",
            "sale_price_per_sf_delta_sum",
            "sale_char_bldg_sf_median",
            "sale_char_land_sf_median",
            "sale_char_yrblt_median",
            "sale_class_mode",
        ]
    ]

    return output


def ingest_geos(geos):
    """
    Function to convert dbt seed into a dictionary that can be iterated over.
    """

    geos = geos.toPandas()
    output = {
        k: list(geos[k].unique()[pd.notnull(geos[k].unique())])
        for k in geos.columns
    }

    return output


def model(dbt, spark_session):
    """
    Function to build a dbt python model using PySpark.
    """
    dbt.config(materialized="table")

    # Ingest geographies and their associated data years
    geos = ingest_geos(dbt.ref("reporting.sot_data_years"))

    input = dbt.ref("reporting.sot_sales_input")

    # Convert the Spark input dataframe to Pandas for
    # compatibility with assesspy functions
    input = input.toPandas()

    df = assemble(input, geos=geos, groups=groups)

    schema = (
        "geography_type: string, geography_id: string, "
        + "geography_data_year: string, group_type: string, "
        + "group_id: string, sale_year: string, pin_n_tot: bigint, "
        + "sale_n_tot: int, sale_price_min: double, sale_price_q10: double, "
        + "sale_price_q25: double, sale_price_median: double, "
        + "sale_price_q75: double, sale_price_q90: double, "
        + "sale_price_max: double, sale_price_mean: double, "
        + "sale_price_sum: double, sale_price_delta_median: double, "
        + "sale_price_delta_mean: double, sale_price_delta_sum: double, "
        + "sale_price_per_sf_min: double, sale_price_per_sf_q10: double, "
        + "sale_price_per_sf_q25: double, sale_price_per_sf_median: double, "
        + "sale_price_per_sf_q75: double, sale_price_per_sf_q90: double, "
        + "sale_price_per_sf_max: double, sale_price_per_sf_mean: double, "
        + "sale_price_per_sf_sum: double, "
        + "sale_price_per_sf_delta_median: double, "
        + "sale_price_per_sf_delta_mean: double, "
        + "sale_price_per_sf_delta_sum: double, "
        + "sale_char_bldg_sf_median: double, "
        + "sale_char_land_sf_median: double, "
        + "sale_char_yrblt_median: double, sale_class_mode: array<string>"
    )

    spark_df = spark_session.createDataFrame(df, schema=schema)

    return spark_df
