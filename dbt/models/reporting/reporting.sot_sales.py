# This script generates aggregated summary stats on sales data across a number
# of geographies, class combinations, and time.

import statistics as stats

# Import libraries
import numpy as np
import pandas as pd

# Declare geographic groups and their associated data years
geos = {
    "year": [
        "county",
        "triad",
        "township",
        "nbhd",
        "tax_code",
        "zip_code",
    ],
    "census_data_year": [
        "census_place",
        "census_tract",
        "census_congressional_district",
        "census_zcta",
    ],
    "cook_board_of_review_district_data_year": [
        "cook_board_of_review_district"
    ],
    "cook_commissioner_district_data_year": ["cook_commissioner_district"],
    "cook_judicial_district_data_year": ["cook_judicial_district"],
    "ward_data_year": ["ward_num"],
    "community_area_data_year": ["community_area"],
    "police_district_data_year": ["police_district"],
    "central_business_district_data_year": ["central_business_district"],
    "school_data_year": [
        "school_elementary_district",
        "school_secondary_district",
        "school_unified_district",
    ],
    "tax_data_year": [
        "tax_municipality",
        "tax_park_district",
        "tax_library_district",
        "tax_fire_protection_district",
        "tax_community_college_district",
        "tax_sanitation_district",
        "tax_special_service_area",
        "tax_tif_district",
    ],
}
# Declare class groupings
groups = ["no_group", "class", "major_class", "modeling_group", "res_other"]


# Define aggregation functions
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
    "price_per_sf": more_stats,
    "char_bldg_sf": ["median"],
    "char_land_sf": ["median"],
    "char_yrblt": ["median"],
    "class": [stats.multimode],
    "data_year": [first],
}


def aggregrate(data, geography_type, group_type):
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
    # Create an empty dataframe to fill with output
    output = pd.DataFrame()

    # Loop through group combinations and stack output
    for key, value in geos.items():
        df["data_year"] = df[key]

        for x in value:
            for z in groups:
                output = pd.concat([output, aggregrate(df, x, z)])

    # Clean combined output and export
    output["sale_price", "sum"] = output["sale_price", "sum"].replace(
        0, np.NaN
    )
    output["price_per_sf", "sum"] = output["price_per_sf", "sum"].replace(
        0, np.NaN
    )

    for i in ["median", "mean", "sum"]:
        output["sale_price", "delta" + i] = output["sale_price", i].diff()
        output["price_per_sf", "delta" + i] = output["price_per_sf", i].diff()

    output.columns = ["_".join(col) for col in output.columns]
    output = output.reset_index()

    return output


def model(dbt, spark_session):
    dbt.config(materialized="table")

    input = dbt.ref("reporting.sot_sales_input")

    # Convert the Spark input dataframe to Pandas for
    # compatibility with assesspy functions
    input = input.toPandas()

    df = assemble(input, geos=geos, groups=groups)

    schema = (
        "geography_type: string, geography_id: string, group_type: string, "
        + "group_id: string, year: bigint, sale_price_size: double, "
        + "sale_price_count: double, sale_price_min: double, "
        + "sale_price_q10: double, sale_price_q25: double, "
        + "sale_price_median: double, sale_price_q75: double, "
        + "sale_price_q90: double, sale_price_max: double, "
        + "sale_price_mean: double, sale_price_sum: double, "
        + "price_per_sf_min: double, price_per_sf_q10: double, "
        + "price_per_sf_q25: double, price_per_sf_median: double, "
        + "price_per_sf_q75: double, price_per_sf_q90: double, "
        + "price_per_sf_max: double, price_per_sf_mean: double, "
        + "price_per_sf_sum: double, char_bldg_sf_median: double, "
        + "char_land_sf_median: double, char_yrblt_median: double, "
        + "class_multimode: array<string>, data_year_first: bigint,"
        + "sale_price_deltamedian: double, price_per_sf_deltamedian: double, "
        + "sale_price_deltamean: double, price_per_sf_deltamean: double, "
        + "sale_price_deltasum: double, price_per_sf_deltasum: double"
    )

    spark_df = spark_session.createDataFrame(df, schema=schema)

    return spark_df
