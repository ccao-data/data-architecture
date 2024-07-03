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
    "sale_price_per_sf": more_stats,
    "sale_char_bldg_sf": ["median"],
    "sale_char_land_sf": ["median"],
    "sale_char_yrblt": ["median"],
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
    output["sale_price_per_sf", "sum"] = output[
        "sale_price_per_sf", "sum"
    ].replace(0, np.NaN)

    for i in ["median", "mean", "sum"]:
        output["sale_price", "delta_" + i] = output["sale_price", i].diff()
        output["sale_price_per_sf", "delta_" + i] = output[
            "sale_price_per_sf", i
        ].diff()

    output.columns = ["_".join(col) for col in output.columns]
    output = output.reset_index()

    output = clean_names(output)

    return output


def clean_names(x):
    output = x.rename(
        columns={
            "sale_price_size": "pin_n_tot",
            "year": "sale_year",
            "sale_price_count": "sale_n_tot",
            "class_multimode": "sale_class_mode",
            "data_year_first": "data_year",
        }
    )

    output = output[
        [
            "geography_type",
            "geography_id",
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
            "data_year",
        ]
    ]

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
        + "sale_char_yrblt_median: double, sale_class_mode: array<string>, "
        + "data_year: string"
    )

    spark_df = spark_session.createDataFrame(df, schema=schema)

    return spark_df
