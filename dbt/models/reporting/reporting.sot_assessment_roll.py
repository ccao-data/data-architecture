# This script generates aggregated summary stats on assessed values across a
# number of geographies, class combinations, and time.

# Import libraries
from functools import reduce

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

spark.driver.maxResultSize = 0  # noqa:F821


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
    if len(x) >= 1:
        output = x.iloc[0]
    else:
        output = None

    return output


def reassessment_year(year, geography, triad):
    if geography in ["triad", "township", "nbhd"]:
        year = int(year) % 3

        if (
            ((year == 0) & (triad == "North"))
            | ((year == 1) & (triad == "South"))
            | ((year == 2) & (triad == "City"))
        ):
            out = "Yes"
        else:
            out = "No"
    else:
        out = ""

    return out


def aggregate_geography(geography):
    def aggregate(key, pdf):
        columns = ["av_tot", "av_bldg", "av_land"]

        out = ()
        out += (
            reassessment_year(pdf["year"][0], geography, pdf["triad"][0]),
            first(pdf[years[geography]]),
            len(pdf["av_tot"]),
            pdf["av_tot"].count(),
            pdf["av_tot"].count() / pdf["av_tot"].size,
        )
        for column in columns:
            out += (
                pdf[column].min(),
                q10(pdf[column]),
                q25(pdf[column]),
                pdf[column].median(),
                q75(pdf[column]),
                q90(pdf[column]),
                pdf[column].max(),
                pdf[column].mean(),
                pdf[column].sum(),
            )

        return pd.DataFrame([key + out])

    return aggregate


groups = [
    "res_other",
    "major_class",
    "no_group",
    "class",
    "modeling_group",
]

years = {
    "county": "year",
    "triad": "year",
    "township": "year",
    "nbhd": "year",
    "tax_code": "year",
    "zip_code": "year",
    "community_area": "community_area_data_year",
    "census_place": "census_data_year",
    "census_tract": "census_data_year",
    "census_congressional_district": "census_data_year",
    "census_zcta": "census_data_year",
    "cook_board_of_review_district": "cook_board_of_review_district_data_year",
    "cook_commissioner_district": "cook_commissioner_district_data_year",
    "cook_judicial_district": "cook_judicial_district_data_year",
    "ward_num": "ward_data_year",
    "police_district": "police_district_data_year",
    "school_elementary_district": "school_data_year",
    "school_secondary_district": "school_data_year",
    "school_unified_district": "school_data_year",
    "tax_municipality": "tax_data_year",
    "tax_park_district": "tax_data_year",
    "tax_library_district": "tax_data_year",
    "tax_fire_protection_district": "tax_data_year",
    "tax_community_college_district": "tax_data_year",
    "tax_sanitation_district": "tax_data_year",
    "tax_special_service_area": "tax_data_year",
    "tax_tif_district": "tax_data_year",
    "central_business_district": "central_business_district_data_year",
}

geographies = list(years.keys())

output_schema = "stage_name string, group_id string, geography_id string, year string, reassessment_year string, geography_data_year string, pin_n_tot bigint, pin_n_w_value bigint, pin_pct_w_value double, min_av_tot double, q10_av_tot double, q25_av_tot double, median_av_tot double, q75_av_tot double, q90_av_tot double, max_av_tot double, mean_av_tot double, sum_av_tot double, min_av_bldg double, q10_av_bldg double, q25_av_bldg double, median_av_bldg double, q75_av_bldg double, q90_av_bldg double, max_av_bldg double, mean_av_bldg double, sum_av_bldg double, min_av_land double, q10_av_land double, q25_av_land double, median_av_land double, q75_av_land double, q90_av_land double, max_av_land double, mean_av_land double, sum_av_land double"


def model(dbt, spark_session):
    dbt.config(materialized="table", engine_config={"MaxConcurrentDpus": 40})
    athena_user_logger.info("Loading assessment roll input table")

    input = dbt.ref("reporting.sot_assessment_roll_input")

    athena_user_logger.info("Dope stuff is happening... maybe?")

    output = []
    for group in groups:
        for geography in geographies:
            output += [
                input.groupby(["stage_name", group, geography, "year"])
                .applyInPandas(
                    aggregate_geography(geography),
                    schema=output_schema,
                )
                .select(
                    "*",
                    lit(group).alias("group_type"),
                    lit(geography).alias("geography_type"),
                )
            ]

    df = reduce(DataFrame.unionByName, output)

    return df
