# pylint: skip-file
# type: ignore

# This script generates aggregated summary stats on taxes and exemptions data
# across a number of geographies, class combinations, and time.

# Import libraries
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

less_stats = ["count", "sum"]

agg_func_math = {
    "eq_factor_final": ["size", first],
    "eq_factor_tentative": [first],
    "tax_bill_total": more_stats,
    "tax_code_rate": more_stats,
    "av_clerk": more_stats,
    "exe_homeowner": less_stats,
    "exe_senior": less_stats,
    "exe_freeze": less_stats,
    "exe_longtime_homeowner": less_stats,
    "exe_disabled": less_stats,
    "exe_vet_returning": less_stats,
    "exe_vet_dis_lt50": less_stats,
    "exe_vet_dis_50_69": less_stats,
    "exe_vet_dis_ge70": less_stats,
    "exe_abate": less_stats,
}


def assemble(df, geos, groups):
    # Create an empty dataframe to fill with output
    output = pd.DataFrame()

    # Loop through group combinations and stack output
    for key, value in geos.items():
        df["data_year"] = df[key]

        for x in value:
            for z in groups:
                group = [x, z, "year"]
                summary = df.groupby(group).agg(agg_func_math).round(2)
                summary["geography_type"] = x
                summary["group_type"] = z
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

                output = pd.concat([output, summary])

    # Clean combined output and export
    for i in ["median", "mean", "sum"]:
        output["tax_bill_total", "delta" + i] = output[
            "tax_bill_total", i
        ].diff()

    output.columns = ["_".join(col) for col in output.columns]
    output = output.reset_index()

    return output


def model(dbt, spark_session):
    dbt.config(materialized="table")

    input = dbt.ref("reporting.sot_taxes_exemptions_input")

    # Convert the Spark input dataframe to Pandas for
    # compatibility with assesspy functions
    input = input.toPandas()

    df = assemble(input, geos=geos, groups=groups)

    spark_df = spark_session.createDataFrame(df)

    return spark_df
