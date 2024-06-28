# pylint: skip-file
# type: ignore

# This script generates aggregated summary stats on sales data across a number
# of geographies, class combinations, and time.

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
    if len(x) >= 1:
        output = x.iloc[0]
    else:
        output = None

    return output


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

stats = {
    "tot": ["size", "count"] + more_stats,
    "bldg": more_stats,
    "land": more_stats,
    "triad": [first],
}


def aggregrate(data, geography_type, group_type):
    print(geography_type, group_type)

    group = [geography_type, group_type, "year", "stage_name"]
    summary = data.groupby(group).agg(stats).round(2)
    summary["geography_type"] = geography_type
    summary["group_type"] = group_type
    summary.index.names = ["geography_id", "group_id", "year", "stage_name"]
    summary = summary.reset_index().set_index(
        [
            "geography_type",
            "geography_id",
            "group_type",
            "group_id",
            "year",
            "stage_name",
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
    for i in ["median", "mean", "sum"]:
        output["tot", "delta" + i] = output["tot", i].diff()
        output["bldg", "delta" + i] = output["bldg", i].diff()
        output["land", "delta" + i] = output["land", i].diff()

    output["tot", "pct_w_value"] = (
        output["tot", "count"] / output["tot", "size"]
    )

    output.columns = ["_".join(col) for col in output.columns]
    output = output.reset_index()
    output = output.rename(columns={"triad_first": "triad"})

    output["year"] = output["year"].astype(int)
    output["triennial"] = output["geography_type"].isin(
        ["triad", "township", "nbhd"]
    )
    output["reassessment_year"] = ""
    output.loc[
        (output["triennial"] == True), "reassessment_year"  # noqa: E712
    ] = "No"
    output.loc[
        (output["year"] % 3 == 0)
        & (output["triad"] == "North")
        & (output["triennial"] == True),  # noqa: E712
        "reassessment_year",
    ] = "Yes"
    output.loc[
        (output["year"] % 3 == 1)
        & (output["triad"] == "South")
        & (output["triennial"] == True),  # noqa: E712
        "reassessment_year",
    ] = "Yes"
    output.loc[
        (output["year"] % 3 == 2)
        & (output["triad"] == "City")
        & (output["triennial"] == True),  # noqa: E712
        "reassessment_year",
    ] = "Yes"
    output = output.drop(["triennial", "triad"], axis=1)

    return output


def model(dbt, spark_session):
    dbt.config(materialized="table")

    input = dbt.ref("reporting.sot_assessment_roll_input")

    # Convert the Spark input dataframe to Pandas for
    # compatibility with assesspy functions
    input = input.toPandas()

    df = assemble(input, geos=geos, groups=groups)

    spark_df = spark_session.createDataFrame(df)

    return spark_df
