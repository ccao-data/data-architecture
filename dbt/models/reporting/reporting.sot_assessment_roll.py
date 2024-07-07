# This script generates aggregated summary stats on assessed values across a
# number of geographies, class combinations, and time.

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
    "av_tot": ["size", "count"] + more_stats,
    "av_bldg": more_stats,
    "av_land": more_stats,
    "triad": [first],
    "geography_data_year": [first],
}


def aggregrate(data, geography_type, group_type):
    """
    Function to group a dataframe by whichever geography and group types it is
    passed and output aggregate stats for that only for that grouping.
    """

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
    """
    Function that loops over predefined geography and class groups and passes
    them to the aggregate function. Outputs stacked aggegrated output from the
    aggregate function.
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
    output = output.rename(columns={"triad_first": "triad"})

    # Create additional stat columns post-aggregation
    output["av_tot_pct_w_value"] = (
        output["av_tot_count"] / output["av_tot_size"]
    )

    output["av_tot_delta_median"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_tot_median.diff()
    )

    output["av_tot_delta_mean"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_tot_mean.diff()
    )

    output["av_tot_delta_sum"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_tot_sum.diff()
    )

    output["av_bldg_delta_median"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_bldg_median.diff()
    )

    output["av_bldg_delta_mean"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_bldg_mean.diff()
    )

    output["av_bldg_delta_sum"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_bldg_sum.diff()
    )

    output["av_land_delta_median"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_land_median.diff()
    )

    output["av_land_delta_mean"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_land_mean.diff()
    )

    output["av_land_delta_sum"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_land_sum.diff()
    )

    output["av_tot_delta_pct_median"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_tot_median.pct_change()
    )

    output["av_tot_delta_pct_mean"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_tot_mean.pct_change()
    )

    output["av_tot_delta_pct_sum"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_tot_sum.pct_change()
    )

    output["av_bldg_delta_pct_median"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_bldg_median.pct_change()
    )

    output["av_bldg_delta_pct_mean"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_bldg_mean.pct_change()
    )

    output["av_bldg_delta_pct_sum"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_bldg_sum.pct_change()
    )

    output["av_land_delta_pct_median"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_land_median.pct_change()
    )

    output["av_land_delta_pct_mean"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_land_mean.pct_change()
    )

    output["av_land_delta_pct_sum"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .av_land_sum.pct_change()
    )

    output["year"] = output["year"].astype(int)
    output["triennial"] = output["geography_type"].isin(
        ["triad", "township", "nbhd"]
    )

    # Reassessment year is constructed as a string rather than a boolean to
    # avoid PySpark errors with nullable booleans that can likely be resolved.
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

    output = clean_names(output)

    return output


def clean_names(x):
    """
    Function to rename and reorder columns.
    """

    output = x.rename(
        columns={
            "av_tot_size": "pin_n_tot",
            "av_tot_count": "pin_n_w_value",
            "av_tot_pct_w_value": "pin_pct_w_value",
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
            "year",
            "reassessment_year",
            "stage_name",
            "pin_n_tot",
            "pin_n_w_value",
            "pin_pct_w_value",
            "av_tot_min",
            "av_tot_q10",
            "av_tot_q25",
            "av_tot_median",
            "av_tot_q75",
            "av_tot_q90",
            "av_tot_max",
            "av_tot_mean",
            "av_tot_sum",
            "av_tot_delta_median",
            "av_tot_delta_mean",
            "av_tot_delta_sum",
            "av_tot_delta_pct_median",
            "av_tot_delta_pct_mean",
            "av_tot_delta_pct_sum",
            "av_bldg_min",
            "av_bldg_q10",
            "av_bldg_q25",
            "av_bldg_median",
            "av_bldg_q75",
            "av_bldg_q90",
            "av_bldg_max",
            "av_bldg_mean",
            "av_bldg_sum",
            "av_bldg_delta_median",
            "av_bldg_delta_mean",
            "av_bldg_delta_sum",
            "av_bldg_delta_pct_median",
            "av_bldg_delta_pct_mean",
            "av_bldg_delta_pct_sum",
            "av_land_min",
            "av_land_q10",
            "av_land_q25",
            "av_land_median",
            "av_land_q75",
            "av_land_q90",
            "av_land_max",
            "av_land_mean",
            "av_land_sum",
            "av_land_delta_median",
            "av_land_delta_mean",
            "av_land_delta_sum",
            "av_land_delta_pct_median",
            "av_land_delta_pct_mean",
            "av_land_delta_pct_sum",
        ]
    ]

    return output


def model(dbt, spark_session):
    dbt.config(materialized="table")

    input = dbt.ref("reporting.sot_assessment_roll_input")

    # Convert the Spark input dataframe to Pandas for
    # compatibility with assesspy functions
    input = input.toPandas()

    df = assemble(input, geos=geos, groups=groups)

    schema = (
        "geography_type: string, geography_id: string, "
        + "geography_data_year: string, group_type: string, group_id: string, "
        + "year: string, reassessment_year: string, stage_name: string, "
        + "pin_n_tot: bigint, pin_n_w_value: bigint, pin_pct_w_value: double, "
        + "av_tot_min: double, av_tot_q10: double, av_tot_q25: double, "
        + "av_tot_median: double, av_tot_q75: double, av_tot_q90: double, "
        + "av_tot_max: double, av_tot_mean: double, av_tot_sum: double, "
        + "av_tot_delta_median: double, av_tot_delta_mean: double, "
        + "av_tot_delta_sum: double, av_tot_delta_pct_median: double, "
        + "av_tot_delta_pct_mean: double, av_tot_delta_pct_sum: double, "
        + "av_bldg_min: double, av_bldg_q10: double, av_bldg_q25: double, "
        + "av_bldg_median: double, av_bldg_q75: double, av_bldg_q90: double, "
        + "av_bldg_max: double, av_bldg_mean: double, av_bldg_sum: double, "
        + "av_bldg_delta_median: double, av_bldg_delta_mean: double, "
        + "av_bldg_delta_sum: double, av_bldg_delta_pct_median: double, "
        + "av_bldg_delta_pct_mean: double, av_bldg_delta_pct_sum: double, "
        + "av_land_min: double, av_land_q10: double, av_land_q25: double, "
        + "av_land_median: double, av_land_q75: double, av_land_q90: double, "
        + "av_land_max: double, av_land_mean: double, av_land_sum: double, "
        + "av_land_delta_median: double, av_land_delta_mean: double, "
        + "av_land_delta_sum: double, av_land_delta_pct_median: double, "
        + "av_land_delta_pct_mean: double, av_land_delta_pct_sum: double"
    )

    spark_df = spark_session.createDataFrame(df, schema=schema)

    return spark_df
