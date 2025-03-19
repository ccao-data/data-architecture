# This script generates aggregated summary stats on assessed values across a
# number of geographies, class combinations, and time.

# Import libraries
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


def aggregrate(data, geography_type, group_type, stats):
    """
    Function to group a dataframe by whichever geography and group types it is
    passed and output aggregate stats for that grouping.
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
                output = pd.concat([output, aggregrate(df, x, z, stats=stats)])

    # Flatten multi-index
    output.columns = ["_".join(col) for col in output.columns]
    output = output.reset_index()
    output = output.rename(columns={"triad_first": "triad"})

    # Create additional stat columns post-aggregation
    output["av_tot_pct_w_value"] = (
        output["av_tot_count"] / output["av_tot_size"]
    )

    output = output.sort_values("year")

    diff_cols = [
        "geography_id",
        "group_id",
        "stage_name",
        "av_tot_median",
        "av_tot_mean",
        "av_tot_sum",
        "av_bldg_median",
        "av_bldg_mean",
        "av_bldg_sum",
        "av_land_median",
        "av_land_mean",
        "av_land_sum",
    ]

    output[
        [
            "av_tot_delta_median",
            "av_tot_delta_mean",
            "av_tot_delta_sum",
            "av_bldg_delta_median",
            "av_bldg_delta_mean",
            "av_bldg_delta_sum",
            "av_land_delta_median",
            "av_land_delta_mean",
            "av_land_delta_sum",
        ]
    ] = (
        output[diff_cols]
        .groupby(["geography_id", "group_id", "stage_name"])
        .diff()
    )

    output[
        [
            "av_tot_delta_pct_median",
            "av_tot_delta_pct_mean",
            "av_tot_delta_pct_sum",
            "av_bldg_delta_pct_median",
            "av_bldg_delta_pct_mean",
            "av_bldg_delta_pct_sum",
            "av_land_delta_pct_median",
            "av_land_delta_pct_mean",
            "av_land_delta_pct_sum",
        ]
    ] = (
        output[diff_cols]
        .groupby(["geography_id", "group_id", "stage_name"])
        .pct_change()
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
        ((output["year"] % 3 == 0) & (output["triad"] == "North"))
        | ((output["year"] % 3 == 1) & (output["triad"] == "South"))
        | ((output["year"] % 3 == 2) & (output["triad"] == "City"))
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

    input = dbt.ref("reporting.sot_assessment_roll_input")

    # Convert the Spark input dataframe to Pandas for
    # compatibility with assesspy functions
    input = input.toPandas()

    df = assemble(input, geos=geos, groups=groups)
    # %%
    schema = {
        "geography_type": "string",
        "geography_id": "string",
        "geography_data_year": "string",
        "group_type": "string",
        "group_id": "string",
        "year": "string",
        "reassessment_year": "string",
        "stage_name": "string",
        "pin_n_tot": "bigint",
        "pin_n_w_value": "bigint",
        "pin_pct_w_value": "double",
        "av_tot_min": "double",
        "av_tot_q10": "double",
        "av_tot_q25": "double",
        "av_tot_median": "double",
        "av_tot_q75": "double",
        "av_tot_q90": "double",
        "av_tot_max": "double",
        "av_tot_mean": "double",
        "av_tot_sum": "double",
        "av_tot_delta_median": "double",
        "av_tot_delta_mean": "double",
        "av_tot_delta_sum": "double",
        "av_tot_delta_pct_median": "double",
        "av_tot_delta_pct_mean": "double",
        "av_tot_delta_pct_sum": "double",
        "av_bldg_min": "double",
        "av_bldg_q10": "double",
        "av_bldg_q25": "double",
        "av_bldg_median": "double",
        "av_bldg_q75": "double",
        "av_bldg_q90": "double",
        "av_bldg_max": "double",
        "av_bldg_mean": "double",
        "av_bldg_sum": "double",
        "av_bldg_delta_median": "double",
        "av_bldg_delta_mean": "double",
        "av_bldg_delta_sum": "double",
        "av_bldg_delta_pct_median": "double",
        "av_bldg_delta_pct_mean": "double",
        "av_bldg_delta_pct_sum": "double",
        "av_land_min": "double",
        "av_land_q10": "double",
        "av_land_q25": "double",
        "av_land_median": "double",
        "av_land_q75": "double",
        "av_land_q90": "double",
        "av_land_max": "double",
        "av_land_mean": "double",
        "av_land_sum": "double",
        "av_land_delta_median": "double",
        "av_land_delta_mean": "double",
        "av_land_delta_sum": "double",
        "av_land_delta_pct_median": "double",
        "av_land_delta_pct_mean": "double",
        "av_land_delta_pct_sum": "double",
    }
    # %%
    spark_df = spark_session.createDataFrame(
        df, schema=", ".join(f"{key}: {val}" for key, val in schema.items())
    )

    return spark_df
