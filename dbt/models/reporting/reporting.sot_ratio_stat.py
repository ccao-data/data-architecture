# pylint: skip-file
# type: ignore
sc.addPyFile(  # noqa: F821
    "s3://ccao-athena-dependencies-us-east-1/assesspy==2.0.2.zip"
)

# This script generates aggregated summary stats on sales ratios across a
# number of geographies, class combinations, and time.

# Import libraries
import assesspy as ass  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

# Declare class groupings
groups = ["no_group", "class", "major_class", "modeling_group", "res_other"]


# Wrap assesspy functions to avoid GitHub runner errors for length 0 groupings
def cod_safe(assessed, sale_price):
    if len(sale_price) >= 1:
        output = ass.cod(estimate=assessed, sale_price=sale_price)
    else:
        output = None

    return output


def prd_safe(assessed, sale_price):
    if len(sale_price) >= 1:
        output = ass.prd(estimate=assessed, sale_price=sale_price)
    else:
        output = None

    return output


def prb_safe(assessed, sale_price):
    if len(sale_price) >= 1:
        output = ass.prb(estimate=assessed, sale_price=sale_price)
    else:
        output = None

    return output


def mki_safe(assessed, sale_price):
    if len(sale_price) >= 1:
        output = ass.mki(estimate=assessed, sale_price=sale_price)
    else:
        output = None

    return output


# Define aggregation functions
def first(x):
    if len(x) >= 1:
        output = x.iloc[0]
    else:
        output = None

    return output


def met(x, lower_limit, upper_limit):
    return np.logical_and(lower_limit <= x, x <= upper_limit)


def within(x, limit):
    return np.logical_and(1 - limit < x, x < 1 + limit)


def aggregrate(data, geography_type, group_type):
    """
    Function to group a dataframe by whichever geography and group types it is
    passed and output aggregate stats for that grouping. Works
    differently than in other SoT scripts since assesspy functions need
    multiple inputs.
    """

    print(geography_type, group_type)

    group = [geography_type, group_type, "year", "stage_name"]
    data["pin_n_tot"] = data.groupby(group)["tot_mv"].transform("size")
    data["sale_n_tot"] = data.groupby(group)["sale_price"].transform("count")
    data["pin_n_w_value"] = data.groupby(group)["tot_mv"].transform("count")

    # Remove parcels with MVs of 0 since they screw up ratios
    data = data[data["tot_mv"] > 0]

    # Remove groups that only have one sale since we can't calculate stats
    data = data.dropna(subset=["sale_price"])
    data = data[data["sale_n_tot"] >= 20]

    summary = data.groupby(group).apply(
        lambda x: pd.Series(
            {
                "triad": first(x["triad"]),
                "pin_n_tot": np.size(x["ratio"]),
                "pin_n_w_value": x["pin_n_w_value"].min(),
                "sale_n_tot": x["sale_n_tot"].min(),
                "mv_min": x["tot_mv"].min(),
                "mv_q10": x["tot_mv"].quantile(0.1),
                "mv_q25": x["tot_mv"].quantile(0.25),
                "mv_median": x["tot_mv"].median(),
                "mv_q75": x["tot_mv"].quantile(0.75),
                "mv_q90": x["tot_mv"].quantile(0.90),
                "mv_max": x["tot_mv"].max(),
                "mv_mean": x["tot_mv"].mean(),
                "mv_sum": x["tot_mv"].sum(),
                "ratio_min": x["ratio"].min(),
                "ratio_q10": x["ratio"].quantile(0.1),
                "ratio_q25": x["ratio"].quantile(0.25),
                "ratio_median": x["ratio"].median(),
                "ratio_q75": x["ratio"].quantile(0.75),
                "ratio_q90": x["ratio"].quantile(0.90),
                "ratio_max": x["ratio"].max(),
                "ratio_mean": x["ratio"].mean(),
                "cod": cod_safe(
                    assessed=x["tot_mv"], sale_price=x["sale_price"]
                ),
                "prd": prd_safe(
                    assessed=x["tot_mv"], sale_price=x["sale_price"]
                ),
                "prb": prb_safe(
                    assessed=x["tot_mv"], sale_price=x["sale_price"]
                ),
                "mki": mki_safe(
                    assessed=x["tot_mv"], sale_price=x["sale_price"]
                ),
                "geography_data_year": first(x["data_year"]),
            }
        )
    )
    summary["geography_type"] = geography_type
    summary["group_type"] = group_type

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
        df["data_year"] = df[key]

        for x in value:
            for z in groups:
                output = pd.concat([output, aggregrate(df, x, z)])

    output.dropna(how="all", axis=1, inplace=True)

    output.index.names = ["geography_id", "group_id", "year", "stage_name"]
    output = output.reset_index().set_index(
        [
            "geography_type",
            "geography_id",
            "group_type",
            "group_id",
            "year",
            "stage_name",
        ]
    )

    output = output.reset_index()

    # Create additional stat columns post-aggregation
    output["pin_pct_w_value"] = output["pin_n_w_value"] / output["pin_n_tot"]

    output = output.sort_values("year")

    diff_cols = [
        "geography_id",
        "group_id",
        "stage_name",
        "mv_median",
        "mv_mean",
        "mv_sum",
    ]

    output[
        [
            "mv_delta_median",
            "mv_delta_mean",
            "mv_delta_sum",
        ]
    ] = (
        output[diff_cols]
        .groupby(["geography_id", "group_id", "stage_name"])
        .diff()
    )

    output[
        [
            "mv_delta_pct_median",
            "mv_delta_pct_mean",
            "mv_delta_pct_sum",
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

    output["cod_met"] = met(output["cod"], 5, 15)
    output["prd_met"] = met(output["prd"], 0.98, 1.03)
    output["prb_met"] = met(output["prb"], -0.05, 0.05)
    output["mki_met"] = met(output["mki"], 0.95, 1.05)

    output["within_05_pct"] = within(output["ratio_mean"], 0.05)
    output["within_10_pct"] = within(output["ratio_mean"], 0.1)
    output["within_15_pct"] = within(output["ratio_mean"], 0.15)
    output["within_20_pct"] = within(output["ratio_mean"], 0.2)

    # PySpark rejects nan, convert them to None
    output = output.replace(np.nan, None)

    output = clean(output)

    return output


def clean(dirty):
    """
    Function to change column types and reorder them.
    """

    dirty = dirty.astype(
        {
            "group_id": "str",
            "year": "str",
            "stage_name": "str",
            "reassessment_year": "str",
            "pin_n_w_value": np.int64,
            "pin_n_tot": np.int64,
            "sale_n_tot": np.int64,
            "mv_min": np.int64,
            "mv_q10": np.int64,
            "mv_q25": np.int64,
            "mv_median": np.int64,
            "mv_q75": np.int64,
            "mv_q90": np.int64,
            "mv_max": np.int64,
            "mv_mean": np.int64,
            "mv_sum": np.int64,
        }
    )

    dirty = dirty[
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
            "sale_n_tot",
            "mv_min",
            "mv_q10",
            "mv_q25",
            "mv_median",
            "mv_q75",
            "mv_q90",
            "mv_max",
            "mv_mean",
            "mv_sum",
            "mv_delta_median",
            "mv_delta_mean",
            "mv_delta_sum",
            "mv_delta_pct_median",
            "mv_delta_pct_mean",
            "mv_delta_pct_sum",
            "ratio_min",
            "ratio_q10",
            "ratio_q25",
            "ratio_median",
            "ratio_q75",
            "ratio_q90",
            "ratio_max",
            "ratio_mean",
            "cod",
            "prd",
            "prb",
            "mki",
            "cod_met",
            "prd_met",
            "prb_met",
            "mki_met",
            "within_05_pct",
            "within_10_pct",
            "within_15_pct",
            "within_20_pct",
        ]
    ]

    return dirty


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

    input = dbt.ref("reporting.sot_ratio_stat_input")

    # Convert the Spark input dataframe to Pandas for
    # compatibility with assesspy functions
    input = input.toPandas()

    df = assemble(input, geos=geos, groups=groups)

    schema = (
        "geography_type: string, geography_id: string, "
        + "geography_data_year: string, group_type: string, group_id: string, "
        + "year: string, reassessment_year: string, stage_name: string, "
        + "pin_n_tot: int, pin_n_w_value: bigint, pin_pct_w_value: double, "
        + "sale_n_tot: bigint, mv_min: bigint, mv_q10: bigint, "
        + "mv_q25: bigint, mv_median: bigint, mv_q75: bigint, "
        + "mv_q90: bigint, mv_max: bigint, mv_mean: bigint, mv_sum: bigint, "
        + "mv_delta_median: double, mv_delta_mean: double, "
        + "mv_delta_sum: double, mv_delta_pct_median: double, "
        + "mv_delta_pct_mean: double, mv_delta_pct_sum: double, "
        + "ratio_min: double, ratio_q10: double, ratio_q25: double, "
        + "ratio_median: double, ratio_q75: double, ratio_q90: double, "
        + "ratio_max: double, ratio_mean: double, cod: double, prd: double, "
        + "prb: double, mki: double, cod_met: boolean, prd_met: boolean, "
        + "prb_met: boolean, mki_met: boolean, within_05_pct: boolean, "
        + "within_10_pct: boolean, within_15_pct: boolean, "
        + "within_20_pct: boolean"
    )

    spark_df = spark_session.createDataFrame(df, schema=schema)

    return spark_df
