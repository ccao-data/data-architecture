# pylint: skip-file
# type: ignore
sc.addPyFile(  # noqa: F821
    "s3://ccao-athena-dependencies-us-east-1/assesspy==1.1.0.zip"
)

# This script generates aggregated summary stats on sales data across a number
# of geographies, class combinations, and time.

# Import libraries
import assesspy as ass  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

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


def cod_safe(ratio):
    if len(ratio) >= 1:
        output = ass.cod(ratio)
    else:
        output = None

    return output


def prd_safe(assessed, sale_price):
    if len(sale_price) >= 1:
        output = ass.prd(assessed=assessed, sale_price=sale_price)
    else:
        output = None

    return output


def prb_safe(assessed, sale_price):
    if len(sale_price) >= 1:
        output = ass.prb(assessed=assessed, sale_price=sale_price, round=3)[
            "prb"
        ]
    else:
        output = None

    return output


def mki_safe(assessed, sale_price):
    if len(sale_price) >= 1:
        output = ass.mki(assessed=assessed, sale_price=sale_price)
    else:
        output = None

    return output


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


# Define aggregation functions
def aggregrate(data, geography_type, group_type):
    print(geography_type, group_type)

    group = [geography_type, group_type, "year", "stage_name"]
    data["size"] = data.groupby(group)["tot_mv"].transform("size")
    data["sale_count"] = data.groupby(group)["sale_price"].transform("count")
    data["mv_count"] = data.groupby(group)["tot_mv"].transform("count")

    # Remove parcels with FMVs of 0 since they screw up ratios
    data = data[data["tot_mv"] > 0]

    # Remove groups that only have one sale since we can't calculate stats
    data = data.dropna(subset=["sale_price"])
    data = data[data["sale_count"] >= 20]

    summary = data.groupby(group).apply(
        lambda x: pd.Series(
            {
                "triad": first(x["triad"]),
                "size": np.size(x["ratio"]),
                "mv_count": x["mv_count"].min(),
                "sale_count": x["sale_count"].min(),
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
                # "cod": ' '.join(x['ratio'].astype(str).values),
                "cod": cod_safe(ratio=x["ratio"]),
                "prd": prd_safe(
                    assessed=x["tot_mv"], sale_price=x["sale_price"]
                ),
                "prb": prb_safe(
                    assessed=x["tot_mv"], sale_price=x["sale_price"]
                ),
                "mki": mki_safe(
                    assessed=x["tot_mv"], sale_price=x["sale_price"]
                ),
            }
        )
    )
    summary["geography_type"] = geography_type
    summary["group_type"] = group_type

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

    output.dropna(how="all", axis=1, inplace=True)

    return output


def clean(dirty):
    dirty.index.names = ["geography_id", "group_id", "year", "stage_name"]

    dirty = dirty.reset_index().set_index(
        [
            "geography_type",
            "geography_id",
            "group_type",
            "group_id",
            "year",
            "stage_name",
        ]
    )

    # Clean combined dirty and export
    dirty["mv_delta_pct_median"] = (
        dirty.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .mv_median.diff()
    )
    dirty["mv_delta_pct_mean"] = (
        dirty.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .mv_mean.diff()
    )
    dirty["mv_delta_pct_sum"] = (
        dirty.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .mv_sum.diff()
    )

    dirty["mv_delta_pct_median"] = (
        dirty.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .mv_median.pct_change()
    )
    dirty["mv_delta_pct_mean"] = (
        dirty.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .mv_mean.pct_change()
    )

    dirty = dirty.reset_index()

    dirty["year"] = dirty["year"].astype(int)
    dirty["triennial"] = dirty["geography_type"].isin(
        ["triad", "township", "nbhd"]
    )
    dirty["reassessment_year"] = ""
    dirty.loc[
        (dirty["triennial"] == True), "reassessment_year"  # noqa: E712
    ] = "No"
    dirty.loc[
        (dirty["year"] % 3 == 0)
        & (dirty["triad"] == "North")
        & (dirty["triennial"] == True),  # noqa: E712
        "reassessment_year",
    ] = "Yes"
    dirty.loc[
        (dirty["year"] % 3 == 1)
        & (dirty["triad"] == "South")
        & (dirty["triennial"] == True),  # noqa: E712
        "reassessment_year",
    ] = "Yes"
    dirty.loc[
        (dirty["year"] % 3 == 2)
        & (dirty["triad"] == "City")
        & (dirty["triennial"] == True),  # noqa: E712
        "reassessment_year",
    ] = "Yes"
    dirty = dirty.drop(["triennial", "triad"], axis=1)

    dirty["cod_met"] = met(dirty["cod"], 5, 15)
    dirty["prd_met"] = met(dirty["prd"], 0.98, 1.03)
    dirty["prb_met"] = met(dirty["prb"], -0.05, 0.05)
    dirty["mki_met"] = met(dirty["mki"], 0.95, 1.05)

    dirty["within_05_pct"] = within(dirty["ratio_mean"], 0.05)
    dirty["within_10_pct"] = within(dirty["ratio_mean"], 0.1)
    dirty["within_15_pct"] = within(dirty["ratio_mean"], 0.15)
    dirty["within_20_pct"] = within(dirty["ratio_mean"], 0.2)

    dirty = dirty.astype(
        {
            "group_id": "str",
            "year": np.int64,
            "stage_name": "str",
            "reassessment_year": "str",
            "size": np.int64,
            "mv_count": np.int64,
            "sale_count": np.int64,
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

    return dirty


def model(dbt, spark_session):
    dbt.config(materialized="table")

    input = dbt.ref("reporting.sot_ratio_stats_input")

    # Convert the Spark input dataframe to Pandas for
    # compatibility with assesspy functions
    input = input.toPandas()

    df = assemble(input, geos=geos, groups=groups)

    df = clean(df)

    schema = (
        "geography_type: string, geography_id: string, "
        + "group_type: string, group_id: string, year: bigint, "
        + "stage_name: string, size: bigint, "
        + "mv_count: bigint, "
        + "sale_count: bigint, mv_min: bigint, mv_q10: bigint, "
        + "mv_q25: bigint, mv_median: bigint, mv_q75: bigint, "
        + "mv_q90: bigint, mv_max: bigint, mv_mean: bigint, "
        + "mv_sum: bigint, ratio_min: double, ratio_q10: double, "
        + "ratio_q25: double, ratio_median: double, ratio_q75: double, "
        + "ratio_q90: double, ratio_max: double, ratio_mean: double, "
        + "cod: double, prd: double, prb: double, mki: double, "
        + "mv_delta_pct_median: double, mv_delta_pct_mean: double, "
        + "mv_delta_pct_sum: double, reassessment_year: string, "
        + "cod_met: boolean, prd_met: boolean, prb_met: boolean, "
        + "mki_met: boolean, within_05_pct: boolean, "
        + "within_10_pct: boolean, within_15_pct: boolean, "
        + "within_20_pct: boolean"
    )

    spark_df = spark_session.createDataFrame(df, schema=schema)

    return spark_df
