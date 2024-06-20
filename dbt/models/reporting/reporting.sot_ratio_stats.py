# pylint: skip-file
# type: ignore
sc.addPyFile(  # noqa: F821
    "s3://ccao-athena-dependencies-us-east-1/assesspy==1.1.0.zip"
)

# This script generates aggregated summary stats on sales data across a number
# of geographies, class combinations, and time.

# Import libraries
import assesspy as ass  # noqa: E402
import pandas as pd  # noqa: E402

# Declare geographic groups and their associated data years
geos = {
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
groups = ["no_group", "class", "major_class", "modeling_group"]


# Define aggregation functions
def aggregrate(data, geography_type, group_type):
    print(geography_type, group_type)

    group = [geography_type, group_type, "year", "stage_name"]
    data["size"] = data.groupby(group)["tot_mv"].transform("size")
    data["sale_count"] = data.groupby(group)["sale_price"].transform("count")
    data["mv_count"] = data.groupby(group)["tot_mv"].transform("count")

    # Remove parcels with FMVs of 0 since they screw up ratios
    data = data[data["tot_mv"] > 0].reset_index()
    data["ratio_count"] = data.groupby(group)["ratio"].transform("count")

    # Remove groups that only have one sale since we can't calculate stats
    data = data[data["ratio_count"] >= 30]

    summary = (
        data.dropna(subset=["ratio"])
        .groupby(group)
        .apply(
            lambda x: pd.Series(
                {
                    "size": x["size"].iloc[0],
                    "mv_count": x["mv_count"].iloc[0],
                    "sale_count": x["sale_count"].iloc[0],
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
                    "cod": ass.cod(ratio=x["ratio"]),
                    "prd": ass.prd(x["tot_mv"], x["sale_price"]),
                    "prb": ass.prb(x["tot_mv"], x["sale_price"], 3)["prb"],
                    "mki": ass.mki(x["tot_mv"], x["sale_price"]),
                }
            ),
            include_groups=False,
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

    # Clean combined output and export
    output["mv_delta_pct_median"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .mv_median.diff()
    )
    output["mv_delta_pct_mean"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .mv_mean.diff()
    )
    output["mv_delta_pct_sum"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .mv_sum.diff()
    )

    output["mv_delta_pct_median"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .mv_median.pct_change()
    )
    output["mv_delta_pct_mean"] = (
        output.sort_values("year")
        .groupby(["geography_id", "group_id", "stage_name"])
        .mv_mean.pct_change()
    )

    output.dropna(how="all", axis=1, inplace=True)

    return output


def model(dbt, spark_session):
    dbt.config(materialized="table")

    input = dbt.ref("reporting.sot_ratio_stats_input")

    # Convert the Spark input dataframe to Pandas for
    # compatibility with assesspy functions
    input = input.toPandas()

    df = assemble(input, geos=geos, groups=groups)

    spark_df = spark_session.createDataFrame(df)

    return spark_df
