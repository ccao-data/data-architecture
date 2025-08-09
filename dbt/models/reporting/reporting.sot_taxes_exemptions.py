# This script generates aggregated summary stats on taxes and exemptions data
# across a number of geographies, class combinations, and time.

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
    "tax_eq_factor_final": ["size", first],
    "tax_eq_factor_tentative": [first],
    "tax_bill_total": more_stats,
    "tax_rate": more_stats,
    "tax_av": more_stats,
    "tax_exe_homeowner": less_stats,
    "tax_exe_senior": less_stats,
    "tax_exe_freeze": less_stats,
    "tax_exe_longtime_homeowner": less_stats,
    "tax_exe_disabled": less_stats,
    "tax_exe_vet_returning": less_stats,
    "tax_exe_vet_dis_lt50": less_stats,
    "tax_exe_vet_dis_50_69": less_stats,
    "tax_exe_vet_dis_ge70": less_stats,
    "tax_exe_abate": less_stats,
    "tax_exe_total": less_stats,
    "geography_data_year": [first],
}


def aggregrate(data, geography_type, group_type):
    """
    Function to group a dataframe by whichever geography and group types it is
    passed and output aggregate stats for that grouping.
    """

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
                output = pd.concat([output, aggregrate(df, x, z)])

    # Flatten multi-index
    output.columns = ["_".join(col) for col in output.columns]
    output = output.reset_index()

    # Create additional stat columns post-aggregation
    output = output.sort_values("year")

    diff_cols = [
        "geography_id",
        "group_id",
        "tax_bill_total_median",
        "tax_bill_total_mean",
        "tax_bill_total_sum",
    ]

    output[
        [
            "tax_bill_total_delta_median",
            "tax_bill_total_delta_mean",
            "tax_bill_total_delta_sum",
        ]
    ] = output[diff_cols].groupby(["geography_id", "group_id"]).diff()

    output = clean_names(output)

    return output


def clean_names(x):
    """
    Function to rename and reorder columns.
    """

    output = x.rename(
        columns={
            "tax_eq_factor_final_size": "pin_n_tot",
            "year": "tax_year",
            "tax_exe_homeowner_count": "tax_exe_n_homeowner",
            "tax_exe_senior_count": "tax_exe_n_senior",
            "tax_exe_freeze_count": "tax_exe_n_freeze",
            "tax_exe_longtime_homeowner_count": "tax_exe_n_longtime_homeowner",
            "tax_exe_disabled_count": "tax_exe_n_disabled",
            "tax_exe_vet_returning_count": "tax_exe_n_vet_returning",
            "tax_exe_vet_dis_lt50_count": "tax_exe_n_vet_dis_lt50",
            "tax_exe_vet_dis_50_69_count": "tax_exe_n_vet_dis_50_69",
            "tax_exe_vet_dis_ge70_count": "tax_exe_n_vet_dis_ge70",
            "tax_exe_abate_count": "tax_exe_n_abate",
            "tax_exe_total_count": "tax_exe_n_total",
            "tax_eq_factor_final_first": "tax_eq_factor_final",
            "tax_eq_factor_tentative_first": "tax_eq_factor_tentative",
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
            "tax_year",
            "pin_n_tot",
            "tax_eq_factor_final",
            "tax_eq_factor_tentative",
            "tax_bill_total_min",
            "tax_bill_total_q10",
            "tax_bill_total_q25",
            "tax_bill_total_median",
            "tax_bill_total_q75",
            "tax_bill_total_q90",
            "tax_bill_total_max",
            "tax_bill_total_mean",
            "tax_bill_total_sum",
            "tax_bill_total_delta_median",
            "tax_bill_total_delta_mean",
            "tax_bill_total_delta_sum",
            "tax_rate_min",
            "tax_rate_q10",
            "tax_rate_q25",
            "tax_rate_median",
            "tax_rate_q75",
            "tax_rate_q90",
            "tax_rate_max",
            "tax_rate_mean",
            "tax_rate_sum",
            "tax_av_min",
            "tax_av_q10",
            "tax_av_q25",
            "tax_av_median",
            "tax_av_q75",
            "tax_av_q90",
            "tax_av_max",
            "tax_av_mean",
            "tax_av_sum",
            "tax_exe_n_homeowner",
            "tax_exe_homeowner_sum",
            "tax_exe_n_senior",
            "tax_exe_senior_sum",
            "tax_exe_n_freeze",
            "tax_exe_freeze_sum",
            "tax_exe_n_longtime_homeowner",
            "tax_exe_longtime_homeowner_sum",
            "tax_exe_n_disabled",
            "tax_exe_disabled_sum",
            "tax_exe_n_vet_returning",
            "tax_exe_vet_returning_sum",
            "tax_exe_n_vet_dis_lt50",
            "tax_exe_vet_dis_lt50_sum",
            "tax_exe_n_vet_dis_50_69",
            "tax_exe_vet_dis_50_69_sum",
            "tax_exe_n_vet_dis_ge70",
            "tax_exe_vet_dis_ge70_sum",
            "tax_exe_n_abate",
            "tax_exe_abate_sum",
            "tax_exe_n_total",
            "tax_exe_total_sum",
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

    input = dbt.ref("reporting.sot_taxes_exemptions_input")

    # Convert the Spark input dataframe to Pandas for
    # compatibility with assesspy functions
    input = input.toPandas()

    df = assemble(input, geos=geos, groups=groups)

    schema = {
        "geography_type": "string",
        "geography_id": "string",
        "geography_data_year": "string",
        "group_type": "string",
        "group_id": "string",
        "tax_year": "string",
        "pin_n_tot": "bigint",
        "tax_eq_factor_final": "double",
        "tax_eq_factor_tentative": "double",
        "tax_bill_total_min": "double",
        "tax_bill_total_q10": "double",
        "tax_bill_total_q25": "double",
        "tax_bill_total_median": "double",
        "tax_bill_total_q75": "double",
        "tax_bill_total_q90": "double",
        "tax_bill_total_max": "double",
        "tax_bill_total_mean": "double",
        "tax_bill_total_sum": "double",
        "tax_bill_total_delta_median": "double",
        "tax_bill_total_delta_mean": "double",
        "tax_bill_total_delta_sum": "double ",
        "tax_rate_min": "double",
        "tax_rate_q10": "double",
        "tax_rate_q25": "double",
        "tax_rate_median": "double",
        "tax_rate_q75": "double",
        "tax_rate_q90": "double",
        "tax_rate_max": "double",
        "tax_rate_mean": "double",
        "tax_rate_sum": "double",
        "tax_av_min": "int",
        "tax_av_q10": "double",
        "tax_av_q25": "double",
        "tax_av_median": "double",
        "tax_av_q75": "double",
        "tax_av_q90": "double",
        "tax_av_max": "int",
        "tax_av_mean": "double",
        "tax_av_sum": "double",
        "tax_exe_n_homeowner": "bigint",
        "tax_exe_homeowner_sum": "double",
        "tax_exe_n_senior": "bigint",
        "tax_exe_senior_sum": "double",
        "tax_exe_n_freeze": "bigint",
        "tax_exe_freeze_sum": "double",
        "tax_exe_n_longtime_homeowner": "bigint",
        "tax_exe_longtime_homeowner_sum": "double",
        "tax_exe_n_disabled": "bigint",
        "tax_exe_disabled_sum": "double",
        "tax_exe_n_vet_returning": "bigint",
        "tax_exe_vet_returning_sum": "double",
        "tax_exe_n_vet_dis_lt50": "bigint",
        "tax_exe_vet_dis_lt50_sum": "double",
        "tax_exe_n_vet_dis_50_69": "bigint",
        "tax_exe_vet_dis_50_69_sum": "double",
        "tax_exe_n_vet_dis_ge70": "bigint",
        "tax_exe_vet_dis_ge70_sum": "double",
        "tax_exe_n_abate": "bigint",
        "tax_exe_abate_sum": "double",
        "tax_exe_n_total": "bigint",
        "tax_exe_total_sum": "double",
    }

    spark_df = spark_session.createDataFrame(
        df, schema=", ".join(f"{key}: {val}" for key, val in schema.items())
    )

    return spark_df
