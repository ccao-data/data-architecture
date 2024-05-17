# pylint: skip-file
# type: ignore
sc.addPyFile(  # noqa: F821
    "s3://ccao-dbt-dependencies-us-east-1/assesspy_v1_1_0.zip"
)

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import assesspy_v1_1_0 as assesspy  # noqa: E402


def median_boot(ratio, nboot=100, alpha=0.05):
    return assesspy.boot_ci(np.median, nboot=nboot, alpha=alpha, ratio=ratio)

def ccao_median(x):
    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = x.between(
        x.quantile(0.05), x.quantile(0.95), inclusive="neither"
    )

    x_no_outliers = x[no_outliers]

    median_n = sum(no_outliers)

    median_val = np.median(x_no_outliers)
    median_ci = median_boot(x_no_outliers, nboot=1000)
    median_ci = f"{median_ci[0]}, {median_ci[1]}"

    out = [median_val, median_ci, median_n]

    return out

def ccao_mki(fmv, sale_price):
    ratio = fmv / sale_price

    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = ratio.between(
        ratio.quantile(0.05), ratio.quantile(0.95), inclusive="neither"
    )

    fmv_no_outliers = fmv[no_outliers]
    sale_price_no_outliers = sale_price[no_outliers]

    mki_n = sum(no_outliers)

    if mki_n >= 20:
        mki_val = assesspy.mki(fmv_no_outliers, sale_price_no_outliers)
        met = assesspy.mki_met(mki_val)

        out = [mki_val, met, mki_n]

    else:
        out = [None, None, mki_n]

    return out

def ccao_cod(ratio):
    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = ratio[
        ratio.between(
            ratio.quantile(0.05), ratio.quantile(0.95), inclusive="neither"
        )
    ]

    cod_n = no_outliers.size

    if cod_n >= 20:
        cod_val = assesspy.cod(no_outliers)
        cod_ci = assesspy.cod_ci(no_outliers, nboot=1000)
        cod_ci = f"{cod_ci[0]}, {cod_ci[1]}"
        met = assesspy.cod_met(cod_val)
        out = [cod_val, cod_ci, met, cod_n]

    else:
        out = [None, None, None, cod_n]

    return out

def ccao_prd(fmv, sale_price):
    ratio = fmv / sale_price

    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = ratio.between(
        ratio.quantile(0.05), ratio.quantile(0.95), inclusive="neither"
    )

    fmv_no_outliers = fmv[no_outliers]
    sale_price_no_outliers = sale_price[no_outliers]

    prd_n = sum(no_outliers)

    if prd_n >= 20:
        prd_val = assesspy.prd(fmv_no_outliers, sale_price_no_outliers)
        prd_ci = assesspy.prd_ci(
            fmv_no_outliers, sale_price_no_outliers, nboot=1000
        )
        prd_ci = f"{prd_ci[0]}, {prd_ci[1]}"
        met = assesspy.prd_met(prd_val)

        out = [prd_val, prd_ci, met, prd_n]

    else:
        out = [None, None, None, prd_n]

    return out

def ccao_prb(fmv, sale_price):
    ratio = fmv / sale_price

    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = ratio.between(
        ratio.quantile(0.05), ratio.quantile(0.95), inclusive="neither"
    )

    fmv_no_outliers = fmv[no_outliers]
    sale_price_no_outliers = sale_price[no_outliers]

    prb_n = sum(no_outliers)

    if prb_n >= 20:
        prb_model = assesspy.prb(fmv_no_outliers, sale_price_no_outliers)
        prb_val = prb_model["prb"]
        prb_ci = prb_model["95% ci"]
        prb_ci = f"{prb_ci[0]}, {prb_ci[1]}"
        met = assesspy.prb_met(prb_val)

        out = [prb_val, prb_ci, met, prb_n]

    else:
        out = [None, None, None, prb_n]

    return out

def report_summarise(df, geography_id, geography_type):
    """
    Aggregates data and calculates summary statistics for given groupings
    """

    group_cols = [
        "year",
        "triad",
        "geography_type",
        "property_group",
        "assessment_stage",
        "geography_id",
        "sale_year",
    ]

    df["geography_id"] = df[geography_id].astype(str)
    df["geography_type"] = geography_type

    # Remove groups with less than three observations
    # TODO: Remove/upgrade detect_chasing output
    df["n"] = df.groupby(group_cols)["ratio"].transform("count")
    df = df[df["n"] > 3]
    df = df.groupby(group_cols).apply(
        lambda x: pd.Series(
            {
                "sale_n": np.size(x["triad"]),
                "ratio": ccao_median(x["ratio"]),
                "cod": ccao_cod(ratio=x["ratio"]),
                "mki": ccao_mki(fmv=x["fmv"], sale_price=x["sale_price"]),
                "prd": ccao_prd(fmv=x["fmv"], sale_price=x["sale_price"]),
                "prb": ccao_prb(fmv=x["fmv"], sale_price=x["sale_price"]),
                "detect_chasing": False,
                "within_20_pct": sum(abs(1 - x["ratio"]) <= 0.20),
                "within_10_pct": sum(abs(1 - x["ratio"]) <= 0.10),
                "within_05_pct": sum(abs(1 - x["ratio"]) <= 0.05),
            }
        )
    )

    df[
        ["median_ratio", "median_ratio_ci", "median_ratio_n"]
    ] = pd.DataFrame(df.ratio.tolist(), index=df.index)
    df[["cod", "cod_ci", "cod_met", "cod_n"]] = pd.DataFrame(
        df.cod.tolist(), index=df.index
    )
    df[["mki", "mki_met", "mki_n"]] = pd.DataFrame(
        df.mki.tolist(), index=df.index
    )
    df[["prd", "prd_ci", "prd_met", "prd_n"]] = pd.DataFrame(
        df.prd.tolist(), index=df.index
    )
    df[["prb", "prb_ci", "prb_met", "prb_n"]] = pd.DataFrame(
        df.prb.tolist(), index=df.index
    )
    df["ratio_met"] = abs(1 - df["median_ratio"]) <= 0.05
    df["vertical_equity_met"] = df.prd_met | df.prb_met

    # Arrange output columns
    df = df[
        [
            "sale_n",
            "median_ratio",
            "median_ratio_ci",
            "cod",
            "cod_ci",
            "cod_n",
            "prd",
            "prd_ci",
            "prd_n",
            "prb",
            "prb_ci",
            "prb_n",
            "mki",
            "mki_n",
            "detect_chasing",
            "ratio_met",
            "cod_met",
            "prd_met",
            "prb_met",
            "mki_met",
            "vertical_equity_met",
            "within_20_pct",
            "within_10_pct",
            "within_05_pct",
        ]
    ].reset_index()

    return df


def model(dbt, spark_session):
    dbt.config(materialized="table")

    input = dbt.ref("reporting.ratio_stats_input")

    # Convert the Spark input dataframe to Pandas for
    # compatibility with assesspy functions
    input = input.toPandas()

    # Replicate filtering from prior vw_ratio_stats pull
    input = input[input.ratio > 0 & input.ratio.notnull()]

    df = pd.concat(
        [
            report_summarise(input, "triad", "Tri"),
            report_summarise(input, "township_code", "Town"),
        ]
    ).reset_index(drop=True)

    # Force certain columns to datatype to maintain parity with old version
    df[["year", "triad", "sale_year"]] = df[
        ["year", "triad", "sale_year"]
    ].astype(int)

    # Create a Spark schema to maintain the datatypes of the
    # previous output (for Tableau compatibility)
    schema = (
        "year: bigint, triad: bigint, geography_type: string, "
        + "property_group: string, assessment_stage: string, "
        + "geography_id: string, sale_year: bigint, sale_n: bigint, "
        + "median_ratio: double, median_ratio_ci: string, cod: double, "
        + "cod_ci: string, cod_n: bigint, prd: double, prd_ci: string, "
        + "prd_n: bigint, prb: double, prb_ci: string, prb_n: bigint, "
        + "mki: double, mki_n: bigint, detect_chasing: boolean, "
        + "ratio_met: boolean, cod_met: boolean, prd_met: boolean, "
        + "prb_met: boolean, mki_met: boolean, vertical_equity_met: boolean, "
        + "within_20_pct: bigint, within_10_pct: bigint, within_05_pct: bigint"
    )

    spark_df = spark_session.createDataFrame(df, schema=schema)

    return spark_df
