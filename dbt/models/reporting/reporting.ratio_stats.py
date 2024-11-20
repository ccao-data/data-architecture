# pylint: skip-file
# type: ignore
sc.addPyFile("s3://ccao-athena-dependencies-us-east-1/assesspy==1.2.0.zip")

import math

import pandas as pd
import statsmodels.api as sm
from pyspark.sql.functions import col, lit


def boot_ci(fun, nboot=1000, alpha=0.05, **kwargs):
    num_kwargs = len(kwargs)
    kwargs = pd.DataFrame(kwargs)
    n = len(kwargs)

    ests = []
    for i in list(range(1, nboot)):
        sample = kwargs.sample(n=n, replace=True)
        if fun.__name__ == "cod" or num_kwargs == 1:
            ests.append(fun(sample.iloc[:, 0]))
        elif fun.__name__ in ["prd", "prb", "mki"]:
            ests.append(fun(sample.iloc[:, 0], sample.iloc[:, 1]))
        else:
            raise Exception(
                "Input function should be one of 'cod', 'prd', 'prb', or 'mki'."
            )

    ests = pd.Series(ests)

    ci = [ests.quantile(alpha / 2), ests.quantile(1 - alpha / 2)]

    return ci


# - - - - COD functions - - -
def cod_boot(ratio, nboot=1000, alpha=0.05):
    return boot_ci(cod, ratio=ratio, nboot=nboot, alpha=alpha)


def cod(x):
    n = x.size
    median_ratio = x.median()
    # No numpy in here, as Spark doesn't seem to play well with it
    ratio_minus_med = x - median_ratio
    abs_diff_sum = ratio_minus_med.abs().sum()
    cod = 100 / median_ratio * (abs_diff_sum / n)
    return cod


def ccao_cod(x):
    no_outliers = x.between(
        x.quantile(0.05), x.quantile(0.95), inclusive="neither"
    )

    x_no_outliers = x[no_outliers]
    cod_n = x_no_outliers.size

    if cod_n >= 20:
        cod_val = cod(x_no_outliers)
        cod_ci = cod_boot(ratio=x_no_outliers.to_numpy(), nboot=1000)

        met = 5 <= cod_val <= 15
        out = [cod_val, cod_ci[0], cod_ci[1], met, cod_n]

    else:
        out = [None, None, None, None, cod_n]

    return out


# Median Functions
def calculate_median(series):
    return series.median()


def median_boot(ratio, nboot=1000, alpha=0.05):
    return boot_ci(calculate_median, nboot=nboot, alpha=alpha, ratio=ratio)


def ccao_median(x):
    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = x.between(
        x.quantile(0.05), x.quantile(0.95), inclusive="neither"
    )

    x_no_outliers = x[no_outliers]
    median_n = x_no_outliers.size
    median_ratio = x_no_outliers.median()

    median_ci = median_boot(x_no_outliers, nboot=1000)
    median_ci_l = median_ci[0]
    median_ci_u = median_ci[1]

    out = [median_ratio, median_ci_l, median_ci_u, median_n]

    return out


def prd(assessed, sale_price):
    ratio = assessed / sale_price
    prd = ratio.mean() / (ratio * sale_price / sale_price.sum()).sum()
    return prd


def prd_boot(assessed, sale_price, nboot=1000, alpha=0.05):
    return boot_ci(
        prd, assessed=assessed, sale_price=sale_price, nboot=nboot, alpha=alpha
    )


# Had to combine into one df because spark can't handle operations on multiple dfs
def ccao_prd(df):
    ratio = df["fmv"] / df["sale_price"]

    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = ratio.between(
        ratio.quantile(0.05), ratio.quantile(0.95), inclusive="neither"
    )
    # quantiles = ratio.quantile([0.05, 0.95])
    # no_outliers = ratio.between(quantiles[0.05], quantiles[0.95], inclusive="neither")

    df_no_outliers = df[no_outliers]

    prd_n = df_no_outliers.shape[0]

    if prd_n >= 20:
        prd_val = prd(df_no_outliers["fmv"], df_no_outliers["sale_price"])
        prd_ci = prd_boot(
            df_no_outliers["fmv"], df_no_outliers["sale_price"], nboot=1000
        )
        prd_ci_l, prd_ci_u = prd_ci[0], prd_ci[1]
        prd_met = 0.98 <= prd_val <= 1.03

        out = [prd_val, prd_ci_l, prd_ci_u, prd_met, prd_n]
    else:
        out = [None, None, None, None, prd_n]

    return out


def calculate_gini(df):
    df = df.sort_values(
        by="sale_price", kind="mergesort"
    )  # for stable sort results
    assessed_price = df["fmv"].to_list()
    sale_price = df["sale_price"].to_list()
    n = len(assessed_price)

    sale_sum = sum(sale_price[i] * (i + 1) for i in range(n))
    g_sale = 2 * sale_sum / sum(sale_price) - (n + 1)
    gini_sale = g_sale / n

    assessed_sum = sum(assessed_price[i] * (i + 1) for i in range(n))
    g_assessed = 2 * assessed_sum / sum(assessed_price) - (n + 1)
    gini_assessed = g_assessed / n

    return float(gini_assessed), float(gini_sale)


def mki(df):
    gini_assessed, gini_sale = calculate_gini(df)
    MKI = gini_assessed / gini_sale
    return float(MKI)


def ccao_mki(df):
    ratio = df["fmv"] / df["sale_price"]

    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = ratio.between(
        ratio.quantile(0.05), ratio.quantile(0.95), inclusive="neither"
    )

    df_no_outliers = df[no_outliers]

    mki_n = sum(no_outliers)

    if mki_n >= 20:
        mki_val = mki(df_no_outliers)
        mki_met = 0.95 <= mki_val <= 1.05

        out = [mki_val, mki_met, mki_n]

    else:
        out = [None, None, mki_n]

    return out


def prb(fmv, sale_price, round=None):
    assessed = fmv
    sale_price = sale_price

    ratio = assessed / sale_price
    median_ratio = ratio.median()

    lhs = (ratio - median_ratio) / median_ratio
    rhs = ((assessed / median_ratio) + sale_price).apply(
        lambda x: math.log2(x / 2)
    )

    prb_model = sm.OLS(lhs.to_numpy(), rhs.to_numpy()).fit()

    prb_val = float(prb_model.params)
    prb_ci = prb_model.conf_int(alpha=0.05)[0].tolist()

    if round is not None:
        out = {
            "prb": round(prb_val, round),
            "95% ci": [round(prb_ci[0], round), round(prb_ci[1], round)],
        }
    else:
        out = {"prb": prb_val, "95% ci": prb_ci}

    return out


def ccao_prb(df):
    fmv = df["fmv"]
    sale_price = df["sale_price"]

    ratio = fmv / sale_price

    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = ratio.between(
        ratio.quantile(0.05), ratio.quantile(0.95), inclusive="neither"
    )

    fmv_no_outliers = fmv[no_outliers]
    sale_price_no_outliers = sale_price[no_outliers]

    prb_n = no_outliers.sum()

    if prb_n >= 20:
        prb_model = prb(fmv_no_outliers, sale_price_no_outliers)
        prb_val = prb_model["prb"]
        prb_ci = prb_model["95% ci"]
        prb_ci_l = prb_ci[0]
        prb_ci_u = prb_ci[1]
        met = -0.05 <= prb_val <= 0.05

        out = [prb_val, prb_ci_l, prb_ci_u, met, prb_n]

    else:
        out = [None, None, None, None, prb_n]

    return out


def report_summarise(df, geography_id, geography_type):
    group_cols = [
        "year",
        "triad",
        "geography_type",
        "property_group",
        "assessment_stage",
        "geography_id",
        "sale_year",
    ]

    schema = (
        "year string, triad string, geography_type string, "
        "property_group string, assessment_stage string, "
        "geography_id string, sale_year string, sale_n bigint, "
        "median_ratio double, median_ci_l double, median_ci_u double, median_n bigint, "
        "cod double, cod_ci_l double, cod_ci_u double, cod_met boolean, cod_n bigint, "
        "prd double, prd_ci_l double, prd_ci_u double, prd_met boolean, prd_n bigint, "
        "prb double, prb_ci_l double, prb_ci_u double, prb_met boolean, prb_n bigint, "
        "mki double, mki_met boolean, mki_n bigint, "
        "within_20_pct bigint, within_10_pct bigint, within_05_pct bigint"
    )

    out = (
        df.withColumn("geography_id", col(geography_id).cast("string"))
        .withColumn("geography_type", lit(geography_type))
        .groupby(group_cols)
        .applyInPandas(
            lambda x: pd.DataFrame(
                [
                    {
                        **dict(
                            zip(group_cols, [x[c].iloc[0] for c in group_cols])
                        ),
                        "sale_n": x["triad"].size,
                        **dict(
                            zip(
                                [
                                    "median_ratio",
                                    "median_ci_l",
                                    "median_ci_u",
                                    "median_n",
                                ],
                                ccao_median(x["ratio"]),
                            )
                        ),
                        **dict(
                            zip(
                                [
                                    "cod",
                                    "cod_ci_l",
                                    "cod_ci_u",
                                    "cod_met",
                                    "cod_n",
                                ],
                                ccao_cod(x["ratio"]),
                            )
                        ),
                        **dict(
                            zip(
                                [
                                    "prd",
                                    "prd_ci_l",
                                    "prd_ci_u",
                                    "prd_met",
                                    "prd_n",
                                ],
                                ccao_prd(x[["fmv", "sale_price"]]),
                            )
                        ),
                        **dict(
                            zip(
                                ["mki", "mki_met", "mki_n"],
                                ccao_mki(x[["fmv", "sale_price"]]),
                            )
                        ),
                        **dict(
                            zip(
                                [
                                    "prb",
                                    "prb_ci_l",
                                    "prb_ci_u",
                                    "prb_met",
                                    "prb_n",
                                ],
                                ccao_prb(x[["fmv", "sale_price"]]),
                            )
                        ),
                        "within_20_pct": sum(abs(1 - x["ratio"]) <= 0.20),
                        "within_10_pct": sum(abs(1 - x["ratio"]) <= 0.10),
                        "within_05_pct": sum(abs(1 - x["ratio"]) <= 0.05),
                    }
                ]
            ),
            schema=schema,
        )
    )

    return out


def model(dbt, spark_session):
    dbt.config(materialized="table", engine_config={"MaxConcurrentDpus": 40})

    input = dbt.ref("reporting.ratio_stats_input")
    input = input.filter(input.ratio.isNotNull()).filter(input.ratio > 0)

    df_tri = report_summarise(input, "triad", "Tri")
    df_town = report_summarise(input, "township_code", "Town")
    df = df_tri.unionByName(df_town)

    # Force certain columns to datatype to maintain parity with old version
    df = df.withColumn("year", col("year").cast("int"))
    df = df.withColumn("triad", col("triad").cast("int"))
    df = df.withColumn("sale_year", col("sale_year").cast("int"))
    athena_user_logger.info("Pre-column arrange")

    # Arrange output columns
    df = df.select(
        col("year"),
        col("triad"),
        col("geography_type"),
        col("property_group"),
        col("assessment_stage"),
        col("geography_id"),
        col("sale_year"),
        col("sale_n"),
        col("median_ratio"),
        col("median_ci_l"),
        col("median_ci_u"),
        col("median_n"),
        col("cod"),
        col("cod_ci_l"),
        col("cod_ci_u"),
        col("cod_met"),
        col("cod_n"),
        col("prd"),
        col("prd_ci_l"),
        col("prd_ci_u"),
        col("prd_n"),
        col("prd_met"),
        col("mki"),
        col("mki_met"),
        col("mki_n"),
        col("prb"),
        col("prb_ci_l"),
        col("prb_ci_u"),
        col("prb_met"),
        col("prb_n"),
        col("within_20_pct"),
        col("within_10_pct"),
        col("within_05_pct"),
    )

    return df
