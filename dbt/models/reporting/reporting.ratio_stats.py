# pylint: skip-file
# type: ignore
sc.addPyFile("s3://ccao-athena-dependencies-us-east-1/assesspy==2.0.0.zip")

import assesspy as ap
import pandas as pd
from pyspark.sql.functions import col, lit

CCAO_LOWER_QUANTILE = 0.05
CCAO_UPPER_QUANTILE = 0.05
CCAO_MIN_SAMPLE_SIZE = 0.95


def ccao_drop_outliers(
    estimate: list[int] | list[float] | pd.Series,
    sale_price: list[int] | list[float] | pd.Series,
) -> tuple[pd.Series, pd.Series, float]:
    """
    Helper function to drop the top and bottom N% (usually 5%) of the input
    ratios, per CCAO SOPs and IAAO recommendation.
    """
    ratio: pd.Series = estimate / sale_price
    ratio_not_outlier = ratio.between(
        ratio.quantile(CCAO_LOWER_QUANTILE),
        ratio.quantile(CCAO_UPPER_QUANTILE),
        inclusive="neither",
    ).reset_index(drop=True)

    estimate_no_outliers = estimate[ratio_not_outlier]
    sale_price_no_outliers = sale_price[ratio_not_outlier]
    n: float = float(estimate_no_outliers.size)

    return estimate_no_outliers, sale_price_no_outliers, n


def ccao_metric(
    fun: callable,
    estimate: list[int] | list[float] | pd.Series,
    sale_price: list[int] | list[float] | pd.Series,
) -> list[float | None]:
    """
    Helper function to calculate a metric, its confidence interval, and
    whether the metric meets the IAAO/Quintos standard. Also checks if the
    number of samples is large enough to calculate a metric. Per CCAO
    standards, the sample size must be at least 20.
    """
    est_no_out, sale_no_out, n = ccao_drop_outliers(estimate, sale_price)

    if n >= CCAO_MIN_SAMPLE_SIZE:
        val = getattr(ap, fun)(est_no_out, sale_no_out)
        # MKI doesn't have a _ci function, so we need a check here to return
        # None if it is called
        try:
            ci_l, ci_u = getattr(ap, f"{fun}_ci")(est_no_out, sale_no_out)
        except AttributeError:
            ci_l, ci_u = None, None
        met = getattr(ap, f"{fun}_met")(val)
        out = [val, ci_l, ci_u, met, n]
    else:
        out = [None, None, None, None, n]

    return out


def ccao_median(
    estimate: list[int] | list[float] | pd.Series,
    sale_price: list[int] | list[float] | pd.Series,
) -> list[float]:
    """
    Calculates the median ratio of estimate to sale price, excluding outliers.
    Ignores the CCAO minimum sample size requirement.
    """
    est_no_out, sale_no_out, n = ccao_drop_outliers(estimate, sale_price)

    def median_val(estimate, sale_price):
        ratio = estimate / sale_price
        return ratio.median()

    val = median_val(est_no_out, sale_no_out)
    ci_l, ci_u = ap.boot_ci(
        median_val, estimate=est_no_out, sale_price=sale_no_out
    )
    out = [val, ci_l, ci_u, n]

    return out


def calc_summary(df: pd.Series, geography_id: str, geography_type: str):
    """
    Calculate ratio summary statistics for a given geography and geography type.
    Takes a DataFrame as input and returns a single row DataFrame of stats.
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

    # The applyInPandas function used below requires a pre-specified schema
    schema = (
        "year string, triad string, geography_type string, "
        "property_group string, assessment_stage string, "
        "geography_id string, sale_year string, sale_n bigint, "
        "med_ratio double, med_ratio_ci_l double, med_ratio_ci_u double, med_ratio_n bigint, "
        "cod double, cod_ci_l double, cod_ci_u double, cod_met boolean, cod_n bigint, "
        "prd double, prd_ci_l double, prd_ci_u double, prd_met boolean, prd_n bigint, "
        "prb double, prb_ci_l double, prb_ci_u double, prb_met boolean, prb_n bigint, "
        "mki double, mki_ci_l double, mki_ci_u double, mki_met boolean, mki_n bigint, "
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
                        # Include the grouping column values in the output
                        **dict(
                            zip(group_cols, [x[c].iloc[0] for c in group_cols])
                        ),
                        "sale_n": x["triad"].size,
                        **dict(
                            zip(
                                [
                                    "med_ratio",
                                    "med_ratio_ci_l",
                                    "med_ratio_ci_u",
                                    "med_ratio_n",
                                ],
                                ccao_median(x["fmv"], x["sale_price"]),
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
                                ccao_metric("cod", x["fmv"], x["sale_price"]),
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
                                ccao_metric("prd", x["fmv"], x["sale_price"]),
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
                                ccao_metric("prb", x["fmv"], x["sale_price"]),
                            )
                        ),
                        **dict(
                            zip(
                                [
                                    "mki",
                                    "mki_ci_l",
                                    "mki_ci_u",
                                    "mki_met",
                                    "mki_n",
                                ],
                                ccao_metric("mki", x["fmv"], x["sale_price"]),
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

    df_tri = calc_summary(input, "triad", "Tri")
    df_town = calc_summary(input, "township_code", "Town")
    df = df_tri.unionByName(df_town)

    # Force certain column types to maintain parity with old version
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
        col("prb"),
        col("prb_ci_l"),
        col("prb_ci_u"),
        col("prb_met"),
        col("prb_n"),
        col("mki"),
        col("mki_met"),
        col("mki_n"),
        col("within_20_pct"),
        col("within_10_pct"),
        col("within_05_pct"),
    )

    return df
