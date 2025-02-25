# pylint: skip-file
# type: ignore

sc.addPyFile("s3://ccao-athena-dependencies-us-east-1/assesspy==2.0.2.zip")


from typing import Union

import assesspy as ap
import pandas as pd
from pyspark.sql.functions import col, lit

CCAO_LOWER_QUANTILE = 0.05
CCAO_UPPER_QUANTILE = 0.95
CCAO_MIN_SAMPLE_SIZE = 20.0

# The schema definition used within applyInPandas (where it is required) and
# to determine the final column order
SPARK_SCHEMA = (
    "year string, triad string, geography_type string, property_group string, "
    "assessment_stage string, geography_id string, sale_year string, sale_n bigint, "
    "sales_removed bigint, "
    "med_ratio double, med_ratio_ci_l double, med_ratio_ci_u double, "
    "med_ratio_met boolean, med_ratio_n bigint, "
    "cod double, cod_ci_l double, cod_ci_u double, cod_met boolean, cod_n bigint, "
    "prd double, prd_ci_l double, prd_ci_u double, prd_met boolean, prd_n bigint, "
    "prb double, prb_ci_l double, prb_ci_u double, prb_met boolean, prb_n bigint, "
    "mki double, mki_ci_l double, mki_ci_u double, mki_met boolean, mki_n bigint, "
    "vertical_equity_met boolean, "
    "is_sales_chased boolean, within_20_pct bigint, within_10_pct bigint, within_05_pct bigint"
)


def ccao_drop_outliers(
    estimate: Union[list[int], list[float], pd.Series],
    sale_price: Union[list[int], list[float], pd.Series],
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
    estimate: Union[list[int], list[float], pd.Series],
    sale_price: Union[list[int], list[float], pd.Series],
) -> dict:
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
            ci_l, ci_u = getattr(ap, f"{fun}_ci")(
                est_no_out, sale_no_out, nboot=300
            )
        except AttributeError:
            ci_l, ci_u = None, None
        met = getattr(ap, f"{fun}_met")(val)
        out = [val, ci_l, ci_u, met, n]
    else:
        out = [None, None, None, None, n]

    # Zip into a dictionary for use with the calc_summary function below,
    # which expects a dict to expand into a DataFrame
    out_dict = dict(
        zip([fun, f"{fun}_ci_l", f"{fun}_ci_u", f"{fun}_met", f"{fun}_n"], out)
    )

    return out_dict


def ccao_median(
    estimate: Union[list[int], list[float], pd.Series],
    sale_price: Union[list[int], list[float], pd.Series],
) -> dict:
    """
    Calculates the median ratio of estimate to sale price, excluding outliers.
    Ignores the CCAO minimum sample size requirement (only needs 2 values).
    """
    est_no_out, sale_no_out, n = ccao_drop_outliers(estimate, sale_price)

    def median_val(estimate, sale_price):
        ratio = estimate / sale_price
        return ratio.median()

    if n >= 2:
        val = median_val(est_no_out, sale_no_out)
        ci_l, ci_u = ap.boot_ci(
            median_val, estimate=est_no_out, sale_price=sale_no_out, nboot=300
        )
        med_met = ap.med_ratio_met(val)
        out = [val, ci_l, ci_u, med_met, n]
    else:
        val = median_val(est_no_out, sale_no_out)
        out = [val, None, None, None, n]

    out_dict = dict(
        zip(
            [
                "med_ratio",
                "med_ratio_ci_l",
                "med_ratio_ci_u",
                "med_ratio_met",
                "med_ratio_n",
            ],
            out,
        )
    )

    return out_dict


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
                        **ccao_median(x["fmv"], x["sale_price"]),
                        **(
                            prb_metrics := ccao_metric(
                                "prb", x["fmv"], x["sale_price"]
                            )
                        ),
                        **(
                            prd_metrics := ccao_metric(
                                "prd", x["fmv"], x["sale_price"]
                            )
                        ),
                        **ccao_metric("cod", x["fmv"], x["sale_price"]),
                        **ccao_metric("mki", x["fmv"], x["sale_price"]),
                        "is_sales_chased": ap.is_sales_chased(x["ratio"])
                        if x["ratio"].size >= CCAO_MIN_SAMPLE_SIZE
                        else None,
                        "within_20_pct": sum(abs(1 - x["ratio"]) <= 0.20),
                        "within_10_pct": sum(abs(1 - x["ratio"]) <= 0.10),
                        "within_05_pct": sum(abs(1 - x["ratio"]) <= 0.05),
                        "vertical_equity_met": (
                            None
                            if (
                                prb_metrics.get("prb_met") is None
                                and prd_metrics.get("prd_met") is None
                            )
                            else bool(
                                prb_metrics.get("prb_met")
                                or prd_metrics.get("prd_met")
                            )
                        ),
                        "sales_removed": x["triad"].size
                        - ccao_drop_outliers(x["fmv"], x["sale_price"])[2],
                    }
                ]
            ),
            schema=SPARK_SCHEMA,
        )
    )

    return out


def model(dbt, spark_session):
    dbt.config(materialized="table", engine_config={"MaxConcurrentDpus": 40})
    athena_user_logger.info("Loading ratio stats input table")

    input = dbt.ref("reporting.ratio_stats_input")
    input = input.filter(input.ratio.isNotNull()).filter(input.ratio > 0)

    athena_user_logger.info("Calculating group-level statistics")
    df_tri = calc_summary(input, "triad", "Tri")
    df_town = calc_summary(input, "township_code", "Town")
    df = df_tri.unionByName(df_town)

    def schema_to_spark_cols(schema: str):
        columns = []
        for col_def in schema.split(", "):
            col_name = col_def.split(" ")[0]
            columns.append(col(col_name))
        return columns

    athena_user_logger.info("Cleaning up and arranging columns")
    df = df.select(*schema_to_spark_cols(SPARK_SCHEMA))

    return df
