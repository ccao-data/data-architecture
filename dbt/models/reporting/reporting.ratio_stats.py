import numpy as np
import pandas as pd
from assesspy import boot_ci, cod
from assesspy import cod_ci as cod_boot
from assesspy import cod_met, detect_chasing, mki, mki_met, prb, prb_met, prd
from assesspy import prd_ci as prd_boot
from assesspy import prd_met


def median_boot(ratio, nboot=100, alpha=0.05):
    return boot_ci(np.median, nboot=nboot, alpha=alpha, ratio=ratio)


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
        mki_val = mki(fmv_no_outliers, sale_price_no_outliers)
        met = mki_met(mki_val)

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
        cod_val = cod(no_outliers)
        cod_ci = cod_boot(no_outliers, nboot=1000)
        cod_ci = f"{cod_ci[0]}, {cod_ci[1]}"
        met = cod_met(cod_val)
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
        prd_val = prd(fmv_no_outliers, sale_price_no_outliers)
        prd_ci = prd_boot(fmv_no_outliers, sale_price_no_outliers, nboot=1000)
        prd_ci = f"{prd_ci[0]}, {prd_ci[1]}"
        met = prd_met(prd_val)

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
        prb_model = prb(fmv_no_outliers, sale_price_no_outliers)
        prb_val = prb_model["prb"]
        prb_ci = prb_model["95% ci"]
        prb_ci = f"{prb_ci[0]}, {prb_ci[1]}"
        met = prb_met(prb_val)

        out = [prb_val, prb_ci, met, prb_n]

    else:
        out = [None, None, None, prb_n]

    return out


def report_summarise(df, geography_id, geography_type):
    # Aggregates data and calculates summary statistics for given groupings

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
                "detect_chasing": detect_chasing(ratio=x["ratio"]),
                "within_20_pct": sum(abs(1 - x["ratio"]) <= 0.20),
                "within_10_pct": sum(abs(1 - x["ratio"]) <= 0.10),
                "within_05_pct": sum(abs(1 - x["ratio"]) <= 0.05),
            }
        )
    )

    df[["median_ratio", "median_ratio_ci", "median_ratio_n"]] = pd.DataFrame(
        df.ratio.tolist(), index=df.index
    )
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


def model(dbt, session):
    dbt.config(packages=["assesspy==1.1.0", "numpy==1.26.*", "pandas==2.*"])

    input = dbt.ref("reporting.vw_ratio_stats")

    final_df = pd.concat(
        [
            report_summarise(input, "triad", "Tri"),
            report_summarise(input, "township_code", "Town"),
        ]
    ).reset_index(drop=True)

    return final_df