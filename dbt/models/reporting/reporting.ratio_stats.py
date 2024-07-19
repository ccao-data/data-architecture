# pylint: skip-file
# type: ignore
sc.addPyFile(  # noqa: F821
    "s3://ccao-athena-dependencies-us-east-1/assesspy==1.1.0.zip"
)

import multiprocessing as mp  # noqa

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pyspark.pandas as ps
import statsmodels.api as sm  # noqa: E402
from pandas.api.types import is_numeric_dtype  # noqa: E402

# from assesspy import cod  # noqa: E402
# from assesspy import prd_met  # noqa: E402
# from assesspy import cod_ci as cod_boot  # noqa: E402
# from assesspy import cod_met, mki, mki_met, prb, prb_met, prd  # noqa: E402
# from assesspy import prd_ci as prd_boot  # noqa: E402

# - - - -
# Paste assesspy functions
# - - - -


def calculate_gini(assessed, sale_price):
    df = pd.DataFrame({"av": assessed, "sp": sale_price})
    df = df.sort_values(by="sp", kind="mergesort")  # for stable sort results
    assessed_price = df["av"].values
    sale_price = df["sp"].values
    n = len(assessed_price)

    sale_sum = np.sum(sale_price * np.arange(1, n + 1))
    g_sale = 2 * sale_sum / np.sum(sale_price) - (n + 1)
    gini_sale = g_sale / n

    assessed_sum = np.sum(assessed_price * np.arange(1, n + 1))
    g_assessed = 2 * assessed_sum / np.sum(assessed_price) - (n + 1)
    gini_assessed = g_assessed / n

    return float(gini_assessed), float(gini_sale)


def check_inputs(*args):
    out = [""]

    for x in args:
        # *args passed into *args can created nested tuples - unnest
        if isinstance(x, tuple):
            args = x

    for x in args:
        if isinstance(x, pd.core.frame.DataFrame):
            raise Exception("Input cannot be a dataframe.")

        check = pd.Series(x)

        if not is_numeric_dtype(check):
            raise Exception("All input vectors must be numeric.")
        if check.isnull().values.any():
            out.append("\nInput vectors contain null values.")
        if len(check) <= 1:
            out.append("\nAll input vectors must have length greater than 1.")
        if not all(np.isfinite(check) | check.isnull()):
            out.append("\nInfinite values in input vectors.")
        if any(check == 0):
            out.append("\nInput vectors cannot contain values of 0.")

    out = set(out)

    if len(out) > 1:
        raise Exception("".join(map(str, out)))


# Formula specific bootstrapping functions
def cod_boot(ratio, nboot=100, alpha=0.05):
    return boot_ci(cod, ratio=ratio, nboot=nboot, alpha=alpha)


def prd_boot(assessed, sale_price, nboot=100, alpha=0.05):
    return boot_ci(
        prd, assessed=assessed, sale_price=sale_price, nboot=nboot, alpha=alpha
    )


# Spark API function
# Define CCAO-specific functions that work with Spark
def boot_ci(fun, nboot=100, alpha=0.05, **kwargs):
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


# Spark version
def cod(x):
    n = x.size
    median_ratio = x.median()
    # No numpy in here, as Spark doesn't seem to play well with it
    ratio_minus_med = x - median_ratio
    abs_diff_sum = ratio_minus_med.abs().sum()
    cod = 100 / median_ratio * (abs_diff_sum / n)
    return cod


def mki(assessed, sale_price):
    r"""
    The Modified Kakwani Index (mki) is a GINI-based measures
    to test for vertical equity. It first orders properties by sale price
    (ascending), then calculates the Gini coefficient for sale values
    and assessed values (while remaining ordered by sale price). The
    Modified Kakwani Index is the the ratio of Gini of Assessed / Gini of Sale.

    For the Modified Kakwani Index:

    MKI < 1 is regressive
    MKI = 1 is vertical equity
    MKI > 1 is progressive

    .. Quintos, C. (2020). A Gini measure for vertical equity in property
        assessments. https://researchexchange.iaao.org/jptaa/vol17/iss2/2

    .. Quintos, C. (2021). A Gini decomposition of the sources of inequality
    in property assessments.
        https://researchexchange.iaao.org/jptaa/vol18/iss2/6

    :param assessed:
        A numeric vector of assessed values. Must be the same
        length as ``sale_price``.
    :param sale_price:
        A numeric vector of sale prices. Must be the same length
        as ``assessed``.
    :type assessed: numeric
    :type sale_price: numeric
    :return: A numeric vector MKI of the input vectors.
    :rtype: float

    :Example:

    .. code-block:: python

        # Calculate MKI:
        import assesspy as ap

        mki(ap.ratios_sample().assessed, ap.ratios_sample().sale_price)
    """

    check_inputs(assessed, sale_price)
    gini_assessed, gini_sale = calculate_gini(assessed, sale_price)
    MKI = gini_assessed / gini_sale
    return float(MKI)


def prb(assessed, sale_price, round=None):
    r"""
    PRB is an index of vertical equity that quantifies the
    relationship betweem ratios and assessed values as a percentage. In
    concrete terms, a PRB of 0.02 indicates that, on average, ratios increase
    by 2\% whenever assessed values increase by 100 percent.

    PRB is centered around 0 and has a generally accepted value of between
    -0.05 and 0.05, as defined in the `IAAO Standard on Ratio Studies`_
    Section 9.2.7. Higher PRB values indicate progressivity in assessment,
    while negative values indicate regressivity.

    .. _IAAO Standard on Ratio Studies:
            https://www.iaao.org/media/standards/Standard_on_Ratio_Studies.pdf

    .. note: PRB is significantly less sensitive to outliers than PRD or COD.

    :param assessed:
        A numeric vector of assessed values. Must be the same
        length as ``sale_price``.
    :param sale_price:
        A numeric vector of sale prices. Must be the same length
        as ``assessed``.
    :param round:
        Indicate desired rounding for output.
    :type assessed: numeric
    :type sale_price: numeric
    :type round: int

    :return: A numeric vector containing the PRB of the input vectors.
    :rtype: float

    :Example:

    .. code-block:: python

        # Calculate PRB:
        import assesspy as ap

        ap.prb(ap.ratios_sample().assessed, ap.ratios_sample().sale_price)
    """

    assessed = np.array(assessed)
    sale_price = np.array(sale_price)
    check_inputs(assessed, sale_price)

    ratio = assessed / sale_price
    median_ratio = np.median(ratio)

    lhs = (ratio - median_ratio) / median_ratio
    rhs = np.log(((assessed / median_ratio) + sale_price) / 2) / np.log(2)

    lhs = np.array(lhs)
    rhs = np.array(rhs)

    prb_model = sm.OLS(lhs, rhs).fit()

    prb_val = float(prb_model.params)
    prb_ci = prb_model.conf_int(alpha=0.05)[0].tolist()

    if round is not None:
        out = {
            "prb": np.round(prb_val, round),
            "95% ci": np.round(prb_ci, round),
        }

    else:
        out = {"prb": prb_val, "95% ci": prb_ci}

    return out


def prd(assessed, sale_price):
    """
    PRD is the mean ratio divided by the mean ratio weighted by sale
    price. It is a measure of vertical equity in assessment. Vertical equity
    means that properties at different levels of the income distribution
    should be similarly assessed.

    PRD centers slightly above 1 and has a generally accepted value of between
    0.98 and 1.03, as defined in the `IAAO Standard on Ratio Studies`_
    Section 9.2.7. Higher PRD values indicate regressivity in assessment.

    .. _IAAO Standard on Ratio Studies:
        https://www.iaao.org/media/standards/Standard_on_Ratio_Studies.pdf

    .. note::
       The IAAO recommends trimming outlier ratios before calculating PRD,
       as it is extremely sensitive to large outliers. PRD is being deprecated
       in favor of PRB, which is less sensitive to outliers and easier to
       interpret.

    :param assessed:
        A numeric vector of assessed values. Must be the same
        length as ``sale_price``.
    :param sale_price:
        A numeric vector of sale prices. Must be the same length
        as ``assessed``.
    :type assessed: numeric
    :type sale_price: numeric

    :return: A numeric vector containing the PRD of the input vectors.
    :rtype: float

    :Example:

    .. code-block:: python

        # Calculate PRD:
        import assesspy as ap

        ap.prd(ap.ratios_sample().assessed, ap.ratios_sample().sale_price)
    """

    assessed = np.array(assessed)
    sale_price = np.array(sale_price)
    check_inputs(assessed, sale_price)

    ratio = assessed / sale_price
    prd = ratio.mean() / np.average(a=ratio, weights=sale_price)

    return prd


def cod_met(x):
    return 5 <= x <= 15


def prd_met(x):
    return 0.98 <= x <= 1.03


def prb_met(x):
    return -0.05 <= x <= 0.05


def mki_met(x):
    return 0.95 <= x <= 1.05


# - - -
# End assesspy paste
# - - -


def median_boot(ratio, nboot=100, alpha=0.05):
    return boot_ci(np.median, nboot=nboot, alpha=alpha, ratio=ratio)


# Spark compatible
def ccao_median(x):
    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = x.between(
        x.quantile(0.05), x.quantile(0.95), inclusive="neither"
    )

    x_no_outliers = x[no_outliers]
    median_n = x_no_outliers.size
    median_val = x_no_outliers.median()
    out = [median_val, median_n]

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


# Spark compatible
def ccao_cod(x):
    no_outliers = x.between(
        x.quantile(0.05), x.quantile(0.95), inclusive="neither"
    )

    x_no_outliers = x[no_outliers]
    cod_n = x_no_outliers.size

    if cod_n >= 20:
        cod_val = cod(x_no_outliers)
        cod_ci = cod_boot(ratio=x_no_outliers.to_numpy(), nboot=100)
        met = 5 <= cod_val <= 15
        out = [cod_val, cod_ci[0], cod_ci[1], met, cod_n]

    else:
        out = [None, None, None, None, cod_n]

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
    """
    Aggregates data and calculates summary statistics for given groupings
    """
    athena_user_logger.info(f"CHeck report 1")
    group_cols = [
        "year",
        "triad",
        "geography_type",
        "property_group",
        "assessment_stage",
        "geography_id",
        "sale_year",
    ]
    athena_user_logger.info(f"CHeck report 2")
    df["geography_id"] = df[geography_id].astype(str)
    athena_user_logger.info(f"CHeck report 2.5")
    df["geography_type"] = geography_type
    athena_user_logger.info(f"CHeck report 2.6")
    # Remove groups with less than three observations
    # TODO: Remove/upgrade detect_chasing output

    # df["n"] = df.groupby(group_cols)["ratio"].transform("count")
    athena_user_logger.info(f"CHeck report 2.7")
    # df = df[df["n"] > 3]
    athena_user_logger.info(f"CHeck report 3")

    df = df.groupby(group_cols).apply(
        lambda x: pd.Series(
            {
                "sale_n": x["triad"].size,
                **dict(
                    zip(["median_val", "median_n"], ccao_median(x["ratio"]))
                ),
                **dict(
                    zip(
                        [
                            "cod_val",
                            "cod_ci_l",
                            "ccao_ci_u",
                            "cod_met",
                            "cod_n",
                        ],
                        ccao_cod(x["ratio"]),
                    )
                ),
            }
        )
    )
    athena_user_logger.info(f"CHeck report 3.1")

    """
    df = df.groupby(group_cols).apply(
        lambda x: pd.Series(
            {
                "sale_n": np.size(x["triad"]),
                "ratio": ccao_median(x["ratio"]),
                "cod": ccao_cod(ratio=x["ratio"]),
                #"mki": ccao_mki(fmv=x["fmv"], sale_price=x["sale_price"]),
                #"prd": ccao_prd(fmv=x["fmv"], sale_price=x["sale_price"]),
                #"prb": ccao_prb(fmv=x["fmv"], sale_price=x["sale_price"]),
                "detect_chasing": False,
                #"within_20_pct": sum(abs(1 - x["ratio"]) <= 0.20),
                #"within_10_pct": sum(abs(1 - x["ratio"]) <= 0.10),
                #"within_05_pct": sum(abs(1 - x["ratio"]) <= 0.05),
            }
        )
    )
    """

    # Trying to get this operation to work
    athena_user_logger.info(f"{df.columns}")

    """
    # Convert the 'ratio' column to a list of lists
    ratio_lists = df['ratio'].apply(lambda x: [x, x, x])  # Repeating the value three times

    # Create a new DataFrame from these lists
    new_columns = ps.DataFrame(ratio_lists.tolist(), 
                            columns=['median_ratio', 'median_ratio_ci', 'median_ratio_n'],
                            index=df.index)

    # Add these new columns to the original DataFrame
    df = df.join(new_columns)
    """

    """
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
    """

    athena_user_logger.info(f"{df.columns}")

    # Arrange columns
    df = df[
        [
            "sale_n",
            # "median_ratio",
            # "median_ratio_ci",
            # "cod",
            # "cod_ci",
            # "cod_n",
            # "prd",
            # "prd_ci",
            # "prd_n",
            # "prb",
            # "prb_ci",
            # "prb_n",
            # "mki",
            # "mki_n",
            # "detect_chasing",
            # "ratio_met",
            # "cod_met",
            # "prd_met",
            # "prb_met",
            # "mki_met",
            # "vertical_equity_met",
            # "within_20_pct",
            # "within_10_pct",
            # "within_05_pct",
        ]
    ].reset_index()

    return df


def model(dbt, spark_session):
    dbt.config(materialized="table")

    input = dbt.ref("reporting.ratio_stats_input")

    # Convert the Spark input dataframe to Pandas for
    # compatibility with assesspy functions
    # input = input.toPandas()

    # Replicate filtering from prior vw_ratio_stats pull
    # input = input[input.ratio > 0 & input.ratio.notnull()]
    athena_user_logger.info("Check 1")
    input = ps.DataFrame(
        input.filter(input.ratio.isNotNull()).filter(input.ratio > 0)
    )
    athena_user_logger.info(" ")
    athena_user_logger.info(f"{input}")
    # report_summarise(input, "triad", "Tri")
    athena_user_logger.info(f"First done")
    # report_summarise(input, "township_code", "Town")
    athena_user_logger.info(f"Second done")
    df = ps.concat(
        [
            report_summarise(input, "triad", "Tri"),
            report_summarise(input, "township_code", "Town"),
        ]
    ).reset_index(drop=True)
    athena_user_logger.info(f"CHeck report 4.1")

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
