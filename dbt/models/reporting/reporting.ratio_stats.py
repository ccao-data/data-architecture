# pylint: skip-file
# type: ignore
sc.addPyFile(  # noqa: F821
    "s3://ccao-athena-dependencies-us-east-1/assesspy==1.1.0.zip"
)

import multiprocessing as mp  # noqa

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
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


def boot_ci(fun, nboot=100, alpha=0.05, **kwargs):
    """
    Calculate the non-parametric bootstrap confidence interval
    for a given numeric input and a chosen function.

    :param fun:
        Function to bootstrap. Must return a single value.
    :param nboot:
        Default 100. Number of iterations to use to estimate
        the output statistic confidence interval.
    :param alpha:
        Default 0.05. Numeric value indicating the confidence
        interval to return. 0.05 will return the 95% confidence interval.
    :param kwargs:
        Arguments passed on to ``fun``.
    :type fun: function
    :type nboot: int
    :type alpha: float
    :type kwargs: numeric

    .. note::
       Input function should require 1 argument or be ``assesspy.prd()``.

    :return:
        A two-long list of floats containing the bootstrapped confidence
        interval of the input vector(s).
    :rtype: list[float]

    :Example:

    .. code-block:: python

        # Calculate PRD confidence interval:
        import assesspy as ap

        ap.boot_ci(
            ap.prd,
            assessed = ap.ratios_sample().assessed,
            sale_price = ap.ratios_sample().sale_price,
            nboot = 100
            )
    """

    # Make sure prd is passed arguments in correct order
    if fun.__name__ == "prd" and set(["assessed", "sale_price"]).issubset(
        kwargs.keys()
    ):
        kwargs = (kwargs["assessed"], kwargs["sale_price"])
    elif fun.__name__ == "prd" and not set(
        ["assessed", "sale_price"]
    ).issubset(kwargs.keys()):
        raise Exception(
            "PRD function expects argurments 'assessed' and 'sale_price'."
        )
    else:
        kwargs = tuple(kwargs.values())

    check_inputs(kwargs)  # Input checking and error handling

    num_kwargs = len(kwargs)
    kwargs = pd.DataFrame(kwargs).T
    n = len(kwargs)

    # Check that the input function returns a numeric vector
    out = (
        fun(kwargs.iloc[:, 0])
        if num_kwargs < 2
        else fun(kwargs.iloc[:, 0], kwargs.iloc[:, 1])
    )
    if not is_numeric_dtype(out):
        raise Exception("Input function outputs non-numeric datatype.")

    data_array = kwargs.to_numpy()

    def bootstrap_worker(
        data_array, fun, num_kwargs, n, nboot, start, end, result_queue
    ):
        ests = []
        for _ in range(start, end):
            sample_indices = np.random.choice(
                data_array.shape[0], size=n, replace=True
            )
            sample_array = data_array[sample_indices]
            if fun.__name__ == "cod" or num_kwargs == 1:
                ests.append(fun(sample_array[:, 0]))
            elif fun.__name__ == "prd":
                ests.append(fun(sample_array[:, 0], sample_array[:, 1]))
            else:
                raise Exception(
                    "Input function should require 1 argument or be assesspy.prd."  # noqa
                )
        result_queue.put(ests)

    def parallel_bootstrap(
        data_array, fun, num_kwargs, n, nboot, num_processes=4
    ):
        processes = []
        result_queue = mp.Queue()
        chunk_size = nboot // num_processes

        for i in range(num_processes):
            start = i * chunk_size
            end = start + chunk_size if i < num_processes - 1 else nboot
            p = mp.Process(
                target=bootstrap_worker,
                args=(
                    data_array,
                    fun,
                    num_kwargs,
                    n,
                    nboot,
                    start,
                    end,
                    result_queue,
                ),
            )
            processes.append(p)
            p.start()

        results = []
        for _ in range(num_processes):
            results.extend(result_queue.get())

        for p in processes:
            p.join()

        return results

    ests = parallel_bootstrap(data_array, fun, num_kwargs, n, nboot)

    ests = pd.Series(ests)

    ci = [ests.quantile(alpha / 2), ests.quantile(1 - alpha / 2)]

    return ci


def cod(ratio):
    """
    COD is the average absolute percent deviation from the
    median ratio. It is a measure of horizontal equity in assessment.
    Horizontal equity means properties with a similar fair market value
    should be similarly assessed.

    Lower COD indicates higher uniformity/horizontal equity in assessment.
    The IAAO sets uniformity standards that define generally accepted ranges
    for COD depending on property class. See `IAAO Standard on Ratio Studies`_
    Section 9.1, Table 1.3 for a full list of standard COD ranges.

    .. _IAAO Standard on Ratio Studies:
            https://www.iaao.org/media/standards/Standard_on_Ratio_Studies.pdf

    .. note::
        The IAAO recommends trimming outlier ratios before calculating COD,
        as it is extremely sensitive to large outliers. The typical method
        used is dropping values beyond 3 * IQR (inner-quartile range). See
        `IAAO Standard on Ratio Studies`_ Appendix B.1.

    :param ratio:
        A numeric vector of ratios centered around 1, where the
        numerator of the ratio is the estimated fair market value and the
        denominator is the actual sale price.
    :type ratio: numeric

    :return: A numeric vector containing the COD of ``ratios``.
    :rtype: float

    :Example:

    .. code-block:: python

        # Calculate COD:
        import assesspy as ap

        ap.cod(ap.ratios_sample().ratio)
    """
    check_inputs(ratio)

    ratio = np.array(ratio)

    n = ratio.size
    median_ratio = np.median(ratio)
    cod = 100 / median_ratio * (sum(abs(ratio - median_ratio)) / n)

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


def model(dbt, spark_session):
    dbt.config(materialized="table")

    input = dbt.ref("reporting.ratio_stats_input")

    # Convert the Spark input dataframe to Pandas for
    # compatibility with assesspy functions
    input = input.toPandas()

    # Replicate filtering from prior vw_ratio_stats pull
    input = input[input.ratio > 0 & input.ratio.notnull()]

    # df = report_summarise(input, "triad", "Tri")

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
