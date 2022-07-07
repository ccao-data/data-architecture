# Import necessary libraries
import boto3
import io
import time
import math
import numpy as np
import pandas as pd
import re
import s3fs
import statsmodels.api as sm
from statsmodels.distributions.empirical_distribution import ECDF
import warnings


# Define AWS boto3 clients
athena_client = boto3.client('athena')
glue_client = boto3.client('glue', region_name='us-east-1')
s3_client = boto3.client('s3')

# Define s3 and Athena paths
athena_db = 'iasworld'

s3_bucket = 'ccao-data-warehouse-us-east-1'
s3_prefix = 'reporting/ratio_stats/'
s3_output = 's3://'+ s3_bucket + '/' + s3_prefix
s3_ratio_stats = 's3://'+ s3_bucket + '/' + s3_prefix + 'ratio_stats.parquet'


# Functions to help with Athena queries ----
def poll_status(athena_client, execution_id):
    """ Checks the status of the a query using an incoming execution id and returns
    a 'pass' string value when the status is either SUCCEEDED, FAILED or CANCELLED. """

    result = athena_client.get_query_execution(QueryExecutionId=execution_id)
    state  = result['QueryExecution']['Status']['State']

    if state == 'SUCCEEDED':
        return 'pass'
    if state == 'FAILED':
        return 'pass'
    if state == 'CANCELLED':
        return 'pass'
    else:
        return 'not pass'

def poll_result(athena_client, execution_id):
    """ Gets the query result using an incoming execution id. This function is ran after the
    poll_status function and only if we are sure that the query was fully executed. """

    result = athena_client.get_query_execution(QueryExecutionId=execution_id)

    return result

def run_query_get_result(
  athena_client,
  s3_bucket,
  query,
  database,
  s3_output,
  s3_prefix):
    """ Runs an incoming query and returns the output as an s3 file like object.
    """

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
    })

    QueryExecutionId = response.get('QueryExecutionId')

    # Wait until query is executed
    while poll_status(athena_client, QueryExecutionId) != 'pass':
        time.sleep(2)
        pass

    result = poll_result(athena_client, QueryExecutionId)

    r_file_object = None

    # Only return file like object when the query succeeded
    if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        print("Query SUCCEEDED: {}".format(QueryExecutionId))

        s3_key = s3_prefix + QueryExecutionId + '.csv'

        r_file_object = boto3.resource('s3').Object(s3_bucket, s3_key)

    return r_file_object


# Athena query ----
SQL_QUERY = """
-- THIS SCRIPT NEEDS TO BE UPDATED WITH FINAL MODEL RUN IDs EACH YEAR

-- Valuation class from pardat
WITH classes AS (

    SELECT
        parid,
        taxyr,
        class,
        CASE WHEN class in ('299', '399') THEN 'CONDO'
            when class in ('211', '212') THEN 'MF'
            when class in ('202', '203', '204', '205', '206', '207', '208', '209', '210', '234', '278', '295') THEN 'SF'
            ELSE NULL END AS property_group
    FROM iasworld.pardat

    ),
-- Townships from legdat since pardat has some errors we can't accept for public reporting
townships AS (

    SELECT
        parid,
        taxyr,
        substr(TAXDIST, 1, 2) AS township_code,
        triad_code AS triad
    FROM iasworld.legdat

    LEFT JOIN spatial.township
        ON substr(TAXDIST, 1, 2) = township_code

),
-- Final model values
model_values AS (

    SELECT
        meta_pin AS parid,
        CAST(CAST(meta_year AS INT) + 1 AS VARCHAR) AS year,
        'model' AS assessment_stage,
        pred_pin_final_fmv_round AS total
    FROM model.assessment_pin

    LEFT JOIN classes
        ON assessment_pin.meta_pin = classes.parid AND assessment_pin.meta_year = classes.taxyr
    LEFT JOIN townships
        ON assessment_pin.meta_pin = townships.parid AND assessment_pin.meta_year = townships.taxyr

    WHERE run_id IN ('2022-04-26-beautiful-dan', '2022-04-27-keen-gabe')
        AND property_group IS NOT NULL

),
-- Values by assessment stages available in iasWorld (not model)
iasworld_values AS (

    SELECT
        asmt_all.parid,
        asmt_all.taxyr as year,
        CASE
            WHEN procname = 'CCAOVALUE' THEN 'mailed'
            WHEN procname = 'CCAOFINAL' THEN 'assessor certified'
            WHEN procname = 'BORVALUE'  THEN 'bor certified'
            ELSE NULL END AS assessment_stage,
        max(
            CASE
                WHEN asmt_all.taxyr < '2020' THEN ovrvalasm3
                WHEN asmt_all.taxyr >= '2020' THEN valasm3
                ELSE NULL END
            ) * 10 AS total
    FROM iasworld.asmt_all

    WHERE (valclass IS null OR asmt_all.taxyr < '2020')
      AND procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
      AND asmt_all.taxyr >= '2021'

    GROUP BY
        asmt_all.parid,
        asmt_all.taxyr,
        procname,
        CASE
            WHEN procname = 'CCAOVALUE' THEN 'mailed'
            WHEN procname = 'CCAOFINAL' THEN 'assessor certified'
            WHEN procname = 'BORVALUE'  THEN 'bor certified'
            ELSE NULL END
),
-- Stack iasWorld and model values
all_values AS (
    SELECT * FROM model_values
    UNION
    SELECT * FROM iasworld_values
)
-- Sales, filtered to exclude outliers and mutlisales
    SELECT
        vps.pin,
        av.year,
        vps.year AS sale_year,
        property_group,
        assessment_stage,
        triad,
        townships.township_code,
        av.total AS fmv,
        sale_price,
        av.total / sale_price AS ratio
    FROM default.vw_pin_sale vps

    LEFT JOIN classes
        ON vps.pin = classes.parid AND vps.year = classes.taxyr
    LEFT JOIN townships
        ON vps.pin = townships.parid AND vps.year = townships.taxyr
    -- Join sales so that values for a given year can be compared to a complete set of sales from the previous year
    INNER JOIN all_values av ON vps.pin = av.parid
        AND CAST(vps.year AS INT) = CAST(av.year AS INT) - 1
    -- Grab parking spaces and join them to aggregate stats for removal
    LEFT JOIN (
      SELECT * FROM default.vw_pin_condo_char WHERE is_parking_space = TRUE
      ) ps ON av.parid = ps.pin AND av.year = ps.year

    WHERE is_multisale = FALSE
        AND property_group IS NOT NULL
        AND ps.pin IS NULL;
"""


# Run run_query_get_result to get file like object ----
r_file_object = run_query_get_result(
    athena_client,
    s3_bucket,
    SQL_QUERY,
    athena_db,
    s3_output,
    s3_prefix
)

# Retrieve s3 location of Athena query result and retrieve it
target = 's3://'+ s3_bucket + '/' + r_file_object.key

pull = pd.read_csv(target)
pull = pull[pull.ratio > 0 & pull.ratio.notnull()]

# Delete all query results for this job from s3 bucket
response = s3_client.list_objects_v2(Bucket = s3_bucket, Prefix = s3_prefix)

for object in response['Contents']:
    if re.search("csv", object['Key']):
        print('Deleting', object['Key'])
        s3_client.delete_object(Bucket = s3_bucket, Key = object['Key'])


# COD, PRD, PRB functions ----
def cod(ratio):

    n = ratio.size
    median_ratio = ratio.median()
    cod = 100 / median_ratio * (sum(abs(ratio - median_ratio)) / n)

    return cod

def prd(fmv, sale_price):

    ratio = fmv / sale_price
    prd = ratio.mean() / np.average(a = ratio, weights = sale_price)

    return prd

def prb(fmv, sale_price):

    ratio = fmv / sale_price
    median_ratio = ratio.median()

    lhs = (ratio - median_ratio) /median_ratio
    rhs = np.log(((fmv / median_ratio) + sale_price) / 2) / np.log(2)

    lhs = np.array(lhs)
    rhs = np.array(rhs)

    return sm.OLS(lhs, rhs).fit()

# Functions to determine whether assessment fairness criteria has been met
cod_met = lambda x: 5 <= x <= 15

prd_met = lambda x: 0.98 <= x <= 1.03

prb_met = lambda x: -0.05 <= x <= 0.05

# General boostrapping function
def boot_ci(fun, *args, nboot = 100, alpha = 0.05):

    num_args = len(args)
    args = pd.DataFrame(args).T
    n = len(args)

    ests = []

    for i in list(range(1, nboot)):
        sample = args.sample(n = n, replace = True)
        if fun.__name__ == 'cod' or num_args == 1:
            ests.append(fun(sample.iloc[:, 0]))
        elif fun.__name__ in ['prd', 'prb']:
            ests.append(fun(sample.iloc[:, 0], sample.iloc[:, 1]))

    ests = pd.Series(ests)

    ci = [ests.quantile(alpha / 2), ests.quantile(1 - alpha / 2)]

    ci = ', '.join([str(element) for element in ci])

    return ci

# Formula specific bootstrapping functions
def cod_boot(ratio, nboot = 100, alpha = 0.05):

    return boot_ci(cod, ratio, nboot = nboot, alpha = alpha)

def prd_boot(fmv, sale_price, nboot = 100, alpha = 0.05):

    return boot_ci(prd, fmv, sale_price, nboot = nboot, alpha = alpha)

def prb_boot(fmv, sale_price, nboot = 100, alpha = 0.05):

    return boot_ci(prb, fmv, sale_price, nboot = nboot, alpha = alpha)

def median_boot(ratio, nboot = 100, alpha = 0.05):

    return boot_ci(np.median, ratio, nboot = nboot, alpha = alpha)

# Fairness metrics functions that comply with CCAO's SOPs
def ccao_cod(ratio):
    """ """

    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = ratio[ratio.between(ratio.quantile(0.05), ratio.quantile(0.95), inclusive = "neither")]

    cod_n = no_outliers.size

    if cod_n >= 20:

        cod_val = cod(no_outliers)
        cod_ci = boot_ci(cod, no_outliers, nboot = 1000)
        met = cod_met(cod_val)

        out = [cod_val, cod_ci, met, cod_n]

    else:

        out = [None, None, None, cod_n]

    return out

def ccao_prd(fmv, sale_price):

    ratio = fmv / sale_price

    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = ratio.between(ratio.quantile(0.05), ratio.quantile(0.95), inclusive = "neither")

    fmv_no_outliers = fmv[no_outliers == True]
    sale_price_no_outliers = sale_price[no_outliers == True]

    prd_n = sum(no_outliers)

    if prd_n >= 20:

        prd_val = prd(fmv_no_outliers, sale_price_no_outliers)
        prd_ci = prd_boot(fmv_no_outliers, sale_price_no_outliers, nboot = 1000)
        met = prd_met(prd_val)

        out = [prd_val, prd_ci, met, prd_n]

    else:

        out = [None, None, None, prd_n]

    return out

def ccao_prb(fmv, sale_price):

    ratio = fmv / sale_price

    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = ratio.between(ratio.quantile(0.05), ratio.quantile(0.95), inclusive = "neither")

    fmv_no_outliers = fmv[no_outliers == True]
    sale_price_no_outliers = sale_price[no_outliers == True]

    prb_n = sum(no_outliers)

    if prb_n >= 20:

        prb_model = prb(fmv_no_outliers, sale_price_no_outliers)
        prb_val = float(prb_model.params)
        prb_ci = ', '.join(
            [str(element) for element in prb_model.conf_int(alpha = 0.05)[0].tolist()]
            )
        met = prb_met(prb_val)

        out = [prb_val, prb_ci, met, prb_n]

    else:

        out = [None, None, None, prb_n]

    return out

def ccao_median(x):

    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = x.between(x.quantile(0.05), x.quantile(0.95), inclusive = "neither")

    x_no_outliers = x[no_outliers == True]

    median_n = sum(no_outliers)

    median_val = np.median(x_no_outliers)
    median_ci = median_boot(x, nboot = 1000)

    out = [median_val, median_ci, median_n]

    return(out)

# Sales chasing functions
def detect_chasing_cdf(ratio, bounds = [0.98, 1.02], cdf_gap = 0.03):
    # CDF gap method for detecting sales chasing.

    # Sort the ratios
    sorted_ratio = ratio.sort_values()

    # Calculate the CDF of the sorted ratios and extract percentile ranking
    cdf = ECDF(sorted_ratio)(sorted_ratio)

    # Calculate the difference between each value and the next value, the largest
    # difference will be the CDF gap
    diffs = np.diff(cdf)

    # Check if the largest difference is greater than the threshold and make sure
    # it's within the specified boundaries
    diff_loc = sorted_ratio.iloc[np.argmax(diffs)]
    out = (max(diffs) > cdf_gap) & ((diff_loc > bounds[0]) & (diff_loc < bounds[1]))

    return(out)

def detect_chasing_dist(ratio, bounds = [0.98, 1.02]):
    # Distribution comparison method for detecting sales chasing.

    # Return the percentage of x within the specified range
    def pct_in_range(x, min, max):
        out = np.mean(((x >= min) & (x <= max)))
        return out

    # Calculate the ideal normal distribution using observed values from input
    ideal_dist = np.random.normal(
        loc = np.mean(ratio),
        scale = np.std(ratio),
        size = 10000
        )

    # Determine what percentage of the data would be within the specified bounds
    # in the ideal distribution
    pct_ideal = pct_in_range(ideal_dist, bounds[0], bounds[1])

    # Determine what percentage of the data is actually within the bounds
    pct_actual = pct_in_range(ratio, bounds[0], bounds[1])

    return pct_actual > pct_ideal

def detect_chasing(ratio, method = 'both'):

    # Remove top and bottom 5% of ratios as per CCAO Data Department SOPs
    no_outliers = ratio.between(ratio.quantile(0.05), ratio.quantile(0.95), inclusive = "neither")

    if len(no_outliers) < 30:
        warnings.warn(
            """Sales chasing detection can be misleading when applied to small samples (N < 30).
            Increase N or use a different statistical test."""
            )

        out = None

    else:
        out = {
            'cdf': detect_chasing_cdf(no_outliers),
            'dist': detect_chasing_dist(no_outliers),
            'both': (detect_chasing_cdf(no_outliers) & detect_chasing_dist(no_outliers))
        }.get(method)

    return out

def report_summarise(df, geography_id, geography_type):
    # Aggregates data and calculates summary statistics for given groupings

    group_cols = ['year', 'triad', 'geography_type', 'property_group', 'assessment_stage', 'geography_id', 'sale_year']

    df['geography_id'] = pull[geography_id]
    df['geography_type'] = geography_type

    # Remove groups with less than three observations
    df['n'] = df.groupby(group_cols)['ratio'].transform('count')
    df = df[df['n'] > 2]
    df = df.groupby(group_cols).apply(
        lambda x: pd.Series({
            'sale_n':np.size(x['triad']),
            'ratio':ccao_median(x['ratio']),
            'cod':ccao_cod(ratio = x['ratio']),
            'prd':ccao_prd(fmv = x['fmv'], sale_price = x['sale_price']),
            'prb':ccao_prb(fmv = x['fmv'], sale_price = x['sale_price']),
            'detect_chasing':detect_chasing(ratio = x['ratio']),
            'within_20_pct':sum(abs(1 - x['ratio']) <= .20),
            'within_10_pct':sum(abs(1 - x['ratio']) <= .10),
            'within_05_pct':sum(abs(1 - x['ratio']) <= .05),
            })
        )
    df[['median_ratio', 'median_ratio_ci', 'median_ratio_n']] = pd.DataFrame(df.ratio.tolist(), index = df.index)
    df[['cod', 'cod_ci', 'cod_met', 'cod_n']] = pd.DataFrame(df.cod.tolist(), index = df.index)
    df[['prd', 'prd_ci', 'prd_met', 'prd_n']] = pd.DataFrame(df.prd.tolist(), index = df.index)
    df[['prb', 'prb_ci', 'prb_met', 'prb_n']] = pd.DataFrame(df.prb.tolist(), index = df.index)
    df['ratio_met'] = abs(1 - df['median_ratio']) <= .05
    df['vertical_equity_met'] = (df.prd_met | df.prb_met)

    # Arrange output columns
    df = df[[
        'sale_n',
        'median_ratio',
        'median_ratio_ci',
        'cod',
        'cod_ci',
        'cod_n',
        'prd',
        'prd_ci',
        'prd_n',
        'prb',
        'prb_ci',
        'prb_n',
        'detect_chasing',
        'ratio_met',
        'cod_met',
        'prd_met',
        'prb_met',
        'vertical_equity_met',
        'within_20_pct',
        'within_10_pct',
        'within_05_pct'
]].reset_index()

    return df


# Append and write output to s3 bucket
pd.concat([
    report_summarise(pull, 'triad', 'Tri'),
    report_summarise(pull, 'township_code', 'Town')
    ]).to_parquet(
        s3_ratio_stats
        )

# Trigger reporting glue crawler
glue_client.start_crawler(Name='ccao-data-warehouse-reporting-crawler')