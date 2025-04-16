# This script generates aggregated summary stats on assessed values across a
# number of geographies, class combinations, and time.
#%%
# Import libraries
import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import pandas_udf

# Connect to Athena
cursor = connect(
    s3_staging_dir="s3://ccao-athena-results-us-east-1/",
    region_name="us-east-1",
    cursor_class=PandasCursor,
).cursor(unload=True)

data = cursor.execute("select * from z_ci_387_reporting_sot_reporting.sot_assessment_roll_input").as_pandas()

spark = SparkSession.builder.appName("SparkByExamples.com").master("local[*]").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
#%%
schema = {'stage_name': 'string',
'class': 'string',
'av_tot': 'double',
'av_bldg': 'double',
'av_land': 'double',
'county': 'string',
'triad': 'string',
'township': 'string',
'nbhd': 'string',
'tax_code': 'string',
'zip_code': 'string',
'community_area': 'string',
'census_place': 'string',
'census_tract': 'string',
'census_congressional_district': 'string',
'census_zcta': 'string',
'cook_board_of_review_district': 'string',
'cook_commissioner_district': 'string',
'cook_judicial_district': 'string',
'ward_num': 'string',
'police_district': 'string',
'school_elementary_district': 'string',
'school_secondary_district': 'string',
'school_unified_district': 'string',
'tax_municipality': 'string',
'tax_park_district': 'string',
'tax_library_district': 'string',
'tax_fire_protection_district': 'string',
'tax_community_college_district': 'string',
'tax_sanitation_district': 'string',
'tax_special_service_area': 'string',
'tax_tif_district': 'string',
'central_business_district': 'string',
'census_data_year': 'string',
'cook_board_of_review_district_data_year': 'string',
'cook_commissioner_district_data_year': 'string',
'cook_judicial_district_data_year': 'string',
'ward_data_year': 'string',
'community_area_data_year': 'string',
'police_district_data_year': 'string',
'central_business_district_data_year': 'string',
'school_data_year': 'string',
'tax_data_year': 'string',
'no_group': 'string',
'major_class': 'string',
'modeling_group': 'string',
'res_other': 'string',
'year': 'string',}
schema = ", ".join(f"{key}: {val}" for key, val in schema.items())
spark_df = spark.createDataFrame(data, schema=schema)
#%%

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
    if len(x) >= 1:
        output = x.iloc[0]
    else:
        output = None

    return output

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

stats = {
    "av_tot": ["size", "count"] + more_stats,
    "av_bldg": more_stats,
    "av_land": more_stats,
    "triad": [first],
    "geography_data_year": [first],
}

#%%
schema = {'stage_name': 'string',
'av_tot': 'double','av_bldg': 'double','av_land': 'double',}
schema = ", ".join(f"{key}: {val}" for key, val in schema.items())
spark_df = spark.createDataFrame(data[['stage_name', 'av_tot', 'av_bldg', 'av_land']], schema=schema)
#%%
def aggregate(key, pdf):

    columns = ['av_tot', 'av_bldg', 'av_land']

    out = ()
    for column in ['av_tot', 'av_bldg', 'av_land']:
        out += (
                pdf[column].min(),
                q10(pdf[column]),
                q25(pdf[column]),
                pdf[column].median(),
                q75(pdf[column]),
                q90(pdf[column]),
                pdf[column].max(),
                pdf[column].mean(),
                pdf[column].sum(),
            )

    return pd.DataFrame([
        key + out
    ])

spark_df.groupby("stage_name").applyInPandas(aggregate, schema="stage_name string, min_av_tot double, q10_av_tot double, q25_av_tot double, median_av_tot double, q75_av_tot double, q90_av_tot double, max_av_tot double, mean_av_tot double, sum_av_tot double").show()
# %%