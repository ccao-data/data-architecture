# This script generates aggregated summary stats on sales data across a number
# of geographies, class combinations, and time.

import os.path
import statistics as stats

# Import libraries
import awswrangler as wr
import numpy as np
import pandas as pd

# Ingest data if it is not already available
if os.path.isfile("sot_sales.parquet.gzip"):
    df = pd.read_parquet("sot_sales.parquet.gzip")

else:
    sql = open("reporting.sot_sales.sql").read()
    df = wr.athena.read_sql_query(sql, database="default", ctas_approach=False)
    df.to_parquet("sot_sales.parquet.gzip", compression="gzip")

# Declare geographic groups and their associated data years
geos = {
    "census_data_year": [
        "census_place",
        "census_tract",
        "census_congressional_district",
        "census_zcta",
    ],
    "cook_board_of_review_district_data_year": [
        "cook_board_of_review_district"
    ],
    "cook_commissioner_district_data_year": ["cook_commissioner_district"],
    "cook_judicial_district_data_year": ["cook_judicial_district"],
    "ward_data_year": ["ward_num"],
    "community_area_data_year": ["community_area"],
    "police_district_data_year": ["police_district"],
    "central_business_district_data_year": ["central_business_district"],
    "school_data_year": [
        "school_elementary_district",
        "school_secondary_district",
        "school_unified_district",
    ],
    "tax_data_year": [
        "tax_municipality",
        "tax_park_district",
        "tax_library_district",
        "tax_fire_protection_district",
        "tax_community_college_district",
        "tax_sanitation_district",
        "tax_special_service_area",
        "tax_tif_district",
    ],
}
# Declare class groupings
groups = ["no_group", "class", "major_class", "modeling_group"]


# Define aggregation functions
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

agg_func_math = {
    "sale_price": ["size", "count"] + more_stats,
    "price_per_sf": more_stats,
    "char_bldg_sf": ["median"],
    "char_land_sf": ["median"],
    "char_yrblt": ["median"],
    "class": [stats.multimode],
    "data_year": [first],
}

# Create an empty dataframe to fill with output
output = pd.DataFrame()

# Loop through group combinations and stack output
for key, value in geos.items():
    df["data_year"] = df[key]

    for x in value:
        for z in groups:
            group = [x, z, "year"]
            summary = df.groupby(group).agg(agg_func_math).round(2)
            summary["geography_type"] = x
            summary["group_type"] = z
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

            output = pd.concat([output, summary])

# Clean combined output and export
output["sale_price", "sum"] = output["sale_price", "sum"].replace(0, np.NaN)
output["price_per_sf", "sum"] = output["price_per_sf", "sum"].replace(
    0, np.NaN
)

for i in ["median", "mean", "sum"]:
    output["sale_price", "delta" + i] = output["sale_price", i].diff()
    output["price_per_sf", "delta" + i] = output["price_per_sf", i].diff()

output.columns = ["_".join(col) for col in output.columns]
output.reset_index()

output.to_csv("sot_sales.csv")
