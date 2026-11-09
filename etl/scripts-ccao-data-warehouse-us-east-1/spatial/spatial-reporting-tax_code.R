library(DBI)
library(dplyr)
library(glue)
library(noctua)
library(sf)
source("utils.R")

# This script builds a map of combined tax code agencies.
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
export_path <- file.path(AWS_S3_WAREHOUSE_BUCKET, "export", "geojson")
output_path <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "reporting", "tax_code"
)

# Declare tax year for which to build the map.
tax_year <- 2024

# Connect to Athena
noctua_options(unload = TRUE)
AWS_ATHENA_CONN_NOCTUA <- dbConnect(noctua::athena(), rstudio_conn_tab = FALSE)

# Aggregate agency names and nums by tax code and rate. This will allow us to
# build a map of combined tax code agencies for a given tax year.
agencies <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA,
  glue_sql("
  WITH tax_codes AS (
    SELECT code.tax_code_num,
      code.tax_code_rate,
      agency.agency_num,
      info.agency_name,
      info.minor_type
    FROM tax.agency
      LEFT JOIN tax.tax_code as code ON code.agency_num = agency.agency_num
      AND agency.year = code.year
      LEFT JOIN tax.agency_info as info ON code.agency_num = info.agency_num
    WHERE agency.year = '{tax_year}'
  )
  SELECT tax_code_num,
    tax_code_rate,
    ARRAY_JOIN(
      ARRAY_AGG(agency_num ORDER BY agency_num),
      ', '
    ) AS agency_nums,
    ARRAY_JOIN(
      ARRAY_AGG(agency_name ORDER BY agency_name),
      ', '
    ) AS agency_names
  FROM tax_codes
  GROUP BY tax_code_num,
    tax_code_rate
",
    .con = AWS_ATHENA_CONN_NOCTUA
  )
)

pin_geometry <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA,
  glue_sql("
  SELECT pin10, geometry
  FROM tax.pin_geometry_raw
  WHERE {tax_year} BETWEEN start_year AND end_year
",
    .con = AWS_ATHENA_CONN_NOCTUA
  )
)
