library(arrow)
library(aws.s3)
library(dplyr)
library(glue)
library(purrr)
library(stringr)
library(tidyr)
library(data.table)
library(DBI)
library(RJDBC)
source("utils.R")

# This script cleans and combines raw foreclosure data for the warehouse
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "ccao", "condominium", "pin_condo_char"
)

# Connect to the JDBC driver
aws_athena_jdbc_driver <- RJDBC::JDBC(
  driverClass = "com.simba.athena.jdbc.Driver",
  classPath = list.files("~/drivers", "^Athena.*jar$", full.names = TRUE),
  identifier.quote = "'"
)

# Establish connection
AWS_ATHENA_CONN_JDBC <- dbConnect(
  aws_athena_jdbc_driver,
  url = Sys.getenv("AWS_ATHENA_JDBC_URL"),
  aws_credentials_provider_class = Sys.getenv("AWS_CREDENTIALS_PROVIDER_CLASS"),
  Schema = "Default"
)

# Get S3 file addresses
files <- grep(
  ".parquet",
  file.path(
    AWS_S3_RAW_BUCKET,
    aws.s3::get_bucket_df(
      AWS_S3_RAW_BUCKET,
      prefix = "ccao/condominium/pin_condo_char/")$Key
  ),
  value = TRUE
)

# function to make different condo sheets stackable
clean_condo_sheets <- function(x) {

  read_parquet(x) %>%
    tibble(.name_repair = "unique") %>%
    rename_with( ~ tolower(.x)) %>%
    mutate(pin = str_pad(parid, 14, side = "left", pad = "0")) %>%
    select(contains(c('pin', 'sqft', 'bed', 'source'))) %>%
    select(-contains(c('x', 'all', 'search'))) %>%
    rename_with( ~ "bedrooms", contains('bed')) %>%
    rename_with( ~ "unit_sf", contains('unit')) %>%
    rename_with( ~ "building_sf", contains('building'))

}

# Grab sales/spatial data
classes <- dbGetQuery(
  conn = AWS_ATHENA_CONN_JDBC, "
  SELECT DISTINCT
      parid AS pin,
      class
  FROM iasworld.pardat
  WHERE taxyr = (SELECT MAX(taxyr) FROM iasworld.pardat)
      AND class IN ('299', '399')
  "
)

# Load raw files, cleanup, then write to warehouse S3
map(files, clean_condo_sheets) %>%
  rbindlist(fill = TRUE) %>%
  inner_join(classes) %>%
  mutate(across(c(unit_sf, building_sf), ~ na_if(., "0"))) %>%
  mutate(across(c(unit_sf, building_sf), ~ na_if(., "1"))) %>%
  mutate(across(c(building_sf, unit_sf, bedrooms), ~ gsub("[^0-9.-]", "", .))) %>%
  mutate(across(.cols = everything(), ~ trimws(., which = "both"))) %>%
  na_if("") %>%
  mutate(
    bedrooms = case_when(
      is.na(unit_sf) & bedrooms == "0" ~ NA_character_,
      TRUE ~ bedrooms
    )
  ) %>%
  mutate(across(c(building_sf, unit_sf, bedrooms), ~ as.numeric(.))) %>%
  mutate(
    parking_pin = str_detect(source, "(?i)parking|garage") & is.na(unit_sf) & is.na(building_sf),
    year = '2021'
  ) %>%
  select(-c(class, source)) %>%
  group_by(year) %>%
  arrow::write_dataset(
    path = output_bucket,
    format = "parquet",
    hive_style = TRUE,
    compression = "snappy"
  )
