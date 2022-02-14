library(arrow)
library(aws.s3)
library(data.table)
library(dplyr)
library(glue)
library(lubridate)
library(purrr)
library(readr)
library(sf)
library(stringr)
library(tidyr)
library(tools)
library(odbc)
library(DBI)
library(RJDBC)
source("utils.R")

# This script cleans and combines raw foreclosure data for the warehouse
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "condominium", "characteristic")

# Destination for upload
dest_file <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  max(aws.s3::get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "condominium/characteristic/")$Key)
)

# Get S3 file addresses
files <- grep(
  ".parquet",
  file.path(
    AWS_S3_RAW_BUCKET,
    aws.s3::get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "condominium/characteristic/")$Key
  ),
  value = TRUE
)

clean_condo_sheets <- function(x) {

  read_parquet(x) %>%
    tibble(.name_repair = "unique") %>%
    select(!contains('Field')) %>%
    rename_with(~ tolower(
      str_replace_all(
        str_squish(
          str_replace_all(.x, c("'" = "", "%" = "per", "#" = "num ", "\\." = " "))
          ),
        " ", "_"
        )
      )) %>%
    rename_with(~"township", ends_with('iasworld_township')) %>%
    select(contains(c('parid', 'sqft', 'park', 'garage', 'source', ignore.case = TRUE)))

}

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

# Grab sales/spatial data
classes <- dbGetQuery(
  conn = AWS_ATHENA_CONN_JDBC,
  "select parid, class from iasworld.pardat where taxyr = (select max(taxyr) from iasworld.pardat)"
)

# Load raw files, cleanup, then write to warehouse S3
temp <- map(files, clean_condo_sheets) %>%

  rbindlist(fill = TRUE) %>%
  left_join(classes) %>%
  filter(class %in% c('299', '399')) %>%
  mutate(parking_pin = str_detect(source, "(?i)parking|garage")) %>%
  filter(str_detect(total_sqft_building, "[[:alpha:]]")) %>%
  filter(!duplicated(total_sqft_building))

  write_partitions_to_s3(output_bucket, is_spatial = TRUE, overwrite = TRUE)
