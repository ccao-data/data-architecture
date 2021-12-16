library(arrow)
library(aws.s3)
library(dplyr)
library(tidyr)
library(readr)
library(stringr)
library(tools)
library(glue)
library(data.table)
library(lubridate)

# This script cleans and combines raw foreclosure data for the warehouse
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

# Destination for upload
dest_file <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  max(aws.s3::get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "sale/foreclosure/")$Key)
)

# Get S3 file addresses
files <- grep(
  ".parquet",
  file.path(
    AWS_S3_RAW_BUCKET,
    aws.s3::get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "sale/foreclosure/")$Key
  ),
  value = TRUE
)

lapply(files, read_parquet) %>%
  rbindlist() %>%
  rename_with(~ tolower(gsub(" ", "_", .x))) %>%
  rename(pin = property_identification_number) %>%
  mutate(
    pin = gsub("[^0-9.-]", "", pin),
    foreclosure_recording_date = lubridate::mdy(foreclosure_recording_date),
    across(
      c(property_type, real_estate_auction, sale_results, mortgage_type),
      toupper
    ),
    datetime_of_sale = lubridate::ymd_hm(paste(
      date_of_sale,
      str_sub(str_pad(readr::parse_number(time_of_sale), 4, "left", "1"), 1, 4)
    )),
    date_of_sale = as_datetime(ifelse(
      !is.na(datetime_of_sale),
      datetime_of_sale,
      as_datetime(ymd(date_of_sale))
    )),
    date_of_interest_calculation = mdy(date_of_interest_calculation),
    deed_recording_date = mdy(deed_recording_date),
    across(
      c(ends_with("_amount"), contains("income"), ends_with("_price")),
      parse_number
    ),
    across(c(contains("zip_code")), as.character),
    mortgage_type = case_when(
      mortgage_type == "ARM" ~ "ARM",
      mortgage_type == "BALLOON" ~ "BALLOON",
      mortgage_type == "FHA" ~ "FHA",
      mortgage_type == "VA" ~ "VA",
      str_detect(mortgage_type, "COMMER") ~ "COMMERCIAL",
      str_detect(mortgage_type, "CONSTR") ~ "CONSTRUCTION",
      str_detect(mortgage_type, "CONVENT") ~ "CONVENTIONAL",
      str_detect(mortgage_type, "MECHAN") ~ "MECHANIC LIEN",
      str_detect(mortgage_type, "OWNER ") ~ "OWNER FINANCE",
      str_detect(mortgage_type, "REVERSE|RES") ~ "REVERSE",
      !is.na(mortgage_type) ~ "OTHER",
      TRUE ~ NA_character_
    ),
    across(c(db_id_number, real_estate_db_id), as.integer),
    year_of_sale = as.character(lubridate::year(date_of_sale)),
    across(c(contains("district"), contains("phone")), as.character)
  ) %>%
  select(-c(county, time_of_sale, datetime_of_sale, global_x, global_y)) %>%
  rename(lon = longitude, lat = latitude) %>%
  separate(bankruptcy_filed, sep = " - Chapter ", into = c(NA, "bankruptcy_chapter")) %>%
  select(pin, everything(), year_of_sale) %>%
  group_by(year_of_sale) %>%
  group_walk(~ {
    year_of_sale <- replace_na(.y$year_of_sale, "__HIVE_DEFAULT_PARTITION__")
    remote_path <- file.path(
      AWS_S3_WAREHOUSE_BUCKET, "sale", "foreclosure",
      paste0("year_of_sale=", year_of_sale),
      "part-0.parquet"
    )
    if (!object_exists(remote_path)) {
      print(paste("Now uploading:", year_of_sale, "data for year:", year_of_sale))
      tmp_file <- tempfile(fileext = ".parquet")
      write_parquet(.x, tmp_file, compression = "snappy")
      aws.s3::put_object(tmp_file, remote_path)
    }
  })

# Cleanup
rm(list = ls())
