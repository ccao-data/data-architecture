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
source("utils.R")

# This script cleans and combines raw foreclosure data for the warehouse
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "sale", "foreclosure")

# Destination for upload
dest_file <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  max(get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "sale/foreclosure/")$Key)
)

# Get S3 file addresses
files <- grep(
  ".parquet",
  file.path(
    AWS_S3_RAW_BUCKET,
    get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "sale/foreclosure/")$Key
  ),
  value = TRUE
)
cook_bbox <- st_as_sfc(st_bbox(c(
  xmin = -88.351,
  xmax = -87.0299,
  ymax = 42.3395,
  ymin = 41.4625
), crs = st_crs(4326)))

# Load raw files, cleanup, then write to warehouse S3
output <- map(files, read_parquet) %>%
  rbindlist() %>%
  rename_with(~ tolower(gsub(" ", "_", .x))) %>%
  rename(pin = property_identification_number) %>%
  filter(!is.na(global_x), !is.na(global_y), date_of_sale < Sys.Date()) %>%
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
  select(-c(county, time_of_sale, datetime_of_sale, longitude, latitude)) %>%
  st_as_sf(coords = c("global_x", "global_y"), crs = 4326) %>%
  filter(as.logical(st_within(geometry, cook_bbox)), year_of_sale >= "2013") %>%
  mutate(geometry_3435 = st_transform(geometry, 3435)) %>%
  separate(
    bankruptcy_filed,
    sep = " - Chapter ", into = c(NA, "bankruptcy_chapter")
  ) %>%
  select(pin, everything(), geometry, geometry_3435, year_of_sale) %>%
  # Because of how we retrieve these sales, it's possible to get overlapping
  # data from one year to another. The data itself if good, but when we refresh
  # it every year we have to define a precise date range for which to gather
  # foreclosures. If one date range for any year overlaps the previous by even a
  # day, we'll get duplicates. Within duplicates it's possible for input date to
  # vary, so we only use distinct on the keys we've defined.
  distinct(case_number, pin, sale_results, date_of_sale, .keep_all = TRUE) %>%
  group_by(year_of_sale)

output %>%
  write_partitions_to_s3(output_bucket, is_spatial = TRUE, overwrite = TRUE)
