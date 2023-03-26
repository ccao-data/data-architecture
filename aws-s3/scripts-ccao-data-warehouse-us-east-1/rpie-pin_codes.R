# This script generates RPIE codes for new PINs, and fills forward RPIE codes for PINs (later than 2021)
# that already have them using the universe of PINs in iasWorld.
library(DBI)
library(digest)
library(glue)
library(ids)
library(noctua)
library(odbc)
library(dplyr)
library(tidyverse)
library(stringr)
source("utils.R")

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "rpie", "pin_codes")

# Connect to Athena
AWS_ATHENA_CONN_NOCTUA <- dbConnect(noctua::athena())

### PIN CODES

# Grab universe of PINs and all known RPIE codes
all_pins <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA,
  "
WITH all_pins AS (
  SELECT DISTINCT
      parid AS pin,
      Cast(Cast(taxyr AS INT) + 1 AS VARCHAR) AS year
  FROM iasworld.asmt_all
  WHERE taxyr >= '2018'
)

SELECT
    all_pins.pin,
    rpie_code,
    all_pins.year
FROM all_pins

LEFT JOIN rpie.pin_codes
    ON all_pins.year = pin_codes.year
    AND all_pins.pin = pin_codes.pin
"
)

# Function to create a binary checksum and convert it to a position in a list of characters
binary_checksum <- function(x) {

  counter <- (abs(
    digest::digest2int(x)
  ) %% 35) + 1

}

# Function to select a value from a list given the position provided by the checksum function
select_value <- function(x) {

  str_sub(
    str_sub(
      "ABCDEFGHJKLMNPQRSTUVWXYZ23456789",
      end = x
    ), -1
  )

}

# Function to generate new RPIE codes
gen_rpie_code <- function(x) {

  counters <- unlist(lapply(ids::uuid(n = 10), binary_checksum))

  output <- str_flatten(
    lapply(counters, select_value)
  )

  return(
    glue(
      "{str_sub(output, 1, 3)}-{str_sub(output, 4, 6)}-{str_sub(output, 7, 9)}-{str_sub(output, 10, 10)}"
    )
  )

}

# Fill codes forward for new year (don't fill missing codes prior to 2021)
all_pins <- bind_rows(
  all_pins %>%
    filter(year < 2021),
  all_pins %>%
    filter(year >= 2021) %>%
    group_by(pin) %>%
    arrange(year, .by_group = TRUE) %>%
    fill(rpie_code)
)

# Gather new pins without RPIE codes
assign_new_code <- all_pins %>%
  filter(is.na(rpie_code) & year >= 2022)

# Generate RPIE codes
assign_new_code$rpie_code <- unlist(lapply(1:nrow(assign_new_code), gen_rpie_code))

# Make sure no old codes have been assigned as new codes
while (any(assign_new_code$rpie_code %in% unique(all_pins$rpie_code))) {

  # Generate RPIE codes
  assign_new_code$rpie_code <- unlist(lapply(1:nrow(assign_new_code), gen_rpie_code))

}

# Recombine PINs
upload <- bind_rows(
  assign_new_code,
  all_pins %>%
    filter(!is.na(rpie_code) | year < 2022)
) %>%
  ungroup()

# Upload to S3
upload %>%
  group_by(year) %>%
  write_partitions_to_s3(output_bucket, is_spatial = FALSE, overwrite = TRUE)

# connect to CCAODATA
CCAODATA <- dbConnect(odbc::odbc(),
                      .connection_string = Sys.getenv("DB_CONFIG_CCAODATAW"))

# Check most recent data in CCAODATA.RPIE_PIN_CODES
max_year_CCAODATA <- dbGetQuery(
  conn = CCAODATA,
  "select max(TAX_YEAR) from rpie_pin_codes"
)

# Update CCAODATA.RPIE_PIN_CODES if need be
if (max(upload$year) > max_year_CCAODATA) {

  dbAppendTable(
    conn = CCAODATA,
    "rpie_pin_codes",
    upload %>%
      filter(year == max(year)) %>%
      select(rpie_pin = pin, tax_year = year, rpie_code)
    )

}

### PIN CODES DUMMY
aws.s3::get_bucket_df(file.path(AWS_S3_RAW_BUCKET)) %>%
  filter(str_detect(Key, "dummy")) %>%
  pull(Key) %>%
  walk(function(x) {

    # We just copy from raw to warehouse for dummy codes
    put_object(
      get_object(file.path(AWS_S3_RAW_BUCKET, x)),
      file.path(file.path(AWS_S3_WAREHOUSE_BUCKET, x))
    )

  })
