library(arrow)
library(aws.s3)
library(DBI)
library(odbc)
library(dplyr)
library(lubridate)
library(tidyr)
source("utils.R")

# This script retrieves codes needed for taxpayers to access the RPIE filing
# portal
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")

# Connect to CCAODATA SQL server
conn <- dbConnect(
  odbc::odbc(),
  .connection_string = Sys.getenv("DB_CONFIG_CCAODATA")
)

# Write RPIE_PIN_CODES to S3 (RPIE did not exist prior to 2018, so only years
# 2019 and later will be uploaded)
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "rpie", "pin_codes")

dbGetQuery(conn, "SELECT * FROM RPIE_PIN_CODES WHERE TAX_YEAR >= 2019") %>%
  mutate(TAX_YEAR = as.character(TAX_YEAR)) %>%
  group_by(TAX_YEAR) %>%
  write_partitions_to_s3(output_bucket, is_spatial = FALSE, overwrite = TRUE)

# Write dummy codes to S3
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "rpie", "pin_codes_dummy")

dbGetQuery(conn,
  "
  SELECT
    DISTINCT CAST(CAST(RPIE_PIN AS DECIMAL) AS VARCHAR) AS RPIE_PIN,
    RPIE_CODE
  FROM RPIE_PIN_CODES_DUMMY
  "
  ) %>%
  # Cross join dummy codes with all possible years of RPIE data
  crossing("TAX_YEAR" = as.character(2018:year(Sys.Date()) + 1)) %>%
  group_by(TAX_YEAR) %>%
  write_partitions_to_s3(output_bucket, is_spatial = FALSE, overwrite = TRUE)

# Cleanup
dbDisconnect(conn)