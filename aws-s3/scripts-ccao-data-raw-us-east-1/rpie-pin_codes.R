library(arrow)
library(aws.s3)
library(DBI)
library(odbc)
library(dplyr)
library(tidyr)
source("utils.R")

# This script retrieves codes needed for taxpayers to access the RPIE filing portal
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "rpie", "pin_codes")

# Connect to CCAODATA SQL server
conn <- dbConnect(
  odbc::odbc(),
  .connection_string = Sys.getenv("DB_CONFIG_CCAODATA")
)

# Write RPIE_PIN_CODES to S3 (RPIE did not exist prior to 2018, so only years 2019 and later will be uploaded)
dbGetQuery(conn, "SELECT * FROM RPIE_PIN_CODES WHERE TAX_YEAR >= 2019") %>%
  group_by(TAX_YEAR) %>%
  write_partitions_to_s3(output_bucket, is_spatial = FALSE, overwrite = TRUE)

# Cleanup
dbDisconnect(conn)