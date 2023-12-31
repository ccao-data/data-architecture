library(arrow)
library(aws.s3)
library(DBI)
library(dplyr)
library(odbc)
library(stringr)
source("utils.R")

# This script retrieves raw versions of all RPIE tables with PII trimmed out
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "rpie", "data")

# Connect to RPIE SQL server
RPIE <- dbConnect(
  odbc::odbc(),
  .connection_string = Sys.getenv("DB_CONFIG_CCAOAPPSRV")
)

# Get list of RPIE tables, stripped of extraneous tables
tables <- grep(
  "Asp|Question|Audit|Deadline|qc|archive|bak",
  dbListTables(RPIE)[2:63],
  value = TRUE,
  invert = TRUE
)

# Function to retrieve the entirety of each RPIE table
grab_table <- function(table_name) {
  dbGetQuery(RPIE, paste0("SELECT * FROM [", table_name, "]"))
}

# Grab listed tables
output <- lapply(tables, grab_table)
names(output) <- tables

# All columns identified as PII, by table
PII <- list(
  "Attachment"         = c("DisplayFileName", "PhisicalFileName"),
  "Building"           = c("ProjectName"),
  "Filing"             = c("FilingName"),
  "IncomeExpenseHotel" = c("HotelName", "CompanyName"),
  "Party"              = c("FirstName", "LastName", "PrimaryPhone", "AlternativePhone", "PartyIdentifier", "Email"),
  "TransferFiling"     = c("ToEmail", "SenderName"),
  "User"               = c("Email")
)

# Strip tables of PII
for (i in names(output)) {
  output[[i]] <- output[[i]] %>%
    mutate(across(PII[[i]], ~ NA))
}

# Save all of table to S3
walk2(output, names(output), function(table, name) {

  # Write each table in "output" to a parquet file on S3
  write_parquet(
    table,
    file.path(output_bucket, paste0(name, ".parquet"))
    )

})

# Cleanup
dbDisconnect(RPIE)