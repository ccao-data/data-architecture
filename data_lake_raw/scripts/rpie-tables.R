library(here)
library(dplyr)
library(purrr)
library(arrow)
library(odbc)
library(DBI)

# this script retrieves raw versions of all RPIE tables, with some PII trimmed out

# connect to RPIE SQL server
RPIE <- dbConnect(odbc::odbc(),
                      .connection_string = Sys.getenv("DB_CONFIG_CCAOAPPSRV"))

# a list of RPIE tables, stripped of extraneous tables
tables <- grep("Asp|Question|Audit|Deadline|qc", dbListTables(RPIE)[2:63], value = TRUE, invert = TRUE)

# a function to retrieve the entirety of each RPIE table list in "tables"
grab_table <- function(table_name) {

  return(

    dbGetQuery(RPIE, paste0("SELECT * FROM [", table_name, "]"))

  )

}

# grab those tables
output <- lapply(tables, grab_table)
names(output) <- tables

# all columns identified as PII, by table
PII <- list(
  "Attachment"         = c("DisplayFileName", "PhisicalFileName"),
  "Building"           = c("ProjectName"),
  "Filing"             = c("FilingName"),
  "IncomeExpenseHotel" = c("HotelName", "CompanyName"),
  "Party"              = c("FirstName", "LastName", "PrimaryPhone", "AlternativePhone", 'PartyIdentifier', "Email"),
  "TransferFiling"     = c("ToEmail", "SenderName"),
  "User"               = c("Email")
)

# clean tables of PII
for (i in names(output)) output[[i]] <- output[[i]] %>% mutate(across(PII[[i]], ~NA))

# a function to write each table in "output" to a parquet file
write_all_dataframes <- function(table, name) {

  arrow::write_parquet(table, here(paste0("s3-bucket/rpie/", name, ".parquet")))

}

# outputting all of the tables
mapply(write_all_dataframes, table = output, name = names(output))

# clean
rm(list = ls())