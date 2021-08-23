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

# cleanse tables of PII
output[["Attachment"]] <- output[["Attachment"]] %>%
  mutate(across(c(DisplayFileName, PhisicalFileName), ~NA))

output[["Building"]] <- output[["Building"]] %>%
  mutate(across(c(ProjectName), ~NA))

output[["Filing"]] <- output[["Filing"]] %>%
  mutate(across(c(FilingName), ~NA))

output[["IncomeExpenseHotel"]] <- output[["IncomeExpenseHotel"]] %>%
  mutate(across(c(HotelName, CompanyName), ~NA))

output[["Party"]] <- output[["Party"]] %>%
  mutate(across(c(FirstName, LastName, PrimaryPhone, AlternativePhone, PartyIdentifier, Email), ~NA))

output[["TransferFiling"]] <- output[["TransferFiling"]] %>%
  mutate(across(c(ToEmail, SenderName), ~NA))

output[["User"]] <- output[["User"]] %>%
  mutate(across(c(Email), ~NA))

# a function to write each table in "output" to a parquet file
write_all_dataframes <- function(table, name) {

  arrow::write_parquet(table, here(paste0("s3-bucket/rpie/", name, ".parquet")))

}

# outputting all of the tables
mapply(write_all_dataframes, table = output, name = tables)
