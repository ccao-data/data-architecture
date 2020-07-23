# This script inserts a table of CofE reason codes into the SQL server.

# ingest necessary functions for overwriting DTBL_PINLOCATIONS
source(paste0("C:/Users/", Sys.info()[['user']],"/Documents/ccao_utility/code.r/99_utility_2.r"))
database <- 1

# load needed packages
library(DBI)
library(odbc)
library(readr)
library(safer)
library(installr)

# codes
codes <- read_delim("C:/Users/wridgew/Documents/ccao_sf_cama_dev/data_dictionary_constituents/cofe_codes.csv", delim = ",")

# Upload table to SQL server ----
CCAODATA <- dbConnect(odbc(),
                      driver   = "SQL Server",
                      server   = odbc.credentials("server"),
                      database = odbc.credentials("database"),
                      uid      = odbc.credentials("uid"),
                      pwd      = odbc.credentials("pwd"))

if (ask.user.yn.question("Are you certain you want to overwrite FTBL_CERT_OF_ERROR_REASONS?") == TRUE) {

  dbWriteTable(CCAODATA, "FTBL_CERT_OF_ERROR_REASONS", codes, overwrite = TRUE)

}

# disconnect after pulls
dbDisconnect(CCAODATA)