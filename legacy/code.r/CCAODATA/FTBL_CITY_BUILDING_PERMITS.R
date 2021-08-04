source(paste0("C:/Users/", Sys.info()[['user']],"/Documents/ccao_utility/code.r/99_utility_2.r"))
database <- 1

library(RSocrata)
library(dplyr)
library(DBI)
library(odbc)
library(safer)
library(installr)

permits <- read.socrata("https://data.cityofchicago.org/resource/ydr8-5enu.json", stringsAsFactors = FALSE) %>%
  select("permit_", "permit_type", "issue_date", "total_fee",
         "pin1", "pin2", "pin3", "pin4", "pin5", "pin6", "pin7", "pin8", "pin9", "pin10")

permits$permit_type <- gsub("PERMIT - ", "", permits$permit_type)

clean_pins <- function(x) {

  x <- gsub("-", "", x)
  x <- ifelse(grepl("[[:alpha:]]|[[:punct:]]", x), NA, x)
  x <- ifelse(nchar(x) %in% c(10, 14), x, NA)
  x <- ifelse(nchar(x) == 10, paste0(x, '0000'), x)
  x <- ifelse(substr(x, 1, 10) %in% c('0000000000', '9999999999', '5555555555'), NA, x)

}

permits <- permits %>% mutate_at(vars(contains("pin")), function(x) clean_pins(x))

permits <- rename(permits, "permit_no" = "permit_")

# Upload table to SQL server ----
CCAODATA <- dbConnect(odbc(),
                      driver   = "SQL Server",
                      server   = odbc.credentials("server"),
                      database = odbc.credentials("database"),
                      uid      = odbc.credentials("uid"),
                      pwd      = odbc.credentials("pwd"))

if (ask.user.yn.question("Are you certain you want to overwrite FTBL_CITY_BUILDING_PERMITS?") == TRUE) {

  dbWriteTable(CCAODATA, "FTBL_CITY_BUILDING_PERMITS", permits, overwrite = TRUE)

}