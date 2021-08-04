# load necessary libraries
library(odbc)
library(DBI)
library(installr)
library(dplyr)

# create CCAODATA connection object
CCAODATA <- dbConnect(odbc(), .connection_string = Sys.getenv("DB_CONFIG_CCAODATA"))

# import class codes and encode
class_codes <- read_csv("O:/CCAODATA/data/raw_data/class_codes.csv") %>%
  mutate(DESCRIPTION = iconv(DESCRIPTION, "latin1", "UTF-8"))

# declare field type for description
sql.field.types <- list(DESCRIPTION = "nvarchar(500)")

# write table
if (ask.user.yn.question("Are you certain you want to overwrite DTBL_CLASSCODES?") == TRUE) {

  dbWriteTable(CCAODATA, "DTBL_CLASSCODES", class_codes, overwrite = TRUE, field.types = sql.field.types)

}