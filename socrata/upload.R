library(DBI)
library(here)
library(dplyr)
library(readr)
library(lubridate)
library(RSocrata)
library(RJDBC)

# Queries and URLs to gather and upload open data
data_info <- read_csv(here("open_data_info.csv"))

# Setup the Athena JDBC driver
aws_athena_jdbc_driver <- RJDBC::JDBC(
  driverClass = "com.simba.athena.jdbc.Driver",
  classPath = list.files("~/drivers", "^Athena.*jar$", full.names = TRUE),
  identifier.quote = "'"
)

# Establish Athena connection
AWS_ATHENA_CONN_JDBC <- dbConnect(
  aws_athena_jdbc_driver,
  url = Sys.getenv("AWS_ATHENA_JDBC_URL"),
  aws_credentials_provider_class = Sys.getenv("AWS_CREDENTIALS_PROVIDER_CLASS"),
  Schema = "Default",
  WorkGroup = "read-only-with-scan-limit"
)

gather_upload <- function(query, url) {

  print(query)

  data <- dbGetQuery(conn = AWS_ATHENA_CONN_JDBC, query) %>%
    mutate(across(contains("date") & !contains("by"), ~ lubridate::as_date(.x)))

  RSocrata::write.socrata(
    data,
    url,
    update_mode = "REPLACE",
    Sys.getenv("SOCRATA_EMAIL"),
    Sys.getenv("SOCRATA_PASSWORD"),
    app_token = Sys.getenv("SOCRATA_APP_TOKEN")
  )

}

# Upload data to Socrata
walk2(data_info$query, data_info$socrataURL, ~ gather_upload(.x, .y))
