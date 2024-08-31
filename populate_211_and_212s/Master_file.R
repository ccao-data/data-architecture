library(DBI)
library(noctua)
library(dplyr)
library(xlsx)
library(readr)

AWS_ATHENA_CONN_NOCTUA <- dbConnect(noctua::athena())

missing_units <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA, read_file("Missing_units.sql")
)

data_quality <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA, read_file("MyDec_script.sql")
)


usps_data <- read.csv("usps_output.csv")
