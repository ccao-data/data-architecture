# Load necessary packages
library(arrow)
library(RJDBC)
library(DBI)
library(readr)
library(dplyr)
library(glue)
options(java.parameters = "-Xmx8000m")

# Establish connection object, the drv parameter points to the locally stored
# Oracle JDBC driver from step 2. You can get it here:
# https://www.oracle.com/database/technologies/appdev/jdbc-ucp-21-1-c-downloads.html
IASWORLD <- dbConnect(
  drv = JDBC(
    driverClass = "oracle.jdbc.OracleDriver",
    classPath = Sys.getenv("DB_CONFIG_IASWORLD_DRV")
  ),
  url = Sys.getenv("DB_CONFIG_IASWORLD")
)

# Get all tables
tables <- dbGetQuery(
  IASWORLD,
  "SELECT table_name FROM ALL_TABLES WHERE OWNER = 'IASWORLD'"
) %>%
  pull(TABLE_NAME)

# Get top 1K rows of each table, then write to CSV
for (table in tables) {
  print(table)
  query_result <- dbGetQuery(
    IASWORLD,
    glue("SELECT * FROM IASWORLD.{table} FETCH FIRST 5000 ROWS ONLY")
  )

  write_csv(query_result, glue("output/tables/{table}.csv"))
}

dbGetQuery(
  IASWORLD,
  "SELECT *
   FROM ALL_CONSTRAINTS
   WHERE OWNER = 'IASWORLD'
   ORDER BY TABLE_NAME
  "
) %>%
  write_csv("output/metadata/ALL_CONSTRAINTS.csv")

dbGetQuery(
  IASWORLD,
  "SELECT *
   FROM ALL_TAB_COLUMNS
   WHERE OWNER = 'IASWORLD'
   ORDER BY TABLE_NAME
  "
) %>%
  write_csv("output/metadata/ALL_TAB_COLUMNS.csv")

dbGetQuery(
  IASWORLD,
  "SELECT *
   FROM IASWORLD.ASMT_ALL
   FETCH FIRST 50000 ROWS ONLY
  "
) %>%
  write_parquet("s3-bucket/iasworld/ASMT_ALL.parquet")
