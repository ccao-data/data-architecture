# Load necessary packages
library(arrow)
library(RJDBC)
library(DBI)
library(dplyr)
library(here)
library(glue)
options(java.parameters = "-Xmx16G")

parquet_compression_lib = "snappy"
parquet_output_path <- here("s3-bucket", "iasworld", "data")
iasworld_conn_string <- Sys.getenv("DB_CONFIG_IASWORLD")
iasworld_drvr_path <- Sys.getenv("DB_CONFIG_IASWORLD_DRV")

# Establish connection object, the drv parameter points to the locally stored
# Oracle JDBC driver from step 2. You can get it here:
# https://www.oracle.com/database/technologies/appdev/jdbc-ucp-21-1-c-downloads.html
iasworld_conn <- dbConnect(
  drv = JDBC(
    driverClass = "oracle.jdbc.OracleDriver",
    classPath = iasworld_drvr_path,
  ),
  url = iasworld_conn_string
)

print("Successfully connected to iasWorld")

# Get all tables
tables <- dbGetQuery(
  iasworld_conn,
  "SELECT table_name FROM ALL_TABLES WHERE OWNER = 'IASWORLD'"
) %>%
  pull(TABLE_NAME)

tables <- c(tables, "ASMT_ALL")

# Get all tables
for (table in tables) {
  print(glue("Pulling {table}"))
  query_result <- dbGetQuery(
    iasworld_conn,
    glue("SELECT * FROM IASWORLD.{table}")
  )

  print(glue("Writing {table} to parquet"))
  write_parquet(query_result, glue(parquet_output_path, "/{table}.parquet"))
}
