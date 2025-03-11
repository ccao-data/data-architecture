library(arrow)
library(aws.s3)
library(DBI)
library(dplyr)
library(glue)
library(noctua)
library(odbc)
library(purrr)
library(readr)
library(sf)
library(stringr)
source("utils.R")

# This script retrieves the historical parcel files from Cook Central
# and saves them as geojson on S3
# It also cleans the most recent parcel file (before it's publicly
# available) and adds some attribute data)
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial_raw")

##### HISTORICAL PARCELS #####

# Read privileges for the this drive location are limited.
# Contact Cook County GIS if permissions need to be changed.
file_path <- "//10.122.19.14/ArchiveServices"

# Paths for all relevant geodatabases
gdb_files <- data.frame("path" = list.files(file_path, full.names = TRUE)) %>%
  filter(
    str_detect(path, "Current", negate = TRUE) &
      str_detect(path, "20") &
      str_detect(path, "Parcels")
  )

# Function to call referenced GDBs, pull requested data, and write it to S3
pwalk(gdb_files, function(...) {
  df <- tibble::tibble(...)
  county_gdb_to_s3(
    s3_bucket_uri = output_bucket,
    dir_name = "parcel",
    file_path = df$path,
    layer = "Parcel"
  )
})


##### Attributes #####
# Connect to CCAODATA SQL server
CCAODATA <- odbc::dbConnect(
  odbc::odbc(),
  .connection_string = Sys.getenv("DB_CONFIG_CCAODATA")
)

# Function to download parcel-level attribute data from old (deprecated) SQL
# Server. Useful for historical PINs for which data is hard-to-find
pull_sql_and_write <- function(year) {
  remote_file_attr <- file.path(
    output_bucket, "parcel_raw",
    paste0(year, "-attr.parquet")
  )

  if (!aws.s3::object_exists(remote_file_attr)) {
    message("Now grabbing year: ", year)
    tmp_file <- tempfile(fileext = ".parquet")
    DBI::dbGetQuery(
      CCAODATA, glue("
      SELECT
        PIN AS pin,
        HD_CLASS AS class,
        HD_TOWN AS tax_code,
        HD_NBHD AS nbhd_code,
        LEFT(HD_TOWN, 2) AS town_code,
        TAX_YEAR AS taxyr
      FROM AS_HEADT
      WHERE TAX_YEAR = {year}")
    ) %>%
      mutate(
        pin = str_pad(pin, 14, "left", "0"),
        tax_code = str_pad(tax_code, 5, "left", "0"),
        nbhd_code = str_pad(nbhd_code, 3, "left", "0"),
        town_code = str_pad(town_code, 2, "left", "0")
      ) %>%
      distinct(pin, .keep_all = TRUE) %>%
      write_parquet(tmp_file)

    save_local_to_s3(remote_file_attr, tmp_file)
    file.remove(tmp_file)
  }
}

walk(2000:2020, pull_sql_and_write)


##### MODERN PARCELS #####
iasworld_years <- unique(2021:format(Sys.Date(), "%Y"))
parcels_current_remote_attr <- file.path(
  output_bucket, "parcel_raw",
  paste0(iasworld_years, "-attr.parquet")
)

# Query iasWorld via Athena to get attribute data we can pre-join
walk(parcels_current_remote_attr, function(x) {
  if (!aws.s3::object_exists(x)) {
    year <- str_sub(x, -17, -14)

    AWS_ATHENA_CONN_NOCTUA <- dbConnect(noctua::athena())

    dbGetQuery(
      AWS_ATHENA_CONN_NOCTUA, glue("
        SELECT
          p.parid AS pin,
          p.class AS class,
          l.taxdist AS tax_code,
          p.nbhd AS nbhd_code,
          p.taxyr AS taxyr
        FROM iasworld.pardat p
        LEFT JOIN iasworld.legdat l
        ON p.parid = l.parid AND p.taxyr = l.taxyr
        WHERE p.taxyr = '{year}'")
    ) %>%
      mutate(
        pin = str_pad(pin, 14, "left", "0"),
        tax_code = str_pad(tax_code, 5, "left", "0"),
        nbhd_code = str_sub(str_pad(nbhd_code, 5, "left", "0"), 3, 5),
        town_code = str_sub(tax_code, 1, 2),
        taxyr = as.integer(taxyr)
      ) %>%
      distinct(pin, .keep_all = TRUE) %>%
      arrange(pin) %>%
      select(pin, class, tax_code, nbhd_code, town_code, taxyr) %>%
      as.data.frame() %>%
      write_parquet(x)
  }
})


# Cleanup
dbDisconnect(CCAODATA)
dbDisconnect(AWS_ATHENA_CONN_NOCTUA)
