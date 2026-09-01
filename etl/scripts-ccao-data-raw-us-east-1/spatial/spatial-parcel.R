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
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial")

##### HISTORICAL PARCELS #####

# Read privileges for the this drive location are limited.
# Contact Cook County GIS if permissions need to be changed.
file_path <- "//gisemcv1.ccounty.com/ArchiveServices"

# Paths for all relevant geodatabases. Parcel shapes come from a layer of a GDB
# file created and maintained by Cook County GIS.
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
    dir_name = "parcel_test",
    file_path = df$path,
    layer = "Parcel"
  )
})


##### Attributes #####

# We grab a little high-level data from iasworld to join with spatial parcel
# data. Stuff like class, township, etc. that's nice to have.
# Connect to CCAODATA SQL server
iasworld_years <- unique(2000:format(Sys.Date(), "%Y"))
parcels_remote_attr <- file.path(
  output_bucket, "parcel_test",
  paste0(iasworld_years, "-attr.parquet")
)

# Query iasWorld via Athena to get attribute data we can pre-join
walk(parcels_remote_attr, function(x) {
  if (!aws.s3::object_exists(x)) {
    year <- str_sub(x, -17, -14)

    AWS_ATHENA_CONN_NOCTUA <- dbConnect(noctua::athena())

    dbGetQuery(
      AWS_ATHENA_CONN_NOCTUA, glue("
        SELECT par.parid AS pin,
        par.class,
        leg.taxdist AS tax_code,
        REGEXP_REPLACE(par.nbhd, '([^0-9])', '') AS nbhd_code,
        leg.user1 AS town_code,
        par.taxyr
      FROM iasworld.pardat AS par
        LEFT JOIN iasworld.legdat AS leg ON par.parid = leg.parid
        AND par.taxyr = leg.taxyr
        AND leg.cur = 'Y'
        AND leg.deactivat IS NULL
      WHERE par.cur = 'Y'
        AND par.deactivat IS NULL
        AND par.taxyr = '{year}'")
    ) %>%
      mutate(
        pin = str_pad(pin, 14, "left", "0"),
        tax_code = str_pad(tax_code, 5, "left", "0"),
        nbhd_code = str_sub(str_pad(nbhd_code, 5, "left", "0"), 3, 5),
        town_code = str_pad(town_code, 2, "left", "0"),
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
dbDisconnect(AWS_ATHENA_CONN_NOCTUA)
