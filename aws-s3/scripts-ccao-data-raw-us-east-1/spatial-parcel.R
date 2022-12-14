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
sources_list <- c(
  "_2000",
  "_2001",
  "_2002",
  "_2003",
  "_2004",
  "_2005",
  "_2006",
  "_2007",
  "_2008",
  "_2009",
  "_2010",
  "_2011",
  "_2012",
  "_2013",
  "_2014",
  "_2015",
  "_2016",
  "_2017",
  "_2018",
  "_2019",
  "_2020",
  "2021_enhancedAll"
)

sources_list <- bind_rows(list(
  "api_url" = paste0(
    sources_list, "/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
    ),
  "year" = parse_number(sources_list)
))

# Function to call referenced API, pull requested data, and write it to S3
pwalk(sources_list, function(...) {
  df <- tibble::tibble(...)
  open_data_to_s3(
    s3_bucket_uri = output_bucket,
    base_url = "https://gis.cookcountyil.gov/hosting/rest/services/Hosted/Parcel",
    data_url = df$api_url,
    dir_name = "parcel",
    file_year = df$year,
    file_ext = ".geojson"
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
    output_bucket, "parcel",
    paste0(year, "-attr.parquet")
  )

  if (!aws.s3::object_exists(remote_file_attr)) {
    message("Now grabbing year: ", year)
    tmp_file <- tempfile(fileext = ".parquet")
    df <- DBI::dbGetQuery(
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


##### CURRENT PARCELS #####
current_year <- format(Sys.Date(), "%Y")
parcels_current <- glue(
  "O:/CCAODATA/data/spatial/{current_year}",
  "parcel/clerkParcel{current_year}.shp"
)
parcels_current_tmp_geo <- tempfile(fileext = ".geojson")
parcels_current_tmp_attr <- tempfile(fileext = ".parquet")
parcels_current_remote_geo <- file.path(
  output_bucket, "parcel",
  paste0(current_year, ".geojson")
)
parcels_current_remote_attr <- file.path(
  output_bucket, "parcel",
  paste0(current_year, "-attr.parquet")
)

# Upload boundary file as geojson if it doesn't exist
if (!aws.s3::object_exists(parcels_current_remote_geo)) {
  st_read(parcels_current) %>%
    st_write(parcels_current_tmp_geo)

  save_local_to_s3(parcels_current_remote_geo, parcels_current_tmp_geo)
  file.remove(parcels_current_tmp_geo)
}

# Query iasWorld via Athena to get attribute data we can pre-join
if (!aws.s3::object_exists(parcels_current_remote_attr)) {
  AWS_ATHENA_CONN_NOCTUA <- dbConnect(noctua::athena())

  parcels_current_attr <- dbGetQuery(
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
        WHERE p.taxyr = '{current_year}'")
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
    write_parquet(parcels_current_tmp_attr)

  save_local_to_s3(parcels_current_remote_attr, parcels_current_tmp_attr)
  file.remove(parcels_current_tmp_attr)
}

# Cleanup
dbDisconnect(CCAODATA)
dbDisconnect(AWS_ATHENA_CONN_NOCTUA)
