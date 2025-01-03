library(aws.s3)
library(arrow)
library(dplyr)
library(purrr)
library(sf)
library(geoarrow)
library(stringr)
source("utils.R")

# This script cleans saved census boundary files and moves the cleaned data
# into the main warehouse
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

# Get a list of all census geo objects in S3
census_geo_objs <- aws.s3::get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET,
  prefix = "spatial/census"
) %>%
  arrange(as.numeric(Size))

# Function to load file from S3, then normalize and output sf dataframe
# Will set geometry and centroid to 4326 and normalize column names
normalize_census_geo <- function(key) {
  # Only run if object doesn't already exist
  key_parts <- str_split(key, "/", simplify = TRUE)
  geography <- key_parts[3]
  year <- str_match(key, "[0-9]{4}")
  remote_file <- file.path(
    AWS_S3_WAREHOUSE_BUCKET, "spatial", "census",
    paste0("geography=", geography),
    paste0("year=", year),
    paste(geography, paste0(year, ".parquet"), sep = "-")
  )

  if (!aws.s3::object_exists(remote_file)) {
    message("Now fetching: ", key, "saving to: ", remote_file)
    tmp_file <- tempfile(fileext = ".geojson")
    aws.s3::save_object(key, AWS_S3_RAW_BUCKET, file = tmp_file)
    df <- sf::read_sf(tmp_file) %>%
      select(starts_with(
        c("GEOID", "NAME", "INTPT", "ALAND", "AWATER"),
        ignore.case = TRUE
      )) %>%
      set_names(str_remove_all(names(.), "[[:digit:]]")) %>%
      st_transform(4326) %>%
      st_cast("MULTIPOLYGON")

    df <- df %>%
      st_drop_geometry() %>%
      st_as_sf(coords = c("INTPTLON", "INTPTLAT"), crs = st_crs(df)) %>%
      cbind(st_coordinates(.), st_coordinates(st_transform(., 3435))) %>%
      st_drop_geometry() %>%
      select(lon = X, lat = Y, x_3435 = `X.1`, y_3435 = `Y.1`) %>%
      cbind(df, .) %>%
      # If dataframe has exactly one name column, rename it
      {
        if (sum(grepl("NAME", names(.))) == 1) {
          rename_with(., ~ paste0("NAME"), starts_with("NAME"))
        } else {
          .
        }
      } %>%
      select(
        -starts_with("INTPT", ignore.case = TRUE),
        -ends_with("LSAD", ignore.case = TRUE)
      ) %>%
      rename_with(tolower) %>%
      mutate(
        geometry_3435 = st_transform(geometry, 3435),
        geoid =
          if (key == "spatial/census/congressional_district/2011.geojson") {
            str_remove(geoid, "112")
          } else {
            geoid
          }
      ) %>%
      select(
        everything(),
        lon, lat, x_3435, y_3435,
        geometry, geometry_3435
      ) %>%
      filter(!str_detect(geoid, "Z")) %>%
      geoparquet_to_s3(remote_file)
  }
}

# Map through and normalize all geographies
walk(census_geo_objs$Key, normalize_census_geo)
