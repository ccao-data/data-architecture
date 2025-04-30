library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(tigris)
source("utils.R")

# This script retrieves boundary files for all census geographies
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "census")
current_year <- strftime(Sys.Date(), "%Y")
min_year <- as.numeric(Sys.getenv("CENSUS_GEO_MIN_YEAR", unset = "2010"))
max_year <- as.numeric(Sys.getenv("CENSUS_GEO_MAX_YEAR", unset = current_year))
years <- min_year:max_year

# Mini-function to fetch geojson of geography if it doesn't exist
get_geojson <- function(FUN, year, dir, state = "17", ...) {
  remote_path <- file.path(output_bucket, dir, paste0(year, ".geojson"))

  if (!aws.s3::object_exists(remote_path)) {
    tmp_file <- tempfile(fileext = ".geojson")
    df <- FUN(state = state, year = year, ...)
    st_write(df, tmp_file, delete_dsn = TRUE)
    save_local_to_s3(remote_path, tmp_file)
    file.remove(tmp_file)
  }
}


##### BLOCK #####
walk(years, ~ get_geojson(blocks, .x, "block", county = "031"))

##### BLOCK GROUP #####
walk(years, ~ get_geojson(block_groups, .x, "block_group"))

##### CONGRESSIONAL DISTRICT #####
# Geographies for con dist only go back to 2011, see ?congressional_districts
congress <- "116" # workaround for 2021 CD file missing congress number
walk(
  2011:max_year,
  ~ get_geojson(congressional_districts, .x, "congressional_district")
)

##### COUNTY #####
walk(years, ~ get_geojson(counties, .x, "county"))

##### COUNTY SUBDIVISION #####
walk(years, ~ get_geojson(county_subdivisions, .x, "county_subdivision"))

##### PLACE #####
walk(2011:max_year, ~ get_geojson(places, .x, "place", state = "IL"))

##### PUBLIC USE MICRODATA AREA (PUMA) #####
walk(2012:max_year, ~ get_geojson(pumas, .x, "puma"))

##### SCHOOL DISTRICTS #####
school_dists <- expand.grid(
  years = 2011:max_year,
  type = c("elementary", "secondary", "unified")
)
walk2(
  school_dists$years, school_dists$type,
  ~ get_geojson(school_districts, .x, paste0("school_district_", .y), type = .y)
)

##### STATE REPRESENTATIVE #####
walk(
  2011:max_year,
  ~ get_geojson(
    state_legislative_districts, .x, "state_representative",
    house = "lower"
  )
)

##### STATE SENATE #####
walk(
  2011:max_year,
  ~ get_geojson(
    state_legislative_districts, .x, "state_senate",
    house = "upper"
  )
)

##### TRACT #####
walk(years, ~ get_geojson(tracts, .x, "tract", county = "031"))

##### ZCTA #####
walk(
  c(2010, 2012:max_year),
  ~ get_geojson(
    zctas, .x, "zcta",
    starts_with = c("60", "61", "62"), state = NULL
  )
)
