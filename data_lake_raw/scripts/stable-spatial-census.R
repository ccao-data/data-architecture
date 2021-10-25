library(dplyr)
library(here)
library(purrr)
library(sf)
library(tigris)

# Get min and max year for retrieved data
cur_year <- format(Sys.Date(), "%Y")
min_year <- as.numeric(Sys.getenv("CENSUS_GEO_MIN_YEAR", unset = "2010"))
max_year <- as.numeric(Sys.getenv("CENSUS_GEO_MAX_YEAR", unset = cur_year))
years <- min_year:max_year

# Mini-function to fetch geojson of geography if it doesn't exist
get_geojson <- function(FUN, year, dir, state = "17", ...) {
  geo_path <- here("s3-bucket", "stable", "spatial", "census")
  path <- here(geo_path, dir, paste0(year, ".geojson"))

  if (!file.exists(paste0(path, ".gz"))) {
    df <- FUN(state = state, year = year, ...)
    st_write(df, path, delete_dsn = TRUE)

    # compress geojson
    gzip(path, destname = paste0(path, ".gz"))
  }
}


# BLOCK
map(years, ~ get_geojson(blocks, .x, "block", county = "031"))

# BLOCK GROUP
map(years, ~ get_geojson(block_groups, .x, "block_group"))

# CONGRESSIONAL DISTRICT
# Geographies for con dist only go back to 2011, see ?congressional_districts
map(
  2011:max_year,
  ~ get_geojson(congressional_districts, .x, "congressional_district")
)

# tigris is bugged for 2020 congress, download manually
tmp_file <- tempfile(fileext = ".zip")
tmp_dir <- tempdir()
download.file(
  "https://www2.census.gov/geo/tiger/TIGER2020/CD/tl_2020_us_cd116.zip",
  destfile = tmp_file
)

if (!file.exists(
  file.path(
    here("s3-bucket", "stable", "spatial", "census"),
    "congressional_district", "2020.geojson.gz"
    )
  )) {

  unzip(tmp_file, exdir = tmp_dir)
  st_read(file.path(tmp_dir, "tl_2020_us_cd116.shp")) %>%
    filter(STATEFP == "17") %>%
    st_write(
      file.path(
        here("s3-bucket", "stable", "spatial", "census"),
        "congressional_district", "2020.geojson"
      )
    )

  # compress geojson
  gzip(file.path(
    here("s3-bucket", "stable", "spatial", "census"),
    "congressional_district", "2020.geojson"
  ),
  destname = file.path(
    here("s3-bucket", "stable", "spatial", "census"),
    "congressional_district", "2020.geojson.gz"
  ))

}

# COUNTY
map(years, ~ get_geojson(counties, .x, "county"))

# COUNTY SUBDIVISION
map(years, ~ get_geojson(county_subdivisions, .x, "county_subdivision"))

# PUBLIC USE MICRODATA AREA (PUMA)
map(2012:max_year, ~ get_geojson(pumas, .x, "puma"))

# SCHOOL DISTRICTS
school_dists <- expand.grid(
  years = 2011:max_year,
  type = c("elementary", "secondary", "unified")
)
map2(
  school_dists$years, school_dists$type,
  ~ get_geojson(school_districts, .x, paste0("school_district_", .y), type = .y)
)

# STATE REPRESENTATIVE
map(
  2011:max_year,
  ~ get_geojson(
    state_legislative_districts, .x, "state_representative", house = "lower"
  )
)

# STATE SENATE
map(
  2011:max_year,
  ~ get_geojson(
    state_legislative_districts, .x, "state_senate", house = "upper"
  )
)

# TRACT
map(years, ~ get_geojson(tracts, .x, "tract", county = "031"))

# ZCTA
map(
  c(2010, 2012:max_year),
  ~ get_geojson(zctas, .x, "zcta", starts_with = c("60", "61", "62"), state = NULL)
)
