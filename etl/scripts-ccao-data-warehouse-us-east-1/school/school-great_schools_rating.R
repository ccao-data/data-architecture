library(aws.s3)
library(arrow)
library(dplyr)
library(geoarrow)
library(glue)
library(noctua)
library(purrr)
library(sf)
library(stringr)
source("utils.R")

# This script cleans data retrieved from greatschools.org and merges
# it with district shapefiles. In order to average school ratings by district
# in the suburbs and attendance areas in Chicago
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
AWS_ATHENA_CONN_NOCTUA <- DBI::dbConnect(noctua::athena())

# Get a list of great schools source files and years
source_files <- grep(
  "parquet",
  file.path(
    AWS_S3_RAW_BUCKET,
    get_bucket_df(
      AWS_S3_RAW_BUCKET,
      prefix = "school/great_schools_rating/"
    )$Key
  ),
  value = TRUE
)

destination_folder <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "school",
  "great_schools_rating"
)

clean_great_schools_rating <- function(file) {
  # First dataset is each school, matched with district names and district types
  # Pull from S3, convert to spatial object with lat/long, 4326 CRS
  great_schools <- read_parquet(file) %>%
    # A few schools have lat/long that will place them outside their
    # attendance boundary and need to be manually moved
    rowwise() %>%
    mutate(
      lat = case_when(
        name == "Tarkington Elementary School" ~ 41.7642,
        TRUE ~ lat
      ),
      lon = case_when(
        name == "Prieto Math-Science Elementary School" ~ -87.766,
        TRUE ~ lon
      ),
      complete_case = sum(
        as.numeric(!is.na(`universal-id`)) * 3,
        as.numeric(!is.na(`nces-id`)) * 2,
        as.numeric(!is.na(`state-id`)) * 1
      )
    ) %>%
    # Some duplicate rows appear in the raw data. We want to keep only the row
    # per school with the most complete data
    group_by(name, phone, lat, lon) %>%
    slice_max(order_by = complete_case, n = 1) %>%
    ungroup() %>%
    sf::st_as_sf(coords = c("lon", "lat"), remove = FALSE, crs = 4326) %>%
    # We"ll need to know which types of districts (elementary, secondary) each
    # school belongs to based on what grades they service, since schools will
    # be spatially joined to districts by type
    mutate(
      county = "Cook",
      `district-id` = na_if(`district-id`, 0),
      grades = case_when(
        grepl("h", `level-codes`) ~ "secondary",
        TRUE ~ "elementary"
      ),
      rating = as.numeric(rating)
    ) %>%
    # Clean up some column names and reorder
    dplyr::rename(school_name = name) %>%
    rename_with(~ gsub("-", "_", .x)) %>%
    select(-any_of(c("district_name")))

  # Retrieve district boundaries from S3. Boundary years should match the file
  # year of the great schools data
  district_boundaries <- st_as_sf(
    dbGetQuery(
      AWS_ATHENA_CONN_NOCTUA, glue_sql(
        "SELECT
             geoid,
             name AS district_name,
             is_attendance_boundary,
             geometry,
             district_type
         FROM spatial.school_district
         WHERE year = '{unique(great_schools$rating_year)*}';",
        .con = AWS_ATHENA_CONN_NOCTUA
      )
    ),
    crs = 4326
  )

  # Join schools to districts - this is a 1 to many join but will be de-duped
  # using district type
  great_schools_w_district <- great_schools %>%
    # We only want to join district information to public schools
    filter(type == "public") %>%
    st_join(district_boundaries, join = st_within) %>%
    # Here is where we de-dupe schools matched to overlapping elementary
    # and secondary districts
    filter(
      grades == district_type |
        (district_type == "unified" & city != "Chicago")
    ) %>%
    # Bind private and charter schools back on
    bind_rows(
      filter(great_schools, type %in% c("private", "charter"))
    ) %>%
    # Convert geometry column to 3435
    mutate(
      geometry_3435 = st_transform(geometry, 3435),
      year = as.character(rating_year)
    ) %>%
    # Keep only needed columns
    select(
      universal_id, nces_id, state_id,
      school_name, school_summary, rating, phone, web_site,
      type, level, level_codes,
      street, city, state, zip, lat, lon,
      district_geoid = geoid, district_name, district_type,
      is_attendance_boundary, geometry, geometry_3435, year
    )
}

# Run function over all GS input files
lapply(source_files, clean_great_schools_rating) %>%
  bind_rows() %>%
  group_by(year) %>%
  write_partitions_to_s3(
    s3_output_path = destination_folder,
    is_spatial = TRUE,
    overwrite = FALSE
  )
