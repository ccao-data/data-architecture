library(sfarrow)
library(dplyr)
library(aws.s3)
library(arrow)
library(noctua)
library(glue)
library(sf)

# This script cleans data retrieved from greatschools.org and merges it with district shapefiles
# In order to average school ratings by district in the suburbs and attendance areas in Chicago
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
AWS_ATHENA_CONN <- DBI::dbConnect(noctua::athena())

source_file <- file.path(
  AWS_S3_RAW_BUCKET,
  "school",
  "great_schools",
  paste0(format(Sys.Date(), "%Y"), ".parquet")
)

destination_folder <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "school",
  "great_schools"
)

# Read, write data if it does not already exist
if (!aws.s3::object_exists(
  file.path(
    destination_folder,
    "school_level",
    paste0(format(Sys.Date(), "%Y"), ".parquet")
  )
)) {

  # First dataset is each school matched with our district names and district types

  # Pull from S3, convert to spatial object with lat/long, 4326 CRS
  great_districts <- read_parquet(source_file) %>%
    sf::st_as_sf(coords = c("lon", "lat"), remove = FALSE, crs = 4326) %>%

    # We'll need to know which types of districts (elementary, secondary) each school belongs to
    # Based on what grades they service since schools will be spatially joined to districts they're not actually part
    mutate(
      county = "Cook",
      `district-id` = na_if(`district-id`, 0),
      grades = case_when(grepl("h", `level-codes`) ~ 'secondary',
                         TRUE ~ 'elementary'
      ),
      rating = as.numeric(rating)
    ) %>%

    # Clean out unneeded columns
    select(-c('universal-id', 'nces-id', 'state-id', 'district-id', 'district-name',
              'web-site', 'phone', 'overview-url', 'rating-description', 'fax')) %>%

    # Clean up some column names
    dplyr::rename(school_name = name) %>%
    rename_with(~ gsub("-", "_", .x))

  # Retrieve district boundaries from S3
  district_boundaries <- st_as_sf(
    dbGetQuery(
      # Use district shapefiles from one year post-Great Schools data
      # Since they describes districts 1 year in the past
      AWS_ATHENA_CONN, glue(
        "SELECT geoid, name AS district_name, is_attendance_boundary, geometry, district_type
      FROM spatial.school_district
      WHERE year IN ('{paste(unique(great_districts$year) + 1, collapse = \"', '\")}');"
      )
    ),
    crs = 4326
  )

  # Join schools to districts - this is a 1 to many join but will be de-duped using district type
  great_districts <- great_districts %>%

    # We only want to join district information to public schools
    filter(type == 'public') %>%
    st_join(district_boundaries) %>%

    # Here is where we de-dupe schools matched to overlapping elementary and secondary districts
    filter(grades == district_type | (district_type == 'unified' & city != 'Chicago')) %>%

    # Bind private schools back on
    bind_rows(
      great_districts %>%
        filter(type %in% c('private', 'charter'))
      ) %>%

    # Add 3435 CRS column
    mutate(geometry_3435 = st_transform(geometry, 3435)) %>%
    distinct()

  # Write to S3
  sfarrow::st_write_parquet(great_districts,
                            file.path(
                              destination_folder,
                              "gs_school_rating",
                              paste0(format(Sys.Date(), "%Y"), ".parquet")
                            )
  )

  # Second dataset is average school rating by district

  great_districts %>%

    # Private school attendance isn't based on attendance/districts and thus isn't a discreet geographic correlate
    filter(type == 'public' & !is.na(rating)) %>%

    # Drop geometry because summarize by group messes it up
    st_drop_geometry() %>%
    group_by(district_name, geoid, district_type, year) %>%
    summarise(mean_rating = mean(rating, na.rm = TRUE),
              n_schools = n()) %>%

    # Rejoin geometry
    left_join(
      district_boundaries %>%
        select(district_name, geometry)
    ) %>%

    # Make sure observations are unique, CRS is correct
    distinct() %>%
    st_as_sf(crs = 4326) %>%

    # Add 3435 CRS column
    mutate(geometry_3435 = st_transform(geometry, 3435)) %>%

    # Write to S3
    sfarrow::st_write_parquet(
      file.path(
        destination_folder,
        "gs_district_rating",
        paste0(format(Sys.Date(), "%Y"), ".parquet")
      )
    )

}
