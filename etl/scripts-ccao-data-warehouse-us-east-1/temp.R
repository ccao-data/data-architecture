library(dplyr)
library(purrr)
source("utils.R")
temp <- aws.s3::get_bucket_df("s3://ccao-data-warehouse-us-east-1/", prefix = "spatial/transit") %>%
  filter(
    stringr::str_detect(Key, "parquet")
  ) %>%
  pull(Key)

walk(file.path("s3://ccao-data-warehouse-us-east-1", temp), \(x) {
  print(x)
  if (x == "s3://ccao-data-warehouse-us-east-1/spatial/transit/transit_dict/transit_dict.parquet") {
    read_parquet(x) %>%
      mutate(loaded_at = as.character(Sys.time())) %>%
      write_parquet(x)
  } else {
    geoarrow::read_geoparquet_sf(x) %>%
      geoparquet_to_s3(x)
  }
}, .progress = TRUE)


library(DBI)
library(glue)
library(noctua)

AWS_ATHENA_CONN_NOCTUA <- dbConnect(
  noctua::athena(),
  # Disable the Connections tab entry for this database. Always use this if
  # you don't want to browser the tables in the Connections tab, since it
  # speeds up instantiating the connection significantly
  rstudio_conn_tab = FALSE
)

tables <- c(
  "bike_trail",
  "board_of_review_district",
  "building_footprint",
  "cemetery",
  "census",
  "central_business_district",
  "coastline",
  "commissioner_district",
  "community_area",
  "community_college_district",
  "congressional_district",
  "coordinated_care",
  "corner",
  "county",
  "enterprise_zone",
  "fire_protection_district",
  "flood_fema",
  #"geojson",
  "golf_course",
  "grocery_store",
  "hospital",
  "hydrology",
  "industrial_corridor",
  "industrial_growth_zone",
  "judicial_district",
  "library_district",
  "major_road",
  "midway_noise_monitor",
  "municipality",
  "neighborhood",
  "ohare_noise_contour",
  "ohare_noise_monitor",
  #"parcel",
  "park",
  "park_district",
  "police_district",
  "qualified_opportunity_zone",
  "railroad",
  "road",
  "sanitation_district",
  "school_district",
  "school_location",
  "secondary_road",
  "special_service_area",
  "stadium",
  #"stadium_raw",
  "state_representative_district",
  "state_senate_district",
  "subdivision",
  "tif_district",
  "township",
  "transit_dict",
  "transit_route",
  "transit_stop",
  "walkability",
  "ward",
  "ward_chicago",
  "ward_evanston"
)

walk(tables, \(x) {
  print(x)
  dbGetQuery(
    conn = AWS_ATHENA_CONN_NOCTUA,
    glue("SELECT loaded_at FROM spatial.{x}")
  )
})
