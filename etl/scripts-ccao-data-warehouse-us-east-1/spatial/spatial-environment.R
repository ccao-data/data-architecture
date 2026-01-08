library(arrow)
library(aws.s3)
library(dplyr)
library(geoarrow)
library(here)
library(purrr)
library(readr)
library(sf)
library(stringr)
library(tidyr)
source("utils.R")

# This script cleans environmental data such as floodplains and coastal
# boundaries and uploads them to S3 as parquet files
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
input_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "environment")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "environment")
current_year <- strftime(Sys.Date(), "%Y")


##### LAKE MICHICAN COASTLINE #####
coastline_years <- parse_number(
  get_bucket_df(input_bucket, prefix = "spatial/environment/coastline/")$Key
)
walk(coastline_years, function(x) {
  remote_file_coastline_raw <- file.path(
    input_bucket, "coastline", paste0(x, ".geojson")
  )
  remote_file_coastline_warehouse <- file.path(
    output_bucket, "coastline", paste0("year=", x), "part-0.parquet"
  )

  if (!aws.s3::object_exists(remote_file_coastline_warehouse)) {
    tmp_file_coastline <- tempfile(fileext = ".geojson")
    aws.s3::save_object(remote_file_coastline_raw, file = tmp_file_coastline)

    # We need to clip the coastlines to only include Cook County
    if (!exists("cook_boundary")) {
      cook_boundary <<- read_geoparquet_sf(
        file.path(
          AWS_S3_WAREHOUSE_BUCKET,
          "spatial/ccao/county/2019.parquet"
        )
      ) %>%
        st_transform(4326) %>%
        st_buffer(1)
    }

    st_read(tmp_file_coastline) %>%
      st_transform(4326) %>%
      filter(as.logical(st_intersects(geometry, cook_boundary))) %>%
      # 2015 has superfluous coastline artifacts we need to remove
      filter(ifelse(x == 2015, row_number() %in% 1:3, TRUE)) %>%
      mutate(
        NAME = "Lake Michigan",
        geometry_3435 = st_transform(geometry, 3435)
      ) %>%
      rename_with(tolower) %>%
      geoparquet_to_s3(remote_file_coastline_warehouse)
  }
})


##### FEMA FLOODPLAINS #####
fema_years <- parse_number(
  get_bucket_df(input_bucket, prefix = "spatial/environment/flood_fema/")$Key
)


# Write FEMA floodplains to S3 if they don't exist
for (year in fema_years) {
  flood_fema_raw <- file.path(
    input_bucket, "flood_fema", paste0(year, ".geojson")
  )
  flood_fema_warehouse <- file.path(
    output_bucket, "flood_fema", paste0("year=", year), "part-0.parquet"
  )

  if (
    aws.s3::object_exists(flood_fema_raw) &&
      !aws.s3::object_exists(flood_fema_warehouse)
  ) {
    tmp_file <- tempfile(fileext = ".geojson")
    aws.s3::save_object(flood_fema_raw, file = tmp_file)

    # All we need to do is recode the special flood hazard area column
    # And remove superfluous columns
    st_read(tmp_file) %>%
      st_transform(4326) %>%
      mutate(
        SFHA_TF = as.logical(SFHA_TF),
        geometry_3435 = st_transform(geometry, 3435)
      ) %>%
      select(
        fema_special_flood_hazard_area = SFHA_TF,
        geometry, geometry_3435
      ) %>%
      geoparquet_to_s3(flood_fema_warehouse)
    file.remove(tmp_file)
  }
}


##### RAILROAD #####
remote_file_rail_raw <- file.path(
  input_bucket, "railroad", "2021.geojson"
)
remote_file_rail_warehouse <- file.path(
  output_bucket, "railroad", "year=2021", "part-0.parquet"
)

if (!aws.s3::object_exists(remote_file_rail_warehouse)) {
  tmp_file_rail <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file_rail_raw, file = tmp_file_rail)

  st_read(tmp_file_rail) %>%
    st_transform(4326) %>%
    rename_with(tolower) %>%
    group_by(name_id, anno_name) %>%
    summarise() %>%
    select(name_id, name_anno = anno_name, geometry) %>%
    mutate(
      geometry_3435 = st_transform(geometry, 3435)
    ) %>%
    geoparquet_to_s3(remote_file_rail_warehouse)
}


##### HYDROLOGY #####
raw_files_hydro <- grep(
  "geojson",
  file.path(
    AWS_S3_RAW_BUCKET,
    get_bucket_df(
      AWS_S3_RAW_BUCKET,
      prefix = "spatial/environment/hydrology/"
    )$Key
  ),
  value = TRUE
)
dest_files_hydro_prefix <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "spatial/environment/hydrology/"
)
dest_files_hydro_years <- raw_files_hydro %>%
  str_extract("[0-9]{4}") %>%
  unique()
dest_files_hydro <- file.path(
  dest_files_hydro_prefix,
  paste0("year=", dest_files_hydro_years),
  "part-0.parquet"
)

# Function to pull raw data from S3 and clean
walk(2011:current_year, function(year) {
  remote_files_hydro_raw <- file.path(
    input_bucket, "hydrology", c("area", "linear"), paste0(year, ".geojson")
  )
  remote_file_hydro_warehouse <- file.path(
    output_bucket, "hydrology", paste0("year=", year), "part-0.parquet"
  )

  if (!aws.s3::object_exists(remote_file_hydro_warehouse)) {
    print(remote_file_hydro_warehouse)
    tmp_file <- tempfile(c(fileext = ".geojson", fileext = ".geojson"))
    mapply(aws.s3::save_object, remote_files_hydro_raw, file = tmp_file)

    names(tmp_file) <- str_extract(remote_files_hydro_raw, "linear|area")

    rbind(
      st_read(tmp_file["linear"]) %>%
        mutate(hydrology_type = "linear") %>%
        select(id = LINEARID, name = FULLNAME, hydrology_type, geometry),
      st_read(tmp_file["area"]) %>%
        mutate(hydrology_type = "area") %>%
        select(id = HYDROID, name = FULLNAME, hydrology_type, geometry)
    ) %>%
      mutate(geometry_3435 = st_transform(geometry, 3435)) %>%
      geoparquet_to_s3(remote_file_hydro_warehouse)

    file.remove(tmp_file)
  }
})
