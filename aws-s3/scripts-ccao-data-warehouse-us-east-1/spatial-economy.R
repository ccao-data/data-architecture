library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(sfarrow)
library(stringr)
library(tidyr)
source("utils.R")

# This scripts cleans and uploads boundaries for various economic incentive
# zones
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "economy")

# Location of file to clean
raw_files <- grep(
  "geojson",
  file.path(
    AWS_S3_RAW_BUCKET,
    get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "spatial/economy/")$Key
  ),
  value = TRUE
)

# Function to clean consolidated care districts
clean_consolidated_care <- function(shapefile, economic_unit) {

  if (economic_unit == "consolidated_care") {

    return(

      shapefile %>%
        st_transform(4326) %>%
        st_make_valid() %>%
        mutate(AGENCY_DES = str_replace(AGENCY_DES, "PROVISIO", "PROVISO")) %>%
        group_by(AGENCY_DES, MUNICIPALI) %>%
        summarise() %>%
        mutate(geometry_3435 = st_transform(geometry, 3435),
               centroid = st_centroid(st_transform(geometry, 3435))) %>%
        cbind(
          st_coordinates(st_transform(.$centroid, 4326)),
          st_coordinates(.$centroid)
        ) %>%
        group_by(AGENCY_DES) %>%
        mutate(cc_num = str_pad(cur_group_id(), width = 3, side = "left", pad = "0"),
               political_boundary = case_when(is.na(MUNICIPALI) ~ "Township",
                                              TRUE ~ "Municipality"),
               cc_name = str_squish(
                 str_to_title(
                   case_when(is.na(MUNICIPALI) ~ str_replace(AGENCY_DES, "TWP", ""),
                             TRUE ~ MUNICIPALI)
                   )
                 )) %>%
        select(cc_num, cc_name, political_boundary,
               lon = X, lat = Y, x_3435 = `X.1`, y_3435 = `Y.1`, geometry, geometry_3435) %>%
        ungroup()
    )

  } else {

    return(shapefile)

  }

}

# TODO: Finish economy script
remote_file <- raw_files[1]
tmp_file <- tempfile(fileext = ".geojson")
aws.s3::save_object(remote_file, file = tmp_file)

shapefile <- st_read(tmp_file)

file.remove(tmp_file)
