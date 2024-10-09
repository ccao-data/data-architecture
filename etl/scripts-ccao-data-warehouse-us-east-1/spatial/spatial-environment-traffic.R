library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(geoarrow)

# Define the S3 bucket and folder path
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
s3_folder <- "spatial/environment/traffic/"
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, s3_folder)

# Get the 'Key'
parquet_files <- get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET, prefix = s3_folder
  ) %>%
  pull(Key)

# Loop through each parquet file and process it
walk(parquet_files, \(file_key) {

  if (!aws.s3::object_exists(file.path(AWS_S3_WAREHOUSE_BUCKET, s3_folder, file_key))) {

    print(paste("Cleaning", file_key))

    # Convert the S3 object into raw data and read using geoarrow
    shapefile_data <- geoarrow::read_geoparquet_sf(
      file.path(AWS_S3_RAW_BUCKET, file_key)
    ) %>%
      st_transform(4326) %>%
      mutate(geometry_3435 = st_transform(geometry, 3435))

      # Convert the S3 object into raw data and read using geoarrow
      shapefile_data <- geoarrow::read_geoparquet_sf(
        file.path(AWS_S3_RAW_BUCKET, file_key)
      ) %>%
        st_transform(4326) %>%
        mutate(geometry_3435 = st_transform(geometry, 3435))


        # We do this because some columns are not present in
        # older versions of the data
        required_columns <- c("FCNAME", "FC_NAME", "LNS", "SURF_TYP", "SURF_WTH", "SRF_YR", "AADT",
                              "CRS_WITH", "CRS_OPP", "CRS_YR",
                              "ROAD_NAME", "DTRESS_WTH", "DTRESS_OPP",
                              "SP_LIM", "INVENTORY", "geometry_3435", "year")

      # Select only the non-geometry columns that exist in the dataset
      existing_columns <- intersect(required_columns, colnames(shapefile_data))
      shapefile_data %>%
        select(all_of(existing_columns)) %>%
        mutate(
          road_type = if ("FCNAME" %in% colnames(.)) FCNAME else if ("FC_NAME" %in% colnames(.)) FC_NAME else NA,
          lanes = if ("LNS" %in% colnames(.)) LNS else NA,
          surface_type = if ("SURF_TYP" %in% colnames(.)) SURF_TYP else NA,
          surface_width = if ("SURF_WTH" %in% colnames(.)) SURF_WTH else NA,
          surface_year = if ("SRF_YR" %in% colnames(.)) SRF_YR else NA,
          annual_traffic = if ("AADT" %in% colnames(.)) AADT else NA,
          condition_with = if ("CRS_WITH" %in% colnames(.)) CRS_WITH else NA,
          condition_opposing = if ("CRS_OPP" %in% colnames(.)) CRS_OPP else NA,
          condition_year = if ("CRS_YR" %in% colnames(.)) CRS_YR else NA,
          road_name = if ("ROAD_NAME" %in% colnames(.)) ROAD_NAME else NA,
          distress_with = if ("DTRESS_WTH" %in% colnames(.)) DTRESS_WTH else NA,
          distress_opposing = if ("DTRESS_OPP" %in% colnames(.)) DTRESS_OPP else NA,
          speed_limit = if ("SP_LIM" %in% colnames(.)) SP_LIM else NA,
          inventory_id = if ("INVENTORY" %in% colnames(.)) INVENTORY else NA
        ) %>%
        select(-one_of(c("FCNAME", "FC_NAME", "LNS", "SURF_TYP", "SURF_WTH", "SRF_YR", "AADT", "CRS_WITH",
                         "CRS_OPP", "CRS_YR", "ROAD_NAME", "DTRESS_WTH", "DTRESS_OPP",
                         "SP_LIM", "INVENTORY"))) %>%
        geoarrow::write_geoparquet(
          file.path(AWS_S3_WAREHOUSE_BUCKET, file_key)
        )

    print(paste(file_key, "cleaned and uploaded."))

    }
  }

})
