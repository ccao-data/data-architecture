library(aws.s3)
library(dplyr)
library(sf)
library(sfarrow)

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
input_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "environment")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "environment")
current_year <- strftime(Sys.Date(), "%Y")

remote_file_golf_course_raw <- file.path(
  input_bucket, "golf_course", "2022.geojson"
)
remote_file_golf_course_warehouse <- file.path(
  output_bucket, "golf_course", "year=2022", "part-0.parquet"
)

if (!aws.s3::object_exists(remote_file_golf_course_warehouse)) {
  tmp_file_golf_course <- tempfile(fileext = ".geojson")
  aws.s3::save_object(remote_file_golf_course_raw, file = tmp_file_golf_course)

  st_read(tmp_file_golf_course) %>%
    st_transform(4326) %>%
    rename_with(tolower) %>%
    mutate(
      geometry_3435 = st_transform(geometry, 3435)
    ) %>%
    sfarrow::st_write_parquet(remote_file_golf_course_warehouse)
}
