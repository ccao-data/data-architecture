library(aws.s3)
library(dplyr)
library(geoarrow)
library(sf)
library(igraph)

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
input_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "environment")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "environment")

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
    # Dissolve touching polygons, keeping the first OSM ID
    mutate(
      geometry_3435 = st_transform(geometry, 3435),
      touches = components(graph.adjlist(st_touches(.)))$membership
    ) %>%
    st_make_valid() %>%
    st_cast("MULTIPOLYGON") %>%
    group_by(touches) %>%
    summarize(
      id = ifelse(
        any(startsWith("way/|relation/", id)),
        first(grep("way/relation/", id, value = TRUE)),
        first(id)
      )
    ) %>%
    ungroup() %>%
    mutate(
      geometry_3435 = st_transform(geometry, 3435)
    ) %>%
    select(-touches) %>%
    geoparquet_to_s3(remote_file_golf_course_warehouse)
}
