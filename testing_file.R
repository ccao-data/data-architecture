parcel_files_df <- aws.s3::get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET,
  prefix = file.path("spatial", "parcel")
) %>%
  filter(Size > 0) %>%
  mutate(
    year = str_extract(Key, "[0-9]{4}"),
    s3_uri = file.path(AWS_S3_RAW_BUCKET, Key),
    type = ifelse(str_detect(s3_uri, "geojson"), "spatial", "attr")
  ) %>%
  select(year, s3_uri, type) %>%
  pivot_wider(names_from = type, values_from = s3_uri)
