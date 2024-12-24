library(arrow)
library(aws.s3)
library(dplyr)
library(geoarrow)
library(lubridate)
library(purrr)
library(sf)
library(stringr)
library(tidytransit)
source("utils.R")

# This script transforms GTFS data from Cook County transit systems into
# spatial data files
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "transit")

# Get list of all GTFS feeds in raw bucket
gtfs_feeds_df <- aws.s3::get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET,
  prefix = file.path("spatial", "transit")
) %>%
  filter(Size > 0) %>%
  mutate(
    date = as.Date(str_extract(Key, "[0-9]{4}-[0-9]{2}-[0-9]{2}")),
    year = year(date),
    agency = dirname(str_sub(Key, 17, -1)),
    raw_feed_url = file.path(AWS_S3_RAW_BUCKET, Key)
  )

process_gtfs_feed <- function(s3_bucket_uri, date, year, agency, feed_url) {
  # Construct dest paths from input
  remote_file_stop <- file.path(
    s3_bucket_uri, "transit_stop",
    paste0("agency=", agency),
    paste0("year=", year),
    paste0(date, "-gtfs.parquet")
  )
  remote_file_route <- file.path(
    s3_bucket_uri, "transit_route",
    paste0("agency=", agency),
    paste0("year=", year),
    paste0(date, "-gtfs.parquet")
  )

  if (!object_exists(remote_file_stop) || !object_exists(remote_file_route)) {
    tmp_file <- tempfile(fileext = ".zip")
    tmp_feed_file <- aws.s3::save_object(feed_url, file = tmp_file)
    gtfs_feed <- read_gtfs(tmp_feed_file)
    file.remove(tmp_file)

    # First fetch all stops for the given feed, get their geometry and attached
    # route, then upload
    if (!object_exists(remote_file_stop)) {
      map_dfr(c(0, 1, 2, 3), function(x) {
        gtfs_feed %>%
          filter_stops(
            route_ids = .$routes %>% filter(route_type == x) %>% pull(route_id),
            service_ids = .$calendar %>% pull(service_id)
          ) %>%
          stops_as_sf() %>%
          st_transform(4326) %>%
          mutate(
            route_type = as.integer(x), feed_pull_date = date,
            geometry_3435 = st_transform(geometry, 3435)
          )
      }) %>%
        select(
          stop_id, stop_name, route_type,
          any_of(c("location_type", "parent_station", "wheelchair_boarding")),
          any_of(c("feed_pull_date", "geometry", "geometry_3435"))
        ) %>%
        geoparquet_to_s3(remote_file_stop)
    }

    # Now create route geometries and save. Skip PACE since they have no geoms
    if (!object_exists(remote_file_route)) {
      if (agency == "pace") {
        gtfs_feed %>%
          .$routes %>%
          mutate(feed_pull_date = date, route_type = as.integer(route_type)) %>%
          select(
            route_id, route_type, route_short_name, route_long_name,
            any_of(c("route_color", "route_text_color")),
            any_of(c("feed_pull_date", "geometry", "geometry_3435"))
          ) %>%
          write_parquet(remote_file_route)
      } else {
        gtfs_feed %>%
          gtfs_as_sf() %>%
          get_route_geometry() %>%
          st_transform(4326) %>%
          left_join(gtfs_feed$routes, by = "route_id") %>%
          mutate(
            feed_pull_date = date,
            route_type = as.integer(route_type),
            geometry_3435 = st_transform(geometry, 3435)
          ) %>%
          select(
            route_id, route_type, route_short_name, route_long_name,
            route_color, route_text_color,
            feed_pull_date, geometry, geometry_3435
          ) %>%
          geoparquet_to_s3(remote_file_route)
      }
    }
  }
}

# Apply function to all transit files
pwalk(gtfs_feeds_df, function(...) {
  df <- tibble::tibble(...)
  process_gtfs_feed(
    s3_bucket_uri = output_bucket,
    date = df$date,
    year = df$year,
    agency = df$agency,
    feed_url = df$raw_feed_url
  )
}, .progress = TRUE)

# Create dictionary for GTFS numeric codes
# See: https://developers.google.com/transit/gtfs/reference
# nolint start
transit_dict <- tribble(
  ~"field_name", ~"field_code", ~"field_label", ~"field_label_long",
  "route_type", 0, "streetcar", "Tram, Streetcar, Light rail. Any light rail or street level system within a metropolitan area.",
  "route_type", 1, "subway", "Subway, Metro. Any underground rail system within a metropolitan area.",
  "route_type", 2, "rail", "Rail. Used for intercity or long-distance travel.",
  "route_type", 3, "bus", "Bus. Used for short- and long-distance bus routes.",
  "route_type", 4, "ferry", "Ferry. Used for short- and long-distance boat service.",
  "route_type", 5, "cable_tram", "Cable tram. Used for street-level rail cars where the cable runs beneath the vehicle, e.g., cable car in San Francisco.",
  "route_type", 6, "aerial_lift", "Aerial lift, suspended cable car (e.g., gondola lift, aerial tramway). Cable transport where cabins, cars, gondolas or open chairs are suspended by means of one or more cables.",
  "route_type", 7, "funicular", "Funicular. Any rail system designed for steep inclines.",
  "route_type", 11, "trolleybus", "Trolleybus. Electric buses that draw power from overhead wires using poles.",
  "route_type", 12, "monorail", "Monorail. Railway in which the track consists of a single rail or a beam."
) %>%
  # nolint end
  mutate(
    field_code = as.integer(field_code),
    loaded_at = as.character(Sys.time())
  )

# Write dict to parquet
remote_file_dict <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "transit",
  "transit_dict", "transit_dict.parquet"
)
write_parquet(transit_dict, remote_file_dict)
