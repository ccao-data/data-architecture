library(arrow)
library(aws.s3)
library(ccao)
library(dplyr)
library(geoarrow)
library(glue)
library(here)
library(osmdata)
library(sf)
library(sfnetworks)
library(tidygraph)
source("utils.R")

# This script queries OpenStreetMap for major roads in Cook County and
# saves them as a spatial parquet
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "corner")
corner_tmp_dir <- here("corner-tmp")

# Get the parcel file years for which we should make corner lot indicators
parcel_path <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "parcel")
parcel_years <- open_dataset(parcel_path) %>%
  distinct(year) %>%
  collect() %>%
  pull(year)

# Drop years before 2014, since OSM data is spotty prior to that
parcel_years <- parcel_years[parcel_years >= 2014]

# Iterate over the years and townships, saving the results to a temporary file
# after each iteration
for (iter_year in parcel_years) {

  # Load the full year's parcel file to iterate though
  parcels <- open_dataset(parcel_path) %>%
    filter(year == iter_year) %>%
    geoarrow_collect_sf()

  for (iter_town in ccao::town_dict$township_code) {
    local_backup <- here(
      corner_tmp_dir,
      glue("year={year}"),
      glue("town={town}"),
      "part-0.parquet"
    )

    town_parcels <- parcels %>%
      filter(town_code == iter_town)

    # Get a bounding box 2000 feet larger than the extent of the town parcels.
    # Used to query OSM for streets and to check for neighboring parcels
    town_bbox <- town_parcels %>%
      st_set_geometry("geometry_3435") %>%
      st_bbox() %>%
      st_as_sfc() %>%
      st_buffer(1000, endCapStyle = "FLAT", joinStyle = "MITRE") %>%
      st_transform(4326) %>%
      st_bbox()

    # Get a buffered copy of the town parcels to check the neighboring parcels
    # at the "edges" of the township
    town_parcels_buf <- parcels %>%
      filter(
        between(lon, town_bbox$xmin, town_bbox$xmax) &
          between(lat, town_bbox$ymin, town_bbox$ymax)
      ) %>%
      st_buffer(dist = units::set_units(1, "m"))

    # Fetch the OSM street network for the township, removing any way types that
    # don't constitute main roads
    osm_streets <- opq(bbox = town_bbox) %>%
      add_osm_feature(key = "highway") %>%
      osmdata_sf() %>%
      .$osm_lines %>%
      filter(
        !highway %in% c(
          "bridleway", "construction", "corridor", "cycleway", "elevator",
          "service", "services", "steps", "platform", "motorway",
          "motorway_link", "pedestrian", "track", "path", "footway", "alley"
        )
      )

    # Step 1: Find the minimum rectangle that bounds the parcel, then use that
    # rectangle to determine the parcel's orientation and length. These values
    # are used in the next step to draw a cross on the parcel
    town_mrr <- town_parcels %>%
      # slice(1:100) %>%
      st_minimum_rotated_rectangle() %>%
      select(lon, lat, geometry = geometry_3435) %>%
      mutate(id = row_number()) %>%
      st_transform(4326) %>%
      st_cast("POINT") %>%
      mutate(
        length = st_distance(geometry, lead(geometry), by_element = TRUE) +
          units::set_units(10, "m"),
        bearing = c(
          lwgeom::st_geod_azimuth(geometry),
          units::set_units(NA, "radians")
        ),
        bearing = units::set_units(bearing, "degrees")
      ) %>%
      filter(row_number() %% 5 != 0)

    # Step 2: Draw a cross that cuts the parcel in four and extends out slightly
    # beyond the minimum rectangle of the parcel. We will look for parcels and
    # streets that intersect this cross in order to determine neighbors. Use the
    # bearing and length calculated in the previous step to draw the cross
    town_parcels_centroid <- town_mrr %>%
      st_drop_geometry() %>%
      st_as_sf(coords = c("lon", "lat"), crs = 4326) %>%
      st_coordinates()

    cross <- geosphere::destPoint(
        p = town_parcels_centroid,
        b = town_mrr$bearing,
        d = town_mrr$length
      ) %>%
      cbind(town_parcels_centroid) %>%
      as.data.frame()

    # Draw the lines that connect the centroids to the endpoints of the cross
    draw_line <- function(r) st_linestring(t(matrix(unlist(r), 2, 2)))
    cross$geometry <- st_sfc(sapply(
      1:nrow(cross),
      function(i) draw_line(cross[i, ]),
      simplify = FALSE
    ))

    # Compute the length and aspect ratio of the cross. The aspect ratio will be
    # useful for filtering out parcels that are dramatically longer than wide
    cross <- cross %>%
      st_set_geometry("geometry") %>%
      st_set_crs(4326) %>%
      mutate(
        id = rep(1:(nrow(.) / 4), each = 4),
        length = st_length(geometry),
        branch = rep(1:4, length.out = n()),
        bearing = town_mrr$bearing
      ) %>%
      st_transform(3435)

    # Step 3: Find the arms of each cross which intersect neighboring parcels
    # which also touch the cross-originating PIN. The idea here is that cross
    # cross arms that intersect neighboring (touching) parcels are likely to be
    # pointed at a building, rather than a street.
    intersects_cross <- st_intersects(cross, town_parcels_buf)
    intersects_neigh <- st_intersects(town_parcels, town_parcels_buf)
    intersects_neigh <- rep(intersects_neigh, each = 4)
    class(intersects_neigh) <- "sgbp"

    # The crosses and parcels will intersect their own buffered origin PIN, so
    # we need to get the index of the origin PIN to filter out the intersection
    intersects_self <- town_parcels %>%
      left_join(
        town_parcels_buf %>%
          mutate(index = row_number()) %>%
          st_drop_geometry() %>%
          select(index, pin10),
        by = "pin10"
      ) %>%
      pull(index) %>%
      rep(each = 4)

    # Compute the intersection of {intersects_cross & intersects_neigh} to find
    # neighboring units that also intersect the cross
    intersects_c_and_n <- lapply(
      seq_along(intersects_cross),
      function(i) setdiff(
        intersect(intersects_cross[[i]], intersects_neigh[[i]]),
        intersects_self[[i]]
      )
    )

    # Remove cross segments that intersect neighboring parcels. Also remove any
    # crosses with only a single remaining segment, since these can't be corners
    cross_filter <- cross %>%
      mutate(int_ind = map_lgl(intersects_c_and_n, \(x) length(x) > 0)) %>%
      filter(!int_ind) %>%
      filter(n() > 1, .by = id)

    # Step 4: Calculate the angle of remaining cross segments in order to filter
    # out segments that are not at right angles. This is important because we
    # don't want parallel segments at opposite sides of the parcel, which would
    # not indicate a corner
    angle_diff <- function(theta1, theta2) {
      theta <- abs(theta1 - theta2) %% 360
      return(ifelse(theta > 180, 360 - theta, theta))
    }

    # Filter for only crosses with right-angle segments or three segments,
    # as 180 degree segments
    cross_corner <- cross_filter %>%
      group_by(id) %>%
      mutate(
        bearing = units::drop_units(bearing),
        diff_degree = angle_diff(bearing, lag(bearing)),
        max_degree = round(max(diff_degree, na.rm = TRUE))
      ) %>%
      filter(max_degree == 90 | n() == 3, na.rm = TRUE) %>%
      ungroup()
  }
}


test_id <- 15
ggplot() +
  geom_sf(
    data = town_parcels %>%
      slice(1:20),
  ) +
  # geom_sf(data = town_parcels_buf %>% slice(551)) +
  geom_sf(
    data = cross_corner %>%
      filter(id %in% 1:20),
      # filter(id %in% test_id) %>%
    aes(color = int_ind),
    alpha = 1
  )
  ggrepel::geom_text_repel(
    data = cross_filter %>%
      filter(id %in% test_id) %>%
      mutate(bearing = round(units::drop_units(bearing))),
    aes(x = lon, y = lat, label = bearing)
  )

