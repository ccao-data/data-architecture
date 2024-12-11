library(arrow)
library(aws.s3)
library(ccao)
library(dplyr)
library(geoarrow)
library(glue)
library(here)
library(osmdata)
library(purrr)
library(sf)
library(tictoc)

# This script detects corner lots in Cook County parcels and saves a boolean
# indicator as well as the cross used by the corner detection algorithm
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "ccao", "corner")

# Get the parcel file years for which we should make corner lot indicators
parcel_path <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "parcel")
parcel_years <- open_dataset(parcel_path) %>%
  distinct(year) %>%
  collect() %>%
  pull(year)

# Drop years before 2014, since OSM data for roads is spotty prior to that
parcel_years <- parcel_years[parcel_years >= 2014]

# Iterate over the years and townships, saving the results to a Parquet file
# on S3 after each iteration
for (iter_year in parcel_years) {
  tictoc::tic(paste("Finished processing corners for:", iter_year))

  # Load the full year's parcel file to iterate though by township
  parcels <- open_dataset(parcel_path) %>%
    filter(year == iter_year) %>%
    geoarrow_collect_sf()

  for (iter_town in ccao::town_dict$township_code) {
    print(paste("Now processing township:", iter_town))

    remote_file <- file.path(
      output_bucket,
      glue("year={iter_year}"),
      glue("township_code={iter_town}"),
      "part-0.parquet"
    )

    town_parcels <- parcels %>%
      filter(town_code == iter_town)

    # Get a bounding box 3,000 feet larger than the extent of the town parcels.
    # Used to query OSM for streets and to ensure parcels on the "edge" of the
    # township have their correct set of neighbors to check in Step 3 below
    town_bbox <- town_parcels %>%
      st_set_geometry("geometry_3435") %>%
      st_bbox() %>%
      st_as_sfc() %>%
      st_buffer(3000, endCapStyle = "FLAT", joinStyle = "MITRE") %>%
      st_transform(4326) %>%
      st_bbox()

    # Get a buffered copy of the town parcels. This is used in Step 3 below in
    # order to fill any tiny gaps between abutting parcels
    town_parcels_buf <- parcels %>%
      filter(
        between(lon, town_bbox$xmin, town_bbox$xmax) &
          between(lat, town_bbox$ymin, town_bbox$ymax)
      ) %>%
      st_buffer(dist = units::set_units(2, "m"))

    # Fetch the OSM street network for the township, removing any OSM way types
    # that are not main roads
    osm_streets <- opq(
      bbox = town_bbox,
      datetime = glue("{iter_year}-01-01T00:00:00Z"),
      timeout = 900
    ) %>%
      add_osm_feature(key = "highway") %>%
      osmdata_sf() %>%
      .$osm_lines %>%
      filter(
        !highway %in% c(
          "bridleway", "construction", "corridor", "cycleway", "elevator",
          "service", "services", "steps", "platform", "motorway",
          "motorway_link", "pedestrian", "track", "path", "footway", "alley"
        )
      ) %>%
      st_transform(3435)

    # Step 1: Find the minimum rectangle that bounds the parcel, then use that
    # rectangle to determine the parcel's orientation and length. These values
    # are used in the next step to draw a cross on the parcel
    town_mrr <- town_parcels %>%
      st_minimum_rotated_rectangle() %>%
      select(lon, lat, geometry = geometry_3435) %>%
      mutate(id = row_number()) %>%
      st_transform(4326) %>%
      st_cast("POINT") %>%
      mutate(
        length = st_distance(geometry, lead(geometry), by_element = TRUE) +
          units::set_units(40, "m"),
        bearing = c(
          lwgeom::st_geod_azimuth(geometry),
          units::set_units(NA, "radians")
        ),
        bearing = units::set_units(bearing, "degrees")
      ) %>%
      # Rectanges cast to points (as performed above) have an additional point
      # to close their polygon; this step drops that point since it isn't needed
      # for our calculation
      filter(row_number() %% 5 != 0) %>%
      # To suppress the warning about repeating constant attributes when casting
      # geometries to POINT
      suppressWarnings()

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
      seq_len(nrow(cross)),
      function(i) draw_line(cross[i, ]),
      simplify = FALSE
    ))

    # Convert the cross raw geometry back to a spatial dataframe
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
      function(i) {
        setdiff(
          intersect(intersects_cross[[i]], intersects_neigh[[i]]),
          intersects_self[[i]]
        )
      }
    )

    # Remove cross segments that intersect neighboring parcels. Also remove any
    # crosses with only a single remaining segment, since these can't be corners
    cross_filter <- cross %>%
      mutate(int_ind = map_lgl(intersects_c_and_n, \(x) length(x) > 0)) %>%
      filter(!int_ind) %>%
      filter(n() > 1, .by = id)

    # Step 4: Find which cross segments intersect streets, and drop any segments
    # that don't. This will remove segments that are pointed at buildings,
    # alleys, and other non-road subjects
    intersects_streets <- st_intersects(cross_filter, osm_streets)
    cross_filter <- cross_filter %>%
      mutate(int_str = map_lgl(intersects_streets, \(x) length(x) > 0)) %>%
      filter(int_str) %>%
      filter(n() > 1, .by = id)

    # Step 5: Calculate the angle of remaining cross segments in order to filter
    # out segments that are not at right angles. This is important because we
    # don't want parallel segments at opposite sides of the parcel, which would
    # not indicate a corner
    angle_diff <- function(theta1, theta2) {
      theta <- abs(theta1 - theta2) %% 360
      return(ifelse(theta > 180, 360 - theta, theta))
    }

    # Filter for only crosses with right-angle segments or gte three segments,
    # as 180 degree segments indicate a parcel sandwiched between two streets
    cross_final <- cross_filter %>%
      group_by(id) %>%
      mutate(
        bearing = units::drop_units(bearing),
        diff_degree = angle_diff(bearing, lag(bearing)),
        max_degree = round(max(diff_degree, na.rm = TRUE))
      ) %>%
      filter(max_degree == 90 | n() >= 3, na.rm = TRUE) %>%
      summarize(
        is_corner_lot = TRUE,
        num_cross_branch = n(),
        geometry = st_make_valid(st_union(geometry))
      ) %>%
      st_cast("MULTILINESTRING") %>%
      rename(geometry_3435 = geometry) %>%
      mutate(geometry = st_make_valid(st_transform(geometry_3435, 4326))) %>%
      relocate(geometry_3435, .after = geometry)

    # Write the final results to a Parquet file on S3
    town_parcels %>%
      mutate(id = row_number()) %>%
      st_drop_geometry() %>%
      select(pin10, id) %>%
      inner_join(cross_final, by = "id") %>%
      select(-id) %>%
      write_geoparquet(remote_file)
  }
  tictoc::toc()
}
