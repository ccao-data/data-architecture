library(arrow)
library(ccao)
library(dplyr)
library(geoarrow)
library(geojsonio)
library(purrr)
library(sf)
library(stringi)
library(stringr)
library(rmapshaper)

# This script generates CCAO neighborhood boundaries from raw parcels
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "spatial", "ccao", "neighborhood"
)

for (year in 2010:2021) {
  message("Now processing year: ", year)

  # Load the parcels file from S3
  parcels <- geoarrow::geoarrow_collect_sf(arrow::open_dataset(
    paste0("s3://ccao-data-warehouse-us-east-1/spatial/parcel/year=", year)
  ))

  # Use a positive then negative buffer trick to get orthogonal polygons for
  # each neighborhood. Taken from: https://github.com/hdus/pgtools
  parcels_ortho <- parcels %>%
    st_set_geometry(.$geometry_3435) %>%
    filter(!nbhd_code %in% c("000", "999", "599"), !is.na(nbhd_code)) %>%
    mutate(town_nbhd = paste0(town_code, nbhd_code)) %>%
    # Recode some neighborhoods that don't exist/are wrong
    mutate(
      town_nbhd = recode(
        town_nbhd,
        "20371" = "20100"
      )
    ) %>%
    group_by(town_nbhd) %>%
    summarize(geometry = st_union(geometry)) %>%
    st_buffer(dist = 200, joinStyle = "MITRE", mitreLimit = 2.5) %>%
    st_buffer(dist = -200, joinStyle = "MITRE", mitreLimit = 2.5) %>%
    # Use a negative then positive buffer to remove polygon "spikes"
    st_buffer(dist = -200, joinStyle = "MITRE", mitreLimit = 2.5) %>%
    st_buffer(dist = 250, joinStyle = "MITRE", mitreLimit = 2.5)

  # Some neighborhoods are so slim they get removed by negative buffering, so
  # add those via a separate process with no negative buffering
  parcels_ortho2 <- bind_rows(
    parcels_ortho %>% filter(!st_is_empty(geometry)),
    parcels %>%
      filter(!nbhd_code %in% c("000", "999", "599"), !is.na(nbhd_code)) %>%
      st_set_geometry(.$geometry_3435) %>%
      mutate(town_nbhd = paste0(town_code, nbhd_code)) %>%
      filter(
        town_nbhd %in% (
          parcels_ortho %>%
            filter(st_is_empty(geometry)) %>%
            pull(town_nbhd)
        )
      ) %>%
      group_by(town_nbhd) %>%
      summarize(geometry = st_union(geometry)) %>%
      st_buffer(dist = 200, joinStyle = "MITRE", mitreLimit = 2.5) %>%
      st_buffer(dist = -200, joinStyle = "MITRE", mitreLimit = 2.5)
  )

  # Use mapshaper to simplify shapes and fill gaps between adjacent polygons
  parcels_simplified <- parcels_ortho2 %>%
    st_transform(4326) %>%
    geojson_json(geometry = "polygon", crs = 4326) %>%
    rmapshaper::apply_mapshaper_commands(
      c("-clean", "gap-fill-area=6000000"),
      force_FC = TRUE
    ) %>%
    geojson_sf() %>%
    st_cast("MULTIPOLYGON") %>%
    st_transform(3435) %>%
    ms_simplify(
      keep = 0.3,
      keep_shapes = TRUE,
      weighting = 1,
      snap_interval = 50
    )

  # Finalize by forcing to valid geometry then cleaning again
  parcels_valid <- parcels_simplified %>%
    st_transform(4326) %>%
    st_make_valid() %>%
    st_transform(3435) %>%
    st_make_valid() %>%
    st_transform(4326) %>%
    st_cast("MULTIPOLYGON") %>%
    geojson_json(geometry = "polygon", crs = 4326) %>%
    rmapshaper::apply_mapshaper_commands("-clean", force_FC = TRUE) %>%
    geojson_sf() %>%
    select(-rmapshaperid) %>%
    st_transform(3435)

  # To fill any remaining gaps, especially around the county edges, we can get
  # the boundary of the whole county, divide it into a grid, get the difference
  # between the neighborhoods and the grid, the merge the diff'd grid squares to
  # the neighborhoods
  cook_boundary <- read_geoparquet_sf(file.path(
    AWS_S3_WAREHOUSE_BUCKET,
    "spatial/ccao/county/2019.parquet"
  )) %>%
    st_transform(3435)

  cook_diff <- cook_boundary %>%
    st_make_grid(cellsize = 1000) %>%
    st_intersection(cook_boundary) %>%
    st_difference(st_union(parcels_valid))

  cook_df <- tibble(
    geometry = cook_diff,
    intersects = st_intersects(geometry, parcels_valid)
  )

  # Iteratively assign grid squares to the largest touching neighborhood until
  # no squares are left
  while (nrow(cook_df) > 0) {
    cook_df <- cook_df %>%
      mutate(town_nbhd = map_chr(
        intersects,
        ~ ifelse(
          length(.x) > 0,
          parcels_valid %>%
            slice(.x) %>%
            filter(max(st_area(geometry)) == st_area(geometry)) %>%
            pull(town_nbhd),
          NA_character_
        )
      ))

    parcels_valid <- parcels_valid %>%
      bind_rows(
        cook_df %>%
          filter(!is.na(town_nbhd)) %>%
          select(town_nbhd, geometry)
      ) %>%
      group_by(town_nbhd) %>%
      summarize(geometry = st_union(geometry))

    cook_df <- cook_df %>%
      filter(is.na(town_nbhd)) %>%
      mutate(
        geometry = st_buffer(geometry, 10),
        intersects = st_intersects(geometry, parcels_valid)
      )
  }

  # Re-validate output, clean, and drop slivers
  parcels_clean <- parcels_valid %>%
    st_transform(4326) %>%
    st_make_valid() %>%
    st_transform(3435) %>%
    st_make_valid() %>%
    st_transform(4326) %>%
    st_cast("MULTIPOLYGON") %>%
    geojson_json(geometry = "polygon", crs = 4326) %>%
    rmapshaper::apply_mapshaper_commands(
      c("-filter-slivers", "-clean"),
      force_FC = TRUE
    ) %>%
    geojson_sf() %>%
    filter(!st_is_empty(geometry))

  # Occasionally, mapshaper will remove entire neighborhoods when cleaning, so
  # we must re-add them manually
  parcels_clean2 <- parcels_clean %>%
    bind_rows(
      parcels_valid %>%
        filter(!town_nbhd %in% parcels_clean$town_nbhd) %>%
        st_transform(4326) %>%
        st_make_valid() %>%
        st_transform(3435) %>%
        st_make_valid() %>%
        st_transform(4326) %>%
        st_cast("MULTIPOLYGON")
    ) %>%
    select(-rmapshaperid)

  # Add more attribute data, finalize, and write to S3
  parcels_final <- parcels_clean2 %>%
    mutate(
      township_code = str_sub(town_nbhd, 1, 2),
      nbhd = str_sub(town_nbhd, 3, 5),
      triad_name = ccao::town_get_triad(township_code, name = TRUE),
      triad_code = ccao::town_get_triad(township_code, name = FALSE),
      township_name = ccao::town_convert(township_code),
      geometry_3435 = st_transform(geometry, 3435)
    ) %>%
    select(
      township_name, township_code, triad_name, triad_code,
      nbhd, town_nbhd, geometry, geometry_3435
    ) %>%
    write_geoparquet(
      file.path(output_bucket, paste0("year=", year), "part-0.parquet"),
      compression = "snappy"
    )
}
