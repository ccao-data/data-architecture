library(ccao)
library(DBI)
library(dplyr)
library(glue)
library(noctua)
library(sf)
library(stringr)
library(tidyr)
source("utils.R")

# Connect to Athena
noctua_options(unload = TRUE)
AWS_ATHENA_CONN_NOCTUA <- dbConnect(noctua::athena(), rstudio_conn_tab = FALSE)

# This script builds a map of tax codes for a given tax year. At a high level,
# it pulls parcel geometry and tax code data from Athena, dissolves parcels into
# tax code polygons, resolves any overlaps between those polygons, and cleans up
# the resulting geometry for display. Because the script performs many spatial
# operations in sequence (buffering, intersection, dissolving, etc.), small
# geometry errors can compound; use sf::st_is_valid() regularly throughout to
# check that polygons remain valid, and repair with sf::st_make_valid() as
# needed.
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
export_path <- file.path(AWS_S3_WAREHOUSE_BUCKET, "export", "geojson")
output_path <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "reporting", "tax_code"
)

# Declare tax year for which to build the map.
tax_year <- 2024

# Declare buffer distance for cleaning up small spaces within polygons and
# artifacts from the intersection operation. Distance is in feet.
buffer_dist <- 3

# Choose a mitre distance that is large enough to create sharp corners, but
# small enough to avoid creating overlaps. After testing, a mitre distance of 3
# feet seems to be a good balance.
mitre_dist <- 3

# Aggregate agency names and nums by tax code and rate. This will allow us to
# create tax code "groups" to include in our map.
agencies <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA,
  glue_sql("
  WITH tax_codes AS (
    SELECT code.tax_code_num,
      code.tax_code_rate,
      agency.agency_num,
      info.agency_name,
      CASE
        WHEN info.minor_type = 'MUNI' then info.agency_name ELSE NULL
      END AS municipality
    FROM tax.agency
      LEFT JOIN tax.tax_code as code ON code.agency_num = agency.agency_num
      AND agency.year = code.year
      LEFT JOIN tax.agency_info as info ON code.agency_num = info.agency_num
    WHERE agency.year = '{tax_year}'
  )
  SELECT tax_code_num,
    tax_code_rate,
    LISTAGG(agency_name, ', ') WITHIN GROUP (ORDER BY agency_name)
      AS agency_names,
    LISTAGG(agency_num, ', ') WITHIN GROUP (ORDER BY agency_num)
      AS agency_nums,
    COALESCE(LISTAGG(municipality, ', ') WITHIN GROUP
      (ORDER BY municipality), 'UNINCORPORATED') AS municipality
  FROM tax_codes
  GROUP BY tax_code_num,
    tax_code_rate
",
    .con = AWS_ATHENA_CONN_NOCTUA
  )
) %>%
  # Assign tax code group number based on unique agency names rather than tax
  # code number. Group numbers CAN change if the underlying data changes.
  group_by(agency_names) %>%
  mutate(tax_code_group = cur_group_id()) %>%
  ungroup() %>%
  # Working with these aggregated columns as lists makes it easier to keep their
  # elements unique as we continue to aggregate.
  mutate(across(starts_with("agency_"), ~ str_split(.x, ", ")))

# Grab geometry and tax code for each pin. This will let us join the combined
# agency names we built above to the geometry for each pin.
pin_geometry <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA,
  glue_sql("
  SELECT DISTINCT SUBSTR(pin.pin, 1, 10) AS pin10,
	  pin.tax_code_num,
	  geo.geometry
  FROM tax.pin
	  INNER JOIN spatial.parcel as geo ON substr(pin.pin, 1, 10) = geo.pin10
	  AND pin.year = geo.year
  WHERE pin.year = '{tax_year}'
  ",
    .con = AWS_ATHENA_CONN_NOCTUA
  )
) %>%
  st_as_sf(crs = 4326) %>%
  # The CRS for this data is set to 4326, so that's what we ingest it as, but
  # working with 3435 is faster and more accurate for buffering and other
  # spatial operations.
  st_transform(crs = 3435) %>%
  mutate(township = ccao::town_convert(substr(tax_code_num, 1, 2)))

# Determine which taxing agencies are "core" agencies for each municipality. A
# core agency taxes *all* pins within a given municipality.
core_agencies <- pin_geometry %>%
  st_drop_geometry() %>%
  left_join(agencies, by = "tax_code_num") %>%
  select(pin10, municipality, agency_names) %>%
  unnest(cols = c(agency_names)) %>%
  mutate(muni_pins = n_distinct(pin10), .by = municipality) %>%
  summarise(
    agency_pins = n_distinct(pin10),
    .by = c("municipality", "muni_pins", "agency_names")
  ) %>%
  mutate(core_agency = muni_pins == agency_pins) %>%
  summarise(
    core_agency_names = list(sort(unique(agency_names[core_agency]))),
    .by = municipality
  )

# Create lists of which agencies are core or peripheral for each tax code.
agencies <- agencies %>%
  left_join(core_agencies, by = "municipality") %>%
  mutate(
    core_agencies = map2(
      agency_names, core_agency_names,
      ~ sort(unique(.x[.x %in% .y]))
    ),
    peripheral_agencies = map2(
      agency_names, core_agency_names,
      ~ sort(unique(.x[!.x %in% .y]))
    )
  ) %>%
  select(-core_agency_names)

# Some PIN10s have multiple tax codes, there is one PIN10 (1230402050) that is
# partially in a municipality and unincorporated, and there are a few cases
# where stacked PIN10s with identical geometry have different tax codes
# (17-10-105-013 - 17-10-105-016). What this all means is that we can only
# aggregate by geometry in order to avoid overlap for tax code polygons. Even
# doing this, we're still going to have some cases where different PIN10s in
# different tax codes have overlapping but not identical geometry. That problem
# needs to be solved using a spatial intersection below.
pin_geometry_clean <- pin_geometry %>%
  left_join(agencies, by = "tax_code_num") %>%
  group_by(geometry) %>%
  summarise(
    tax_code_num = paste(unique(tax_code_num), collapse = "/"),
    across(
      c(
        township, municipality, tax_code_rate, tax_code_group,
        core_agencies, peripheral_agencies
      ),
      ~ list(unique(.x))
    ),
    across(starts_with("agency_"), ~ list(unique(unlist(.x)))),
    .groups = "drop"
  )

# Dissolve simplified parcel geometry by tax code. Make sure to write this
# to a file so that the tax groups that are used for the intersection are
# consistent with the groups that will be used in the final aggregation.
tax_code_geo_path <- file.path("tmp", glue("tax_code_geo_{tax_year}.parquet"))
if (!file.exists(tax_code_geo_path)) {
  message(glue("Building tax code geometry for {tax_year}..."))
  tax_code_geometry <- pin_geometry_clean %>%
    group_by(tax_code_num) %>%
    summarise(across(
      c(
        township, municipality, tax_code_rate, tax_code_group,
        agency_nums, agency_names, core_agencies, peripheral_agencies
      ),
      ~ list(unique(unlist(.x)))
    ), .groups = "drop") %>%
    st_make_valid()

  tax_code_geometry %>%
    sfarrow::st_write_parquet(tax_code_geo_path, compression = "snappy")
} else {
  message(glue("Reading tax code geometry for {tax_year}..."))
  tax_code_geometry <- sfarrow::st_read_parquet(tax_code_geo_path)
}

# Resolve overlapping polygons: split overlaps into distinct pieces.
intersection_file_path <- file.path("tmp", glue("pieces_{tax_year}.parquet"))
if (!file.exists(intersection_file_path)) {
  message(glue("Building tax code pieces for {tax_year}..."))
  pieces <- st_intersection(tax_code_geometry)

  pieces %>%
    sfarrow::st_write_parquet(intersection_file_path, compression = "snappy")
} else {
  message(glue("Reading tax code pieces for {tax_year}..."))
  pieces <- sfarrow::st_read_parquet(intersection_file_path)
}

# We now need to take these pieces and re-aggregate them by tax code along with
# the polygons not included in the intersection. Because the pieces are the
# result of a spatial intersection, we need to figure out which pieces came from
# which original polygons. At the same time we need to make sure we are
# aggregating all the combined attributes (tax_code_group, township,
# municipality, etc.) for each piece while keeping their values distinct. The
# result will be a clean set of polygons that represent the tax codes for the
# given tax year.
tax_code_geometry_clean <- pieces %>%
  st_make_valid() %>%
  mutate(
    across(
      c(
        tax_code_group, township, municipality, tax_code_num, tax_code_rate,
        agency_nums, agency_names, core_agencies, peripheral_agencies
      ),
      # Note that we are using the original tax_code_geometry object here to get
      # the values for each piece. This is because "pieces" is the result of a
      # spatial intersection, so we need to figure out which pieces came from
      # which original polygons.
      ~ map(origins, \(i) unique(unlist(tax_code_geometry[[cur_column()]][i])))
    )
  ) %>%
  select(-n.overlaps, -origins) %>%
  # Drop any non-polygon geometries that may have been created during the
  # intersection operation. We don't want linestrings and points.
  st_collection_extract("POLYGON") %>%
  st_make_valid() %>%
  # Clean up tiny artifacts from the intersection operation.
  st_buffer(dist = -buffer_dist) %>%
  st_buffer(
    dist = buffer_dist, joinStyle = "MITRE", mitreLimit = mitre_dist
  ) %>%
  # Drop any tiny sliver polygons left over after the negative/positive buffer.
  filter(as.numeric(st_area(geometry)) >= 100) %>%
  # Aggregate by tax code one final time after the spatial intersection and make
  # sure the values for non-grouping columns are distinct.
  mutate(
    tax_code_num = map_chr(
      tax_code_num,
      ~ paste(sort(unique(unlist(str_split(.x, "/")))), collapse = "/")
    )
  ) %>%
  group_by(tax_code_num) %>%
  summarise(across(
    c(
      township, municipality, tax_code_rate, tax_code_group,
      agency_nums, agency_names, core_agencies, peripheral_agencies
    ),
    ~ paste(sort(unique(unlist(.x))), collapse = ", ")
  ), .groups = "drop") %>%
  mutate(multiple_tax_nums = str_detect(tax_code_num, "/")) %>%
  # Fill in any small gaps within polygons
  st_buffer(
    dist = buffer_dist, joinStyle = "MITRE", mitreLimit = mitre_dist
  ) %>%
  st_buffer(
    dist = -buffer_dist, joinStyle = "MITRE", mitreLimit = mitre_dist
  ) %>%
  st_make_valid() %>%
  st_transform(crs = 4326)

# Write parquet file for use in Athena
tax_code_geometry_clean %>%
  geoparquet_to_s3(file.path(output_path, glue("{tax_year}.parquet")))

# Upload geojson to export bucket for tableau usage
tmp_file <- tempfile(fileext = ".geojson")
st_write(tax_code_geometry_clean, tmp_file)
save_local_to_s3(
  s3_uri = file.path(export_path, "tax_code.geojson"),
  path = tmp_file,
  overwrite = TRUE
)
file.remove(tmp_file)
