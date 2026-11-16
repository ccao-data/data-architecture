library(ccao)
library(DBI)
library(dplyr)
library(glue)
library(noctua)
library(sf)
library(stringr)
source("utils.R")

# This script builds a map of combined tax code agencies for a given tax year.
# At a high level, it pulls parcel geometry and tax code/agency data from
# Athena, dissolves parcels into combined tax code group polygons, resolves
# any overlaps between those polygons, and cleans up the resulting geometry
# for display. Because the script performs many spatial operations in
# sequence (buffering, intersection, dissolving, etc.), small geometry
# errors can compound; use sf::st_is_valid() regularly throughout to check
# that polygons remain valid, and repair with sf::st_make_valid() as needed.
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
# small enough to avoid creating overlaps. After testing, a mitre distance of 5
# feet seems to be a good balance.
mitre_dist <- 5

# Connect to Athena
noctua_options(unload = TRUE)
AWS_ATHENA_CONN_NOCTUA <- dbConnect(noctua::athena(), rstudio_conn_tab = FALSE)

# Aggregate agency names and nums by tax code and rate. This will allow us to
# build a map of combined tax code agencies for a given tax year. We can also
# grab the municipality name for a given taxe code.
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
  # code number. This process is NOT deterministic, so the group numbers will
  # change if the underlying data changes.
  mutate(tax_code_group = cur_group_id(), .by = agency_names) %>%
  # Working with these aggregated columns as lists makes it easier to keep their
  # elements unique as we continue to aggregate.
  mutate(across(starts_with("agency_"), ~ str_split(.x, ", ")))

# Grab geometry and tax code for each pin. This will let us join the combined
# agency names we built above to the geometry for each pin.
pin_geometry <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA,
  glue_sql("
  WITH geo AS (
    SELECT DISTINCT pin10,
      geometry,
      CASE
        WHEN {tax_year} BETWEEN start_year AND end_year
        THEN '{tax_year}' ELSE NULL
      END as year
    FROM tax.pin_geometry_raw
  )
  SELECT DISTINCT SUBSTR(pin.pin, 1, 10) AS pin10,
    pin.tax_code_num,
    geo.geometry
  FROM tax.pin
    INNER JOIN geo ON substr(pin.pin, 1, 10) = geo.pin10
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

# Some PIN10s have multiple tax codes, there is one PIN10 (1230402050) that is
# partially in a municipality and unincorporated, and there are a few cases
# where stacked PIN10s with identical geometry have different tax codes
# (17-10-105-013 - 17-10-105-016). What this all means is that we can only
# aggregate by geometry in order to avoid overlap for tax code group polygons.
# Even doing this, we're still going to have some cases where different PIN10s
# in different tax code groups have overlapping but not identical geometry. That
# problem needs to be solved using a spatial intersection below.
pin_geometry_clean <- pin_geometry %>%
  left_join(agencies, by = "tax_code_num") %>%
  group_by(township, geometry) %>%
  summarise(
    tax_code_group = paste(unique(tax_code_group), collapse = "/"),
    across(
      c(municipality, tax_code_rate), ~ list(unique(.x))
    ),
    across(starts_with("agency_"), ~ list(unique(unlist(.x)))),
    .groups = "drop"
  )

# Dissolve simplified parcel geometry by tax code group. Make sure to write this
# to a file so that the tax groups that are used for the intersection are
# consistent with the groups that will be used for the final aggregation.
tax_code_geo_path <- file.path("tmp", glue("tax_code_geo_{tax_year}.parquet"))
if (!file.exists(tax_code_geo_path)) {
  message(glue("Building tax code geometry for {tax_year}..."))
  tax_code_geometry <- pin_geometry_clean %>%
    group_by(tax_code_group) %>%
    summarise(across(
      c(
        township,
        municipality,
        tax_code_rate,
        agency_nums,
        agency_names
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

# We now need to take these pieces and re-aggregate them by tax code group along
# with the polygons not included in the intersection. This is a bit tricky
# because the pieces are the result of a spatial intersection, so we need to
# figure out which pieces came from which original polygons. At the same time we
# need to make sure we are aggregating all the combined attributes
# (tax_code_group, township, municipality, tax_code_rate, agency_nums, and
# agency_names) for each piece while keeping their values distinct. The result
# will be a clean set of polygons that represent the combined tax code agencies
# for the given tax year.
tax_code_geometry_clean <- pieces %>%
  st_make_valid() %>%
  mutate(
    across(
      c(
        tax_code_group, township, municipality,
        tax_code_rate, agency_nums, agency_names
      ),
      # Note that we are using the original tax_code_geometry object here to get
      # the values for each piece. This is because the pieces object is the
      # result of a spatial intersection, so we need to figure out which pieces
      # came from which original polygons.
      ~ map(origins, \(i) unique(unlist(tax_code_geometry[[cur_column()]][i])))
    )
  ) %>%
  select(-n.overlaps, -origins) %>%
  st_collection_extract("POLYGON") %>%
  st_make_valid() %>%
  # Clean up tiny artifacts from the intersection operation.
  st_buffer(dist = -buffer_dist) %>%
  st_buffer(
    dist = buffer_dist, joinStyle = "MITRE", mitreLimit = mitre_dist
  ) %>%
  # Aggregate tax code group one final time after spatial intersection to make
  # sure its values are distinct.
  mutate(
    tax_code_group = map_chr(
      tax_code_group,
      ~ paste(sort(unique(unlist(str_split(.x, "/")))), collapse = "/")
    )
  ) %>%
  group_by(tax_code_group) %>%
  summarise(across(
    c(
      township,
      municipality,
      tax_code_rate,
      agency_nums,
      agency_names
    ),
    ~ paste(sort(unique(unlist(.x))), collapse = "<br>")
  ), .groups = "drop") %>%
  mutate(multiple_tax_groups = str_detect(tax_code_group, "/")) %>%
  # Fill in any small gaps within polygons
  st_buffer(
    dist = buffer_dist, joinStyle = "MITRE", mitreLimit = mitre_dist
  ) %>%
  st_buffer(dist = -buffer_dist) %>%
  st_make_valid() %>%
  st_transform(crs = 4326) %>%
  st_make_valid()

tax_code_geometry_clean %>%
  geoparquet_to_s3(file.path(output_path, glue("{tax_year}.parquet")))
