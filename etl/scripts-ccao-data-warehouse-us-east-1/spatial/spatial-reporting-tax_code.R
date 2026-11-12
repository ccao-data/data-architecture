library(ccao)
library(DBI)
library(dplyr)
library(glue)
library(noctua)
library(sf)
library(stringr)
source("utils.R")

# This script builds a map of combined tax code agencies for a given tax year
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
export_path <- file.path(AWS_S3_WAREHOUSE_BUCKET, "export", "geojson")
output_path <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "reporting", "tax_code"
)

# Declare tax year for which to build the map.
tax_year <- 2024

# Declare buffer distance for cleaning up small spaces between parcels that are
# part of the same tax code group. The units are in feet, so 2 feet is a
# reasonable distance to use here.
buffer_dist <- 2

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
  # code number.
  mutate(tax_code_group = cur_group_id(), .by = agency_names) %>%
  mutate(across(starts_with("agency_"), ~ str_split(.x, ", ")))

# Grab geometry and tax code for each pin. This will let us join on the combined
# agency names we built above.
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

# Combine parcel geometry and tax code information and consolidate by geometry.
# PIN10s aren't relevant at this point since we only care about where on the map
# the tax code groups are located.
pin_geometry_clean <- pin_geometry %>%
  left_join(agencies, by = "tax_code_num") %>%
  # Some PIN10s have multiple tax codes, there is one PIN10 (1230402050) that
  # is partially in a municipality and unincorporated, and there are a few
  # cases where stacked PIN10s with identical geometry have different tax
  # codes (17-10-105-013 - 17-10-105-016). What this all means is that we can
  # only aggregate by geometry in order to avoid overlap for tax code group
  # polygons. Even doing this, we're still going to have some cases where
  # different PIN10s in different tax code groups have overlapping but not
  # identical geometry. That problem needs to be solved using spatial means.
  group_by(township, geometry) %>%
  summarise(
    # Unfortunately there is one PIN (1230402050) that is partially in a
    # municipality and unincorporated, so we have aggregate municipality as
    # well
    tax_code_group = paste(unique(tax_code_group), collapse = "/"),
    across(
      c(municipality, tax_code_rate), ~ list(unique(.x))
    ),
    across(starts_with("agency_"), ~ list(unique(unlist(.x)))),
    .groups = "drop"
  )

# Dissolve parcel geometry by tax code group
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
  ), .groups = "drop")

# Resolve overlapping polygons: split overlaps into distinct pieces and
# combine the non-geometry columns of any rows contributing to each piece.
# township, municipality, tax_code_rate, agency_nums, and agency_names are
# all list-columns here, so each combined value is also wrapped in a list.
intersection_file_path <- file.path("tmp", glue("pieces_{tax_year}.parquet"))
if (!file.exists(intersection_file_path)) {
  pieces <- st_intersection(tax_code_geometry)

  pieces %>%
    sfarrow::st_write_parquet(intersection_file_path, compression = "snappy")
} else {
  pieces <- sfarrow::st_read_parquet(intersection_file_path)
}

tax_code_geometry_clean <- pieces %>%
  mutate(
    across(
      c(
        tax_code_group, township, municipality,
        tax_code_rate, agency_nums, agency_names
      ),
      ~ map(origins, \(i) unique(unlist(geo[[cur_column()]][i])))
    ),
    area = as.numeric(st_area(geometry))
  ) %>%
  select(-n.overlaps, -origins) %>%
  mutate(tax_code_group = map_chr(
    tax_code_group,
    ~ paste(sort(unique(unlist(strsplit(.x, "/")))), collapse = "/")
  )) %>%
  st_collection_extract("POLYGON") %>%
  st_buffer(dist = -buffer_dist) %>%
  st_buffer(dist = buffer_dist) %>%
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
  st_transform(crs = 4326)

tax_code_geometry_clean %>%
  geoparquet_to_s3(
    s3_output_path = file.path(output_path, glue("{tax_year}.parquet")),
    overwrite = TRUE
  )
