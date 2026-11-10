library(ccao)
library(DBI)
library(dplyr)
library(glue)
library(noctua)
library(purrr)
library(sf)
library(sfarrow)
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
    ARRAY_AGG(agency_num ORDER BY agency_num) as agency_nums_list,
    LISTAGG(agency_name, ', ') WITHIN GROUP (ORDER BY agency_name)
      AS agency_names,
    ARRAY_AGG(agency_name ORDER BY agency_name) as agency_names_list,
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
  select(-agency_names)

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
  st_transform(crs = 3435)

# For a list of vectors (one per row in a group), keep elements common to every
# row as individual values, and collapse elements that vary across rows into one
# slash-separated string. In other words, we're going to look within 10-digit
# PINs for common agencies and include them as individual values, while
# collapsing the agencies that don't appear across all 14-digit PINs within the
# 10-digit PIN into a single string separated by slashes.
combine_list_elements <- function(x) {
  elements <- unique(unlist(x))
  in_every_row <- map_lgl(elements, \(element) {
    all(map_lgl(x, \(vector) element %in% vector))
  })
  common <- elements[in_every_row]
  varying <- elements[!in_every_row]
  sort(c(common, if (length(varying) > 0) paste(varying, collapse = "/")))
}

# Combine parcel geometry and tax code information with the combo agency names
# and numbers we built above.
tmp_path <- file.path("tmp", glue("tax_code_map_{tax_year}.parquet"))
if (!file.exists(tmp_path)) {
  message(glue("Cleaning parcel geometry for {tax_year}..."))

  pin_geometry_clean <- pin_geometry %>%
    left_join(agencies, by = "tax_code_num") %>%
    mutate(township = ccao::town_convert(substr(tax_code_num, 1, 2))) %>%
    # Some PINs have multiple tax codes, so here we aggregate our data at the
    # pin-level.
    summarise(
      # Unfortunately there is one PIN (1230402050) that is partially in a
      # municipality and unincorporated, so we have aggregate municipality as
      # well
      municipality = paste(unique(municipality), collapse = "/"),
      tax_code_group = paste(unique(tax_code_group), collapse = "/"),
      tax_code_rate = paste(unique(tax_code_rate), collapse = "/"),
      agency_nums = list(combine_list_elements(agency_nums_list)),
      agency_names = list(combine_list_elements(agency_names_list)),
      .by = c(pin10, township, geometry)
    )
  pin_geometry_clean %>%
    sfarrow::st_write_parquet(tmp_path)
} else {
  message(glue("Reading cleaned parcel geometry for {tax_year}..."))
  pin_geometry_clean <- sfarrow::st_read_parquet(tmp_path)
}

tax_code_geometry <- pin_geometry_clean %>%
  # Collapse agency names and numbers into a single string for each PIN to
  # speed up summarizing and display better on a map.
  mutate(across(
    starts_with("agency_"), ~ map_chr(.x, ~ paste(.x, collapse = "<br>"))
  )) %>%
  summarise(
    .by = c(
      township,
      municipality,
      tax_code_group,
      tax_code_rate,
      agency_nums,
      agency_names,
    ),
    across(geometry, st_union)
  ) %>%
  mutate(multiple_tax_code_group = str_detect(tax_code_group, "/"))

# Clean up small spaces left between parcels that are part of the same tax code
# group.
buffer_dist <- 2
tax_code_buffered <- tax_code_geometry %>%
  st_buffer(dist = buffer_dist) %>%
  st_buffer(dist = -buffer_dist) %>%
  st_transform(crs = 4326)
