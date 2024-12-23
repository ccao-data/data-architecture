library(aws.s3)
library(DBI)
library(dplyr)
library(geoarrow)
library(glue)
library(janitor)
library(miniUI)
library(noctua)
library(purrr)
library(rJava)
library(shiny)
library(sf)
library(stringr)
library(tabulizer)
library(tidygeocoder)
library(tidyr)
source("utils.R")

# Script to transform raw data on O'Hare noise into clean Athena tables
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

##### OHARE NOISE MONITORS #####
file_paths <- c(
  "noise_levels" = file.path(
    AWS_S3_RAW_BUCKET, "spatial", "environment",
    "ohare_noise_monitor", "ORD_Fact_Sheet_Monitors_History.pdf"
  ),
  "addresses" = file.path(
    AWS_S3_RAW_BUCKET, "spatial", "environment",
    "ohare_noise_monitor", "ORD_Fact_Sheet_Monitors_Introduction.pdf"
  ),
  "contour" = file.path(
    AWS_S3_RAW_BUCKET, "spatial", "environment",
    "ohare_noise_contour", "ORD_2016_Noise_Contour.geojson"
  )
)

# Grab noise level pdf
tmp_file <- tempfile(fileext = ".pdf")
tmp_dir <- tempdir()
aws.s3::save_object(file_paths["noise_levels"], file = tmp_file)

noise_levels <- extract_tables(split_pdf(tmp_file)[2])

# Clean columns
columns <- c(
  noise_levels[[1]][1, 1:2],
  noise_levels[[1]][3, 3:ncol(noise_levels[[1]])]
)
columns <- unlist(strsplit(columns, " "))
columns[length(columns)] <- paste(
  noise_levels[[1]][, ncol(noise_levels[[1]])],
  collapse = " "
)

# Add column names to dataframe of noise levels
noise_levels <- data.frame(noise_levels[[2]])
names(noise_levels) <- columns

# Clean NAs and remove inactive sites
noise_levels <- noise_levels %>%
  mutate(
    across(where(is.character), ~ na_if(.x, "n/a")),
    across(where(is.character), ~ na_if(.x, "--"))
  ) %>%
  filter(!Site %in% c("6", "9", "36", "45"))

# Grab sensor addresses pdf
aws.s3::save_object(file_paths["addresses"], file = tmp_file)

# Only select site and address columns
addresses <- data.frame(extract_areas(tmp_file[1], pages = 1)[[1]]) %>%
  row_to_names(row_number = 1)

noise_addresses <- left_join(noise_levels, addresses, by = "Site") %>%
  # Some of these addresses are wrong or won't geocode
  # for other reasons and need to be manually updated
  mutate(
    Address = case_when(
      Address == "1600 Nicholas Avenue" ~ "1600 Nicholas Blvd",
      Address == "7240 Argyle Street" ~ "7240 W Argyle St",
      Address == "7515 W. Cullom Avenue" ~ "7515 Cullom Ave",
      Address == "1803 Lavergne Drive" ~ "1803 Lavergne Dr",
      Address == "799 School Street" ~ "799 S School St",
      Address == "1100 Parkside Drive" ~ "1100 Parkside Dr",
      Address == "1421 Garden Street" ~ "1421 Garden St",
      grepl("Harold Avenue", Address) ~ "4934 Harold Ave",
      Address == "744 Edgewood Avenue" ~ "744 S Edgewood Ave",
      Address == "720A S. Prospect Avenue" ~ "720 S Prospect Ave",
      Address == "7990 W. Keeney Street" ~ "7990 Keeney St",
      TRUE ~ Address
    ),
    Community = case_when(
      Address == "459 Geneva Avenue" ~ "Hillside",
      Community == "Mount Prospect" ~ "Mt Prospect",
      TRUE ~ Community
    )
  ) %>%
  mutate(complete_address = paste(Address, Community, "IL", sep = ", ")) %>%
  geocode(complete_address) %>%
  select(-complete_address)

# Cleanup geocode results
noise_addresses_clean <- noise_addresses %>%
  mutate(
    lat = ifelse(is.na(Address), NA, lat),
    long = ifelse(is.na(Address), NA, long)
  ) %>%
  pivot_longer(
    cols = starts_with(c("1", "2")),
    names_to = "year",
    values_to = "noise"
  ) %>%
  clean_names() %>%
  mutate(
    across(c("noise", "modeled_omp_build_out_values"), as.numeric),
    year = as.integer(year)
  ) %>%
  st_as_sf(
    coords = c("long", "lat"),
    crs = 4326
  ) %>%
  mutate(geometry_3435 = st_transform(geometry, 3435))

# Write to S3
remote_file <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "environment",
  "ohare_noise_monitor", "ohare_noise_monitor.parquet"
)
geoparquet_to_s3(noise_addresses_clean, remote_file)
file.remove(tmp_file)


##### OHARE NOISE CONTOUR #####
remote_file <- file.path(
  AWS_S3_WAREHOUSE_BUCKET, "spatial", "environment",
  "ohare_noise_contour", "year=2020", "part-0.parquet"
)

# Grab contour file
tmp_file <- tempfile(fileext = ".geojson")
aws.s3::save_object(file_paths["contour"], file = tmp_file)

# Read file and cleanup
ohare_noise_contour <- st_read(tmp_file) %>%
  st_transform(4326) %>%
  st_cast("POLYGON") %>%
  summarize(geometry = st_union(geometry)) %>%
  nngeo::st_remove_holes() %>%
  rename_with(tolower) %>%
  mutate(
    airport = "ORD",
    decibels = 65L,
    geometry_3435 = st_transform(geom, 3435)
  ) %>%
  select(airport, decibels, geometry = geom, geometry_3435) %>%
  geoparquet_to_s3(remote_file)
