library(aws.s3)
library(dplyr)
library(purrr)
library(sf)
library(stringr)
library(tidyr)
source("utils.R")

# This script retrieves the boundaries of various Cook County taxing districts
# and entities, such as TIFs, libraries, etc.
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "spatial", "tax")

# Read privileges for the this drive location are limited.
# Contact Cook County GIS if permissions need to be changed.
file_path <- "//10.122.19.14/ArchiveServices"

# Tax districts
crossing(

  # Paths for all relevant geodatabases
  data.frame("path" = list.files(file_path, full.names = TRUE)) %>%
    filter(
      str_detect(path, "Current", negate = TRUE) &
        str_detect(path, "20") &
        str_detect(path, "Admin")
    ),

  # S3 paths and corresponding layers
  data.frame(
    dir_name = c(
      "bond_district",
      "community_college_district",
      "cc_commissioner_district",
      "county_district",
      "drainage_district",
      "fire_protection_district",
      "forest_preserve_district",
      "general_assistance_district",
      "home_equity_assurance_district",
      "library_fund_district",
      "library_district",
      "mental_health_district",
      "metro_water_reclamation_district",
      "mosquito_abatement_district",
      "municipality_district",
      "neighborhood_district",
      "park_district",
      "public_health_district",
      "road_and_bridge_district",
      "sanitation_district",
      "special_police_district",
      "special_service_area",
      "street_light_district",
      "suburban_tb_sanitarium_district",
      "tif_district",
      "water_commission_river_consrv_district"
    ),
    layer = c(
      "BondTaxDist",
      "CommCollTaxDist",
      "CommTaxDist",
      "CountyTaxDist",
      "DrainageTaxDist",
      "FireProTaxDist",
      "ForestPresHoldTaxDist",
      "GenAssTaxDist",
      "HeatTaxDist",
      "LibrFundTaxDist",
      "LibrTaxDist",
      "MenHealthTaxDist",
      "MetroTaxDist",
      "MosqAbatTaxDis",
      "MuniTaxDist",
      "NeighborhoodTaxDist",
      "ParkTaxDist",
      "PubHealthTaxDist",
      "RdBrTaxDist",
      "SanitaryTaxDist",
      "SpecPolTaxDist",
      "SpecServTaxDist",
      "StrLightTaxDist",
      "SubTBSanTaxDist",
      "TIFTaxDist",
      "WCRCTaxDist"
    )
  )
) %>%
  arrange(dir_name, path) %>%
  # Function to call referenced GDBs, pull requested data, and write it to S3
  pwalk(function(...) {
    df <- tibble::tibble(...)
    county_gdb_to_s3(
      s3_bucket_uri = output_bucket,
      dir_name = df$dir_name,
      file_path = df$path,
      layer = df$layer
    )
  })

# Sidwell grid, originally formatted as Cook County Clerk tax map pages, which
# are Sidwell grid sections divided into eighths.
save_local_to_s3(
  s3_uri = file.path(output_bucket, "sidwell_grid", "sidwell_grid.geojson"),
  path = "O:/AndrewGIS/TaxMaps/TaxMapSheetIndex.geojson",
  overwrite = FALSE
)
