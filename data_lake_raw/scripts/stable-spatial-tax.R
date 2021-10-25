library(dplyr)
library(here)
library(purrr)
library(sf)
library(R.utils)

tax_path <- here("s3-bucket", "stable", "spatial", "tax")

api_info <- list(
  # TIF
  "tif_2015" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "si4i-nrtg?method=export&format=GeoJSON",
                 "boundary" = "tif",
                 "year"     = "2015"),

  "tif_2016" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "5dup-xpsj?method=export&format=GeoJSON",
                 "boundary" = "tif",
                 "year"     = "2016"),

  "tif_2018" = c("source"   = "https://opendata.arcgis.com/datasets/",
                 "api_url"  = "cc516b88a47547bd974bba4ebc120ecf_15.geojson",
                 "boundary" = "tif",
                 "year"     = "2018"),

  # SSA
  "ssa_2018" = c("source"   = "https://data.cityofchicago.org/api/geospatial/",
                 "api_url"  = "fz5x-7zak?method=export&format=GeoJSON",
                 "boundary" = "ssa",
                 "year"     = "2020"),

  # PARCEL
  "par_2015" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "nxb6-rw3s?method=export&format=GeoJSON",
                 "boundary" = "parcel",
                 "year"     = "2015"),
  "par_2016" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "a33b-b59u?method=export&format=GeoJSON",
                 "boundary" = "parcel",
                 "year"     = "2016"),
  "par_2017" = c("source"   = "https://opendata.arcgis.com/datasets/",
                 "api_url"  = "a45722101ed8491fb71930fd4c2c64ab_0.geojson",
                 "boundary" = "parcel",
                 "year"     = "2017"),
  "par_2018" = c("source"   = "https://opendata.arcgis.com/datasets/",
                 "api_url"  = "9539568a52124b99addb042efd0f83b1_0.geojson",
                 "boundary" = "parcel",
                 "year"     = "2018"),
  "par_2019" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "c5mi-ck9v?method=export&format=GeoJSON",
                 "boundary" = "parcel",
                 "year"     = "2019"),
  "par_2020" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "2yvh-uwrw?method=export&format=GeoJSON",
                 "boundary" = "parcel",
                 "year"     = "2020"),

  # LIBRARY
  "lib_2015" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "nu5w-d9cb?method=export&format=GeoJSON",
                 "boundary" = "library",
                 "year"     = "2015"),
  "lib_2016" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "junp-4s2h?method=export&format=GeoJSON",
                 "boundary" = "library",
                 "year"     = "2016"),
  "lib_2018" = c("source"   = "https://opendata.arcgis.com/datasets/",
                 "api_url"  = "5d02a289bd774880a49b03e9ed16fc29_7.geojson",
                 "boundary" = "library",
                 "year"     = "2018"),

  # PARK
  "prk_2015" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "r43b-5ipg?method=export&format=GeoJSON",
                 "boundary" = "park",
                 "year"     = "2015"),
  "prk_2016" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "2df4-kwbu?method=export&format=GeoJSON",
                 "boundary" = "park",
                 "year"     = "2016"),
  "prk_2018" = c("source"   = "https://opendata.arcgis.com/datasets/",
                 "api_url"  = "b9e46f08fde04125a9d225da9b1e33f6_11.geojson",
                 "boundary" = "park",
                 "year"     = "2018"),

  # FIRE PROTECTION
  "frp_2015" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "2rg9-v9k5?method=export&format=GeoJSON",
                 "boundary" = "fire_protection",
                 "year"     = "2015"),
  "frp_2016" = c("source"   = "https://datacatalog.cookcountyil.gov/api/geospatial/",
                 "api_url"  = "egxy-cjyk?method=export&format=GeoJSON",
                 "boundary" = "fire_protection",
                 "year"     = "2016"),
  "frp_2018" = c("source"   = "https://opendata.arcgis.com/datasets/",
                 "api_url"  = "8250672861de4690a6602113376015c9_3.geojson",
                 "boundary" = "fire_protection",
                 "year"     = "2018")
)

# function to call referenced API, pull requested data, and write it to specified file path
pull_and_write <- function(x) {

  current_file <- here(tax_path, x["boundary"], paste0(x["year"], ".geojson"))

  if (!file.exists(paste0(current_file, ".gz"))) {

    st_read(paste0(x["source"], x["api_url"])) %>%

      st_write(current_file)

    # compress geojson using gzip
    gzip(filename = current_file,
         destname = paste0(current_file, ".gz"))


  }

}

# apply function to "api_info"
lapply(api_info, pull_and_write)

# clean
rm(list = ls())