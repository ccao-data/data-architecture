library(arrow)
library(aws.s3)
library(dplyr)
library(geoarrow)
library(purrr)
library(sf)
library(stringr)
library(tidyr)
source("utils.R")

# This script retrieves major political boundaries such as townships and
# judicial districts and keeps only necessary columns for reporting and analysis
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "spatial", "political")

# Gather all relevant raw bucket files
raw_files <- aws.s3::get_bucket_df(
  AWS_S3_RAW_BUCKET,
  prefix = "spatial/political/"
)$Key %>% sapply(function(x) {
  x <- file.path(AWS_S3_RAW_BUCKET, x)
  tmp_file <- tempfile(fileext = ".geojson")
  save_s3_to_local(x, tmp_file)
  st_read(tmp_file) %>% rename_with(tolower)
}, simplify = TRUE, USE.NAMES = TRUE)

# List of columns to keep - place number columns before name columns, if only
# one column is identifed it will be used as both district number and name
columns <- list(
  "board_of_review_district_2012" = c("district_n"),
  "board_of_review_district_2023" = c("district_int", "district_txt"),
  "commissioner_district_2012" = c("district"),
  "commissioner_district_2023" = c("district_int", "district_txt"),
  "congressional_district_2010" = c("district_n"),
  "congressional_district_2023" = c("district_int", "district_txt"),
  "judicial_district_2012" = c("district"),
  "judicial_district_2022" = c("district"),
  "municipality_2000" = c("agency", "agency_desc"),
  "municipality_2001" = c("agency", "agency_desc"),
  "municipality_2002" = c("agency", "agency_desc"),
  "municipality_2003" = c("agency", "agency_desc"),
  "municipality_2004" = c("agency", "agency_desc"),
  "municipality_2005" = c("agency", "agency_desc"),
  "municipality_2006" = c("agency", "agency_desc"),
  "municipality_2007" = c("agency", "agency_desc"),
  "municipality_2008" = c("agency", "agency_desc"),
  "municipality_2009" = c("agency", "agency_desc"),
  "municipality_2010" = c("agency", "agency_desc"),
  "municipality_2011" = c("agency", "agency_desc"),
  "municipality_2012" = c("agency", "agency_desc"),
  "municipality_2013" = c("agency", "agency_desc"),
  "municipality_2014" = c("agency", "agency_desc"),
  "municipality_2015" = c("agency", "agency_desc"),
  "municipality_2016" = c("agency", "agency_desc"),
  "municipality_2017" = c("agency", "agency_desc"),
  "municipality_2018" = c("agency", "agency_desc"),
  "municipality_2019" = c("agency", "agency_desc"),
  "municipality_2020" = c("agency", "agency_desc"),
  "municipality_2021" = c("agency", "agency_desc"),
  "municipality_2022" = c("agency", "agency_desc"),
  "state_representative_district_2010" = c("district_n"),
  "state_representative_district_2023" = c("district_int", "district_txt"),
  "state_senate_district_2010" = c("senatenum", "senatedist"),
  "state_senate_district_2023" = c("district_int", "district_txt"),
  "ward_chicago_2003" = c("ward"),
  "ward_chicago_2015" = c("ward"),
  "ward_chicago_2023" = c("ward"),
  "ward_evanston_2019" = c("ward"),
  "ward_evanston_2022" = c("ward")
)

# Clean data according to each shapefile's associated columns
clean_files <- mapply(function(x, y) {
  message(paste0("processing "), y)

  if (length(columns[[y]]) == 1) {
    x <- x %>% select(all_of(columns[[y]]), geometry)
    names(x) <- c("district_num", "geometry")
    x <- x %>%
      mutate(district_name = str_remove_all(district_num, "[:alpha:]")) %>%
      select("district_num", "district_name", "geometry")
  } else {
    x <- x %>% select(all_of(columns[[y]]), geometry)
    names(x) <- c("district_num", "district_name", "geometry")

    if (str_detect(y, "municipality")) {
      x <- x %>% mutate(
        district_num = case_when(
          (
            is.na(district_name) | district_name %in% c("1", "Unincorp")
          ) ~ NA_integer_,
          TRUE ~ district_num
        ),
        district_name = case_when(
          (
            is.na(district_name) | district_name %in% c("1", "Unincorp")
          ) ~ "Unincorporated",
          TRUE ~ str_to_title(district_name)
        )
      )
    }
  }

  x <- x %>%
    st_make_valid() %>%
    st_transform(4326) %>%
    mutate(district_num = str_remove_all(district_num, "[:alpha:]")) %>%
    mutate(
      across(ends_with("num"), as.integer),
      across(ends_with("name"), str_remove_all, "\\..*"),
      across(ends_with("name"), str_squish),
      year = str_extract(y, "[0-9]{4}")
    ) %>%
    filter(!(district_name %in% c("", "0"))) %>%
    group_by_at(vars(-geometry)) %>%
    summarise() %>%
    ungroup() %>%
    st_cast("MULTIPOLYGON") %>%
    mutate(geometry_3435 = st_transform(geometry, 3435)) %>%
    arrange(district_num)

  return(x)
}, raw_files, names(columns), SIMPLIFY = FALSE, USE.NAMES = TRUE)

# For the sake of convenience, Chicago and Evanston wards are combined
ward_files <- grep("ward", names(clean_files), value = TRUE)

clean_files[["ward"]] <- mapply(function(x, y) {
  x <- x %>%
    mutate(
      district_name = paste(
        str_extract(y, "chicago|evanston"),
        district_name,
        sep = "_"
      )
    )
}, clean_files[ward_files], ward_files, SIMPLIFY = FALSE, USE.NAMES = TRUE) %>%
  bind_rows()

clean_files[ward_files] <- NULL

# Upload to S3
unique(str_remove_all(str_sub(names(columns), 1, -6), "_chicago|_evanston")) %>%
  walk(function(x) {
    message(x)

    bind_rows(clean_files[grepl(x, names(clean_files))]) %>%
      group_by(district_name) %>%
      mutate(district_num = min(district_num, na.rm = TRUE)) %>%
      mutate(district_num = na_if(district_num, Inf)) %>%
      rename_with(~ gsub("district", x, .x)) %>%
      group_by(year) %>%
      write_partitions_to_s3(
        file.path(output_bucket, x),
        is_spatial = TRUE,
        overwrite = TRUE
      )
  })
