# This script cleans and combines raw condo characteristics data for the
# warehouse
library(arrow)
library(aws.s3)
library(DBI)
library(data.table)
library(dplyr)
library(glue)
library(noctua)
library(purrr)
library(stringr)
library(tidyr)
source("utils.R")

# Declare raw and clean condo data locations
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "ccao", "condominium", "pin_condo_char"
)

# Connect to Athena
AWS_ATHENA_CONN_NOCTUA <- dbConnect(noctua::athena())

# Get S3 file addresses
files <- grep(
  ".parquet",
  file.path(
    AWS_S3_RAW_BUCKET,
    aws.s3::get_bucket_df(
      AWS_S3_RAW_BUCKET,
      prefix = "ccao/condominium/pin_condo_char/"
    )$Key
  ),
  value = TRUE
)

# Grab sales/spatial data
classes <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA, "
  SELECT DISTINCT
      parid AS pin,
      class
  FROM iasworld.pardat
  WHERE taxyr = (SELECT MAX(taxyr) FROM iasworld.pardat)
      AND class IN ('299', '399')
  "
)

# Grab all years of previously assembled condo data already present on Athena
years <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA, "
  SELECT DISTINCT year FROM ccao.pin_condo_char
  "
) %>%
  pull(year)

# Function to grab chars data from Athena if it's already available
athena_chars <- function(x) {
  dbGetQuery(
    conn = AWS_ATHENA_CONN_NOCTUA, glue("
  SELECT * FROM ccao.pin_condo_char
  WHERE year = '{x}'
  ")
  )
}

# A place to store characteristics data so we can stack it
chars <- list()

# Determine if original condo data needs to be processed or not. This data was
# collected in 2021-2023. We have also been updating and correcting this data
# post-2023 - this process is handled below (updates).

# We use tax year, valuations uses year the work was done
for (i in c("2021", "2022", "2023")) {
  if (!("2021" %in% years) && i == "2021") {
    # If clean 2021 data is not already in Athena, load and clean it
    chars[[i]] <- map(
      grep("2022", files, value = TRUE), function(x) {
        read_parquet(x) %>%
          tibble(.name_repair = "unique") %>%
          rename_with(~ tolower(.x)) %>%
          mutate(pin = str_pad(parid, 14, side = "left", pad = "0")) %>%
          select(contains(c("pin", "sqft", "bed", "source"))) %>%
          select(-contains(c("x", "all", "search"))) %>%
          rename_with(~"bedrooms", contains("bed")) %>%
          rename_with(~"unit_sf", contains("unit")) %>%
          rename_with(~"building_sf", contains("building"))
      }
    ) %>%
      rbindlist(fill = TRUE) %>%
      inner_join(classes) %>%
      mutate(across(c(unit_sf, building_sf), ~ na_if(., "0"))) %>%
      mutate(across(c(unit_sf, building_sf), ~ na_if(., "1"))) %>%
      mutate(
        across(c(building_sf, unit_sf, bedrooms), ~ gsub("[^0-9.-]", "", .))
      ) %>%
      mutate(across(.cols = everything(), ~ trimws(., which = "both"))) %>%
      na_if("") %>%
      mutate(
        bedrooms = case_when(
          is.na(unit_sf) & bedrooms == "0" ~ NA_character_,
          TRUE ~ bedrooms
        )
      ) %>%
      mutate(across(c(building_sf, unit_sf, bedrooms), ~ as.numeric(.))) %>%
      mutate(
        bedrooms = ceiling(bedrooms),
        parking_pin = str_detect(source, "(?i)parking|garage") &
          is.na(unit_sf) & is.na(building_sf),
        year = "2021"
      ) %>%
      select(-c(class, source)) %>%
      # These are obvious typos
      mutate(unit_sf = case_when(
        unit_sf == 28002000 ~ 2800,
        unit_sf == 20002800 ~ 2000,
        unit_sf == 182901 ~ 1829,
        TRUE ~ unit_sf
      ))
  } else if (!("2022" %in% years) && i == "2022") {
    # If clean 2022 data is not already in Athena, load and clean it
    chars[[i]] <- lapply(grep("2023", files, value = TRUE), function(x) {
      raw <- read_parquet(x)[, 1:20]

      names <- tolower(names(raw))
      names(raw) <- make.unique(names)

      raw %>%
        select(!contains("pin")) %>%
        rename_with(~ str_replace(.x, "iasworold", "iasworld")) %>%
        mutate(pin = str_pad(iasworld_parid, 14, side = "left", pad = "0")) %>%
        rename_with(~ str_replace_all(.x, "[[:space:]]", "")) %>%
        rename_with(~ str_replace_all(.x, "\\.{4}", "")) %>%
        select(!contains(c("1", "2", "all"))) %>%
        select(contains(c("pin", "sq", "bed", "bath"))) %>%
        rename_with(~"bedrooms", contains("bed")) %>%
        rename_with(~"unit_sf", contains("unit")) %>%
        rename_with(~"building_sf", contains(c("building", "bldg"))) %>%
        rename_with(~"half_baths", contains("half")) %>%
        rename_with(~"full_baths", contains("full")) %>%
        mutate(
          across(!contains("pin"), as.numeric),
          year = "2022",
          # Define a parking pin as a unit with only 0 or NA values for
          # characteristics
          parking_pin = case_when(
            (bedrooms == 0 | unit_sf == 0) &
              rowSums(
                across(c(unit_sf, bedrooms, full_baths, half_baths)),
                na.rm = TRUE
              ) == 0 ~ TRUE,
            TRUE ~ FALSE
          ),
          # Really low unit_sf should be considered NA
          unit_sf = case_when(
            unit_sf < 5 & !parking_pin ~ NA_real_,
            TRUE ~ unit_sf
          ),
          # Assume missing half_baths value is 0 if there is full bathroom data
          # for PIN
          half_baths = case_when(
            is.na(half_baths) & !is.na(full_baths) & full_baths > 0 ~ 0,
            TRUE ~ half_baths
          ),
          # Make beds and baths are integers
          across(c(half_baths, full_baths, bedrooms), ~ ceiling(.x)),
          # Set all characteristics to NA for parking pins
          across(
            c(bedrooms, unit_sf, half_baths, full_baths),
            ~ ifelse(parking_pin, NA, .x)
          )
        )
    }) %>%
      bind_rows() %>%
      group_by(pin) %>%
      arrange(unit_sf) %>%
      filter(row_number() == 1) %>%
      ungroup() %>%
      filter(!is.na(pin))
  } else if (!("2023" %in% years) && i == "2023") {
    chars[[i]] <- lapply(grep("2024", files, value = TRUE), function(x) {
      read_parquet(x) %>%
        select(
          pin = "14.Digit.PIN",
          building_sf = "Building.Square.Footage",
          unit_sf = "Unit.Square.Footage",
          bedrooms = "Bedrooms",
          parking_pin = "Parking.Space.Change",
          full_baths = "Full.Baths",
          half_baths = "Half.Baths"
        ) %>%
        mutate(
          pin = gsub("[^0-9]", "", pin),
          parking_pin = if_all(
            c(unit_sf, bedrooms, full_baths, half_baths), is.na
          ) & !is.na(parking_pin),
          year = "2023",
          bedrooms = case_when(bedrooms > 15 ~ NA_real_, TRUE ~ bedrooms),
          full_baths = case_when(full_baths > 10 ~ NA_real_, TRUE ~ full_baths),
          unit_sf = case_when(unit_sf < 5 ~ NA_real_, TRUE ~ unit_sf)
        )
    }) %>%
      bind_rows()
  } else {
    # If data is already in Athena, just take it from there
    chars[[i]] <- athena_chars(i)
  }
}

# At the end of 2024 and 2025 (tax years 2025 and 2026) valuations revisited
# some old condos and updated their characteristics
updates <- map(
  hug <- file.path(
    "s3://ccao-data-raw-us-east-1",
    aws.s3::get_bucket_df(
      AWS_S3_RAW_BUCKET,
      prefix = "ccao/condominium/pin_condo_char"
    ) %>%
      filter(str_detect(Key, "2025|2026")) %>%
      pull(Key)
  ),
  \(x) {
    read_parquet(x) %>%
      mutate(across(.cols = everything(), as.character))
  }
) %>%
  bind_rows() %>%
  rename_with(~ gsub("\\.", "_", tolower(.x)), .cols = everything()) %>%
  select("pin", starts_with("new")) %>%
  mutate(
    pin = gsub("-", "", pin),
    across(starts_with("new"), as.numeric),
    # Three units with 100 for unit sqft
    new_unit_sf = ifelse(new_unit_sf == 100, 1000, new_unit_sf)
  ) %>%
  filter(!if_all(starts_with("new"), is.na))

# Update parcels with new column values
chars <- chars %>%
  bind_rows() %>%
  left_join(updates, by = "pin") %>%
  mutate(
    building_sf = coalesce(new_building_sf, building_sf),
    unit_sf = coalesce(new_unit_sf, unit_sf),
    bedrooms = coalesce(new_bedrooms, bedrooms),
    full_baths = coalesce(new_full_baths, full_baths),
    half_baths = coalesce(new_half_baths, half_baths)
  ) %>%
  select(!starts_with("new"))

# Upload cleaned data to S3
chars %>%
  mutate(loaded_at = as.character(Sys.time())) %>%
  group_by(year) %>%
  arrow::write_dataset(
    path = output_bucket,
    format = "parquet",
    hive_style = TRUE,
    compression = "snappy"
  )
