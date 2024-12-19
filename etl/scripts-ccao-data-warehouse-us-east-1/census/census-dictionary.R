library(arrow)
library(aws.s3)
library(dplyr)
library(purrr)
library(stringr)
library(tidycensus)
source("utils.R")

# This script retrieves a dictionary of census variable names
# It populates the warehouse s3 bucket
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "census")

# Retrieve census API key from local .Renviron
tidycensus::census_api_key(key = Sys.getenv("CENSUS_API_KEY"))

##### Tables #####
# Years for which to grab variables
census_acs_years <-
  Sys.getenv("CENSUS_ACS_MIN_YEAR"):Sys.getenv("CENSUS_ACS_MAX_YEAR")
census_acs_tables <- c(
  "Sex by Age" = "B01001",
  "Median Age by Sex" = "B01002",
  "Race" = "B02001",
  "Hispanic or Latino Origin" = "B03003",
  "Geographical Mobility in the Past Year by Sex for Current Residence in the United States" = "B07003", # nolint
  "Household Type" = "B11001",
  "Sex by Educational Attainment" = "B15002",
  "Poverty Status by Sex by Age" = "B17001",
  "Household Income" = "B19001",
  "Median Household Income" = "B19013",
  "Per Capita Income" = "B19301",
  "Receipt of Food Stamps/SNAP by Poverty Status for Households" = "B22003",
  "Employment Status" = "B23025",
  "Tenure" = "B25003",
  "Median Year Structure Built by Tenure" = "B25037",
  "Median Gross Rent (Dollars)" = "B25064",
  "Median Value (Dollars)" = "B25077",
  "Tenure by Selected Physical and Financial Conditions" = "B25123"
)

census_acs_tables_df <- census_acs_tables %>%
  tibble::enframe(
    name = "variable_table_title",
    value = "variable_table_code"
  ) %>%
  mutate(survey = "acs")

# Table defs for PL census files
census_dec_tables <-
  tribble(
    ~"variable_table_code", ~"variable_table_title",
    "H1", "Housing Units",
    "P1", "Race",
    "P2", "Hispanic Or Latino, And Not Hispanic Or Latino By Race",
    "P3", "Race For The Population 18 Years And Over",
    "P4", "Hispanic Or Latino, And Not Hispanic Or Latino By Race For The Population 18 Years And Over", # nolint
    "P5", "Group Quarters Population By Major Group Quarters Type"
  ) %>%
  mutate(survey = "decennial")

# Combine table defs and write to dataset
census_tables <- bind_rows(census_acs_tables_df, census_dec_tables) %>%
  mutate(loaded_at = as.character(Sys.time())) %>%
  group_by(survey) %>%
  select(variable_table_code, variable_table_title, survey)
remote_path_tables <- file.path(output_bucket, "table_dict")
write_dataset(
  dataset = census_tables,
  path = remote_path_tables,
  format = "parquet",
  hive_style = TRUE,
  existing_data_behavior = "overwrite",
  compression = "snappy"
)


##### Variables #####
# Grid of possible year/dataset combos
census_acs_grid <- expand.grid(
  year = census_acs_years,
  dataset = c("acs1", "acs5")
) %>%
  mutate(dataset = as.character(dataset)) %>%
  # Census did not release 2020 acs1 data because of COVID
  filter(year != 2020 | dataset != "acs1")

# Get ALL ACS vars
census_acs_vars <- map2_dfr(
  census_acs_grid$year, census_acs_grid$dataset,
  function(y, d) {
    load_variables(year = y, dataset = d, cache = TRUE) %>%
      mutate(year = y, dataset = d)
  }
)

# Keep only distinct variables that are in the tables of interest
census_vars <- census_acs_vars %>%
  distinct(name, .keep_all = TRUE) %>%
  filter(str_starts(name, paste(census_acs_tables, collapse = "|"))) %>%
  mutate(
    survey = "acs",
    label = str_trim(label),
    table = ifelse(
      str_starts(name, "B22005"),
      str_sub(name, 1, 7),
      str_sub(name, 1, 6)
    )
  ) %>%
  select(
    survey,
    variable_name = name,
    variable_label = label,
    variable_table_code = table
  )

# Get vars for 2020 decennial PL file (2000 and 2010 vars renamed to 2020)
census_dec_vars <- load_variables(2020, "pl", cache = TRUE) %>%
  mutate(
    survey = "decennial",
    label = str_sub(label, 4, -1),
    label = str_trim(str_remove_all(label, ":")),
    table = str_sub(name, 1, 2)
  ) %>%
  select(
    survey,
    variable_name = name,
    variable_label = label,
    variable_table_code = table
  )

# Combine ACS and decennial
census_vars_merged <- bind_rows(census_vars, census_dec_vars) %>%
  mutate(loaded_at = as.character(Sys.time())) %>%
  group_by(survey)

# Write final data to S3
remote_path_variables <- file.path(output_bucket, "variable_dict")
write_dataset(
  dataset = census_vars_merged,
  path = remote_path_variables,
  format = "parquet",
  hive_style = TRUE,
  existing_data_behavior = "overwrite",
  compression = "snappy"
)
