library(arrow)
library(aws.s3)
library(dplyr)
library(purrr)
library(stringr)
library(tidycensus)

# This script retrieves a dictionary of census variable names
# It populates the warehouse s3 bucket
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")

# Retrieve census API key from local .Renviron
tidycensus::census_api_key(key = Sys.getenv("CENSUS_API_KEY"))

# Years for which to grab variables
census_acs_years <- Sys.getenv("CENSUS_ACS_MIN_YEAR"):Sys.getenv("CENSUS_ACS_MAX_YEAR")
census_acs_tables <- c(
  "Sex By Age"                                                    = "B01001",
  "Race"                                                          = "B02001",
  "Household Type"                                                = "B11001",
  "Sex by Marital Status"                                         = "B12001",
  "Sex by Age by Educational Attainment"                          = "B15001",
  "Poverty Status by Sex by Age"                                  = "B17001",
  "Household Income"                                              = "B19001",
  "Median Household Income"                                       = "B19013",
  "Per Capita Income"                                             = "B19301",
  "Receipt of SNAP by Race of Householder (White Alone)"          = "B22005A",
  "Receipt of SNAP by Race of Householder (Black Alone)"          = "B22005B",
  "Receipt of SNAP by Race of Householder (AIAN Alone)"           = "B22005C",
  "Receipt of SNAP by Race of Householder (Asian Alone)"          = "B22005D",
  "Receipt of SNAP by Race of Householder (NHPI Alone)"           = "B22005E",
  "Receipt of SNAP by Race of Householder (Other Alone)"          = "B22005F",
  "Receipt of SNAP by Race of Householder (Two or More)"          = "B22005G",
  "Receipt of SNAP by Race of Householder (WA, NHis)"             = "B22005H",
  "Receipt of SNAP by Race of Householder (His/Lat)"              = "B22005I",
  "Sex by Age by Employment Status"                               = "B23001",
  "Tenure"                                                        = "B25003"
)

# Grid of possible year/dataset combos
census_acs_grid <- expand.grid(
  year = census_acs_years,
  dataset = c("acs1", "acs5")
)

# Get ALL ACS vars
census_acs_vars <- map2_dfr(
  census_acs_grid$year, census_acs_grid$dataset,
  function(y, d) load_variables(year = y, dataset = d, cache = TRUE) %>%
    mutate(year = y, dataset = d)
)

# Keep only distinct variables that are in the tables of interest
census_vars <- census_acs_vars %>%
  distinct(name, .keep_all = TRUE) %>%
  filter(str_starts(name, paste(census_acs_tables, collapse = "|"))) %>%
  mutate(
    survey = "acs",
    label = str_trim(label),
    source_table_name = ifelse(
      str_starts(name, "B22005"),
      str_sub(name, 1, 7),
      str_sub(name, 1, 6)
    )
  ) %>%
  left_join(
    tibble::enframe(census_acs_tables) %>% rename(source_table_label = name),
    by = c("source_table_name" = "value")
  ) %>%
  select(
    survey, variable_name = name, variable_label = label,
    source_table_name, source_table_label
  )

# Table defs for PL census files
census_dec_tables <-
  tribble(
    ~ "source_table_name", ~ "source_table_label",
    "H1", "Housing Units",
    "P1", "Race",
    "P2", "Hispanic Or Latino, And Not Hispanic Or Latino By Race",
    "P3", "Race For The Population 18 Years And Over",
    "P4", "Hispanic Or Latino, And Not Hispanic Or Latino By Race For The Population 18 Years And Over",
    "P5", "Group Quarters Population By Major Group Quarters Type"
  )

# Get vars for 2020 decennial PL file (2000 and 2010 vars renamed to 2020)
census_dec_vars <- load_variables(2020, "pl", cache = TRUE) %>%
  mutate(
    survey = "decennial",
    label = str_sub(label, 4, -1),
    label = str_trim(str_remove_all(label, ":"))
  ) %>%
  select(survey, variable_name = name, variable_label = label) %>%
  mutate(source_table_name = str_sub(variable_name, 1, 2)) %>%
  left_join(census_dec_tables, by = "source_table_name")

# Combine ACS and decennial
census_vars_merged <- bind_rows(census_vars, census_dec_vars) %>%
  group_by(survey)

# Write final data to S3
remote_path <- file.path(AWS_S3_WAREHOUSE_BUCKET, "census", "dictionary")
write_dataset(
  dataset = census_vars_merged,
  path = remote_path,
  format = "parquet",
  hive_style = TRUE,
  existing_data_behavior = "overwrite",
  compression = "snappy"
)
