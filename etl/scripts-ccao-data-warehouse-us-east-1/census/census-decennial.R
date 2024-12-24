library(arrow)
library(aws.s3)
library(dplyr)
library(purrr)
library(stringr)
library(tidycensus)
source("utils.R")

# This script retrieves raw decennial census data for the data lake
# It populates the warehouse s3 bucket
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "census")

# Retrieve census API key from local .Renviron
tidycensus::census_api_key(key = Sys.getenv("CENSUS_API_KEY"))

# Declare years we'd like to grab census data for
census_years <- c(2000, 2010, 2020)

# All decennial census variables we're looking to grab
# Var names differ between years to we need a df to translate them
census_variables_2000 <- load_variables(2000, "pl", cache = TRUE)
census_variables_2010 <- load_variables(2010, "pl", cache = TRUE)
census_variables_2020 <- load_variables(2020, "pl", cache = TRUE)
census_variables_df <- census_variables_2020 %>%
  mutate(
    name_2010 = str_replace_all(name, "P", "P00"),
    name_2010 = str_replace_all(name_2010, "H", "H00"),
    name_2010 = str_remove_all(name_2010, "_"),
    name_2010 = str_remove_all(name_2010, "N")
  ) %>%
  select(name, name_2010, label) %>%
  left_join(
    census_variables_2010 %>% select(-label, -concept) %>% mutate(temp = 1),
    by = c("name_2010" = "name")
  ) %>%
  left_join(
    census_variables_2000 %>%
      select(-label, -concept, name_2000 = name) %>%
      mutate(name_2010 = str_replace_all(name_2000, "PL", "P")),
    by = c("name_2010")
  ) %>%
  rename(name_2020 = name) %>%
  mutate(name_2010 = ifelse(temp != 1, NA, name_2010)) %>%
  select(name_2020, name_2010, name_2000, label)

# Declare geographies we'd like to query
census_geographies <- c(
  "county",
  "tract",
  "block"
)

# Folders have a different naming schema than geographies
folders <- c(
  "county",
  "tract",
  "block"
)

# Link geographies and folders
folders_df <- data.frame(geography = census_geographies, folder = folders)

# Generate a combination of all years, geographies, and tables
all_combos <- expand.grid(
  geography = census_geographies,
  year = census_years,
  survey = "decennial",
  stringsAsFactors = FALSE
) %>%
  left_join(folders_df)

# Function to loop through rows in all_combos, grab census data,
# and write it to a parquet file on S3 if it doesn't already exist
pull_and_write_dec <- function(s3_bucket_uri, survey, folder, geography, year) {
  remote_file <- file.path(
    output_bucket, survey,
    paste0("geography=", folder),
    paste0("year=", year),
    paste(survey, folder, year, "pl.parquet", sep = "-")
  )

  # Check to see if file already exists on S3; if it does, skip it
  if (!aws.s3::object_exists(remote_file)) {
    # Print file being written
    message(Sys.time(), " - ", remote_file)

    # Get variables for the specific year of interest
    vars <- census_variables_df %>%
      pull(paste0("name_", year)) %>%
      .[!is.na(.)]

    # Rename 2000 and 2010 vars to 2020 names
    rename_to_2020 <- function(col_name, year) {
      census_variables_df$name_2020[match(
        col_name,
        census_variables_df[[paste0("name_", year)]]
      )]
    }

    # Retrieve specified data from census API
    df <- get_decennial(
      geography = geography,
      variables = vars,
      output = "wide",
      state = "IL",
      county = "Cook",
      year = as.numeric(year),
      sumfile = "pl",
      cache_table = TRUE
    ) %>%
      select(-NAME) %>%
      rename_with(~ rename_to_2020(.x, year), .cols = !GEOID) %>%
      mutate(loaded_at = as.character(Sys.time()))

    # Write to S3
    arrow::write_parquet(df, remote_file)
  }
}

# Apply function to all_combos
pwalk(all_combos, function(...) {
  df <- tibble::tibble(...)
  pull_and_write_dec(
    s3_bucket_uri = output_bucket,
    survey = df$survey,
    folder = df$folder,
    geography = df$geography,
    year = df$year
  )
})
