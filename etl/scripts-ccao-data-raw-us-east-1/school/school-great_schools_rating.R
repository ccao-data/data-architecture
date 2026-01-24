library(arrow)
library(aws.s3)
library(dplyr)
library(httr)
library(purrr)
library(tidyr)
library(zipcodeR)
source("utils.R")

# This script retrieves data including ratings from greatschools.org
# Documentation available here:
# https://docs.google.com/document/d/1pSe1AeZXGL01m5uG3wwRr9k4pI2Qw52xzPi2NmNrhyI/edit # nolint

################################################################################
# We no longer have access to this data and this script should not be run unless
# we actively restore access to new Great Schools data.
################################################################################

GREAT_SCHOOLS_API_KEY <- Sys.getenv("GREAT_SCHOOLS_API_KEY")
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "school", "great_schools_rating")

# We'll loop over Cook County zip codes to get all schools,
# since we can only grab 50 at a time
cook_zips <- zipcodeR::search_county("Cook", "IL") %>%
  pull(zipcode) %>%
  expand.grid(zip = ., "page" = 0:1)

# Function to loop over zip codes and retrieve data
gather_schools <- function(zipcode, page, api_key) {
  req <- httr::GET(
    paste0(
      "https://gs-api.greatschools.org/schools?zip=", zipcode,
      "&page=", page,
      "&limit=50"
    ),
    add_headers("X-API-Key" = api_key)
  )

  httr::content(req, as = "parsed")$schools
}

# Apply function to zip codes
great_schools <- pmap_dfr(cook_zips, function(...) {
  df <- tibble::tibble(...)
  gather_schools(
    zipcode = df$zip,
    page = df$page,
    api_key = GREAT_SCHOOLS_API_KEY
  )
})

great_schools_fil <- bind_rows(great_schools) %>%
  dplyr::filter(fipscounty == "17031") %>%
  rename(rating_year = year) %>%
  fill(rating_year, .direction = "downup")

# Write data if it does not already exist
# Files are written to rating year + 1 since ratings correspond to the
# in-progress school year (i.e. 2020 rating year is the 2020-2021 school year)
remote_file <- file.path(
  output_bucket,
  paste0(unique(great_schools_fil$rating_year) + 1, ".parquet")
)
if (!aws.s3::object_exists(remote_file)) {
  write_parquet(great_schools_fil, remote_file)
}
