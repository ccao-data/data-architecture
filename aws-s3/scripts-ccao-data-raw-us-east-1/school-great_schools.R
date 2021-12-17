library(arrow)
library(aws.s3)
library(dplyr)
library(httr)
library(purrr)
library(zipcodeR)

# This script retrieves data including ratings from greatschools.org
GREAT_SCHOOLS_API_KEY <- Sys.getenv("GREAT_SCHOOLS_API_KEY")
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
remote_file <- file.path(
  AWS_S3_RAW_BUCKET, "school", "great_schools",
  paste0(format(Sys.Date(), "%Y"), ".parquet")
)

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

output <- bind_rows(great_schools) %>%
  dplyr::filter(fipscounty == "17031")

# Write data if it does not already exist
if (!aws.s3::object_exists(remote_file)) {
  write_parquet(output, remote_file)
}

# Cleanup
rm(list = ls())