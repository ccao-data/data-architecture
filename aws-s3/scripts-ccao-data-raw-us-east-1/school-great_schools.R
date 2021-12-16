library(httr)
library(arrow)
library(dplyr)
library(zipcodeR)
library(aws.s3)

# This script retrieves data including ratings from greatschools.org
API_KEY <- Sys.getenv("GREAT_SCHOOLS_API_KEY")
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
remote_file <- file.path(
  AWS_S3_RAW_BUCKET,
  "school",
  "great_schools",
  paste0(format(Sys.Date(), "%Y"), ".parquet")
)

# We'll loop over Cook County zip codes to get all schools, since we can only grab 50 at a time
cook_zips <- zipcodeR::search_county("Cook", "IL") %>%
  pull(zipcode) %>%
  expand.grid(zip = ., "page" = 0:1)

# Function to loop over zip codes and retrieve data
gather_schools <- function(zipcode, api_key) {
  req <- httr::GET(
    paste0(
      "https://gs-api.greatschools.org/schools?zip=", zipcode[1],
      "&page=", zipcode[2],
      "&limit=50"
    ),
    add_headers("X-API-Key" = API_KEY)
  )

  return(
    httr::content(req, as = "parsed")
  )
}

# Apply function to zip codes
great_schools <- apply(cook_zips, 1, gather_schools, api_key = API_KEY)

output <- list()

# Condense data into dataframe, limit to Cook County FIPS
for (i in 1:length(great_schools)) {
  output[[i]] <- great_schools[[i]]$schools
}

output <- bind_rows(output) %>%
  dplyr::filter(fipscounty == "17031")

# Write data if it does not already exist
if (!aws.s3::object_exists(remote_file)) {
  write_parquet(output, remote_file)
}
