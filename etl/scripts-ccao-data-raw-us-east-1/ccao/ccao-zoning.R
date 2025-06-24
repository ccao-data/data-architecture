library(arrow)
library(aws.s3)
library(dplyr)
library(glue)
library(purrr)
library(readr)
library(tools)
source("utils.R")

# This script retrieves zoning from the CCAO's O Drive
# THIS SOURCE WILL NEED TO BE UPDATED
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "ccao", "zoning")

# Chicago has it's own zoning structure so it's not formatted as a township
Chicago <- read.csv(
  "O:/AndrewGIS/ZoningMaps/ChicagoZoning/ChicagoTriCSV.csv"
) %>%
  select(Pin10, zoning_code = zone_class, township = GIS_TWP) %>%
  # select current year since upload dates are not consistently formatted
  mutate(year = format(Sys.Date(), "%Y"))

# We can't download all xlsx files since there are some duplicates.
# This is also on North Tri. He will send information when he does South.
other_townships <- c(
  "O:/AndrewGIS/ZoningMaps/TownshipZoning/Barrington/BarringtonTwp.xlsx",
  "O:/AndrewGIS/ZoningMaps/TownshipZoning/ElkGrove/ElkGroveTwpZoning.xlsx",
  "O:/AndrewGIS/ZoningMaps/TownshipZoning/Evanston/evanstontw317.xlsx",
  "O:/AndrewGIS/ZoningMaps/TownshipZoning/Hanover/HanoverZoning.xlsx",
  "O:/AndrewGIS/ZoningMaps/TownshipZoning/Lyons/LyonsTwpZoning.xlsx",
  "O:/AndrewGIS/ZoningMaps/TownshipZoning/Maine/MaineZoning.xlsx",
  "O:/AndrewGIS/ZoningMaps/TownshipZoning/NewTrier/NewTrierZoning.xlsx",
  "O:/AndrewGIS/ZoningMaps/TownshipZoning/Niles/Niles.xlsx",
  "O:/AndrewGIS/ZoningMaps/TownshipZoning/Northfield/NorthfieldZoning.xlsx",
  "O:/AndrewGIS/ZoningMaps/TownshipZoning/NorwoodPark/norwoodpark313.xlsx",
  "O:/AndrewGIS/ZoningMaps/TownshipZoning/Palatine/PalatineTwp.xlsx",
  "O:/AndrewGIS/ZoningMaps/TownshipZoning/Schaumburg/SchaumburgTwp.xlsx",
  "O:/AndrewGIS/ZoningMaps/TownshipZoning/Wheeling/Wheeling.xlsx"
)

# Read all Excel files into a named list
township_data <- map(other_townships, read_excel)


# Function to retrieve data and write to S3
read_write <- function(x) {
  output_dest <- file.path(output_bucket, glue(parse_number(x), ".parquet"))

  if (!object_exists(output_dest)) {
    print(output_dest)

    readr::read_delim(
      glue("O:/CCAODATA/data/foreclosures/", x),
      delim = ","
    ) %>%
      write_parquet(output_dest)
  }
}

# Apply function to foreclosure data
walk(files, read_write)
