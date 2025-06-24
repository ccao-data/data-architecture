library(arrow)
library(aws.s3)
library(dplyr)
library(glue)
library(purrr)
library(readr)
library(stringr)
library(tools)
library(readxl)
source("utils.R")

# Define S3 root
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "ccao", "zoning")

# === Full file paths with folders ===
township_paths <- c(
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

# Corresponding metadata for each file
township_specs <- tibble::tibble(
  file_path = township_paths,
  file_name = basename(township_paths),
  folder = basename(dirname(township_paths)),
  pin_col = c(
    "Barrington_PIN10", "ElkGrove_PIN10", "Evanston_PIN10",
    "Hanover_PIN10", "PARID", "Maine_PIN10", "NewTrier_PIN10",
    "Niles_PIN10", "Northfield_PIN10", "NorwoodPark_PIN10",
    "PalatineTwp_PIN10", "Schaumburg_PIN10", "Wheeling_PIN10"
  ),
  zone_col = c(
    "Barrington_MunZone", "ElkGrove_MunZone", "Evanston_MunZone",
    "Hanover_MunZone", "Leyden_MunZone", "Maine_MunZone",
    "NewTrier_MunZone", "Niles_MunZone",
    "Northfield_MunZone", "MunZone",
    "PalatineTwp_MunZone", "Schaumburg_MunZone",
    "Wheeling_MunZone"
  ),
  special_case = c(
    TRUE, FALSE, FALSE, FALSE,
    TRUE, FALSE, FALSE, FALSE,
    FALSE, FALSE, FALSE, FALSE,
    FALSE
  )
)

# === Reader function for Excel zoning files ===
read_and_standardize <- function(file_path,
                                 file_name,
                                 folder,
                                 pin_col,
                                 zone_col,
                                 special_case) {
  df <- read_excel(file_path)

  df <- if (file_name == "Leyden.xlsx") {
    df %>%
      mutate(Pin10 = str_sub(!!sym(pin_col), 1, 10)) %>%
      select(Pin10, zoning_code = !!sym(zone_col))
  } else if (special_case) {
    df %>%
      rename(Pin10 = !!sym(pin_col)) %>%
      select(Pin10, zoning_code = !!sym(zone_col))
  } else {
    df %>%
      select(Pin10 = !!sym(pin_col), zoning_code = !!sym(zone_col))
  }

  df %>%
    filter(!is.na(Pin10), !is.na(zoning_code))
}

# === Read all township Excel files ===
township_data <- pmap(township_specs, read_and_standardize)
names(township_data) <- township_specs$folder

# === Read Chicago CSV from unique path and csv ===
Chicago <- read_csv(
  "O:/AndrewGIS/ZoningMaps/ChicagoZoning/ChicagoTriCSV.csv"
) %>%
  select(Pin10 = PIN10, zoning_code = zone_class) %>%
  filter(!is.na(Pin10), !is.na(zoning_code))

township_data$Chicago <- Chicago

# === Upload to S3 with only Pin10 and zoning_code ===
walk2(township_data, names(township_data), function(df, folder_name) {
  output_path <- file.path(
    output_bucket, folder_name, "zoning.parquet"
  )
  write_parquet(df, output_path)
})
