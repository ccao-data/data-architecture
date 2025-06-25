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
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "ccao", "other", "zoning")

township_paths <- c(
  "O:/CCAODATA/zoning/data/BarringtonTwp.xlsx",
  "O:/CCAODATA/zoning/data/ElkGroveTwpZoning.xlsx",
  "O:/CCAODATA/zoning/data/evanstontw317.xlsx",
  "O:/CCAODATA/zoning/data/HanoverZoning.xlsx",
  "O:/CCAODATA/zoning/data/Leyden.xlsx",
  "O:/CCAODATA/zoning/data/MaineZoning.xlsx",
  "O:/CCAODATA/zoning/data/NewTrierZoning.xlsx",
  "O:/CCAODATA/zoning/data/Niles.xlsx",
  "O:/CCAODATA/zoning/data/NorthfieldZoning.xlsx",
  "O:/CCAODATA/zoning/data/norwoodpark313.xlsx",
  "O:/CCAODATA/zoning/data/PalatineTwp.xlsx",
  "O:/CCAODATA/zoning/data/SchaumburgTwp.xlsx",
  "O:/CCAODATA/zoning/data/Wheeling.xlsx",
  "O:/CCAODATA/zoning/data/ChicagoTriCSV.csv"
)

# === Metadata for each file ===
township_specs <- tibble::tibble(
  file_path = township_paths,
  file_name = basename(township_paths),
  folder = basename(dirname(township_paths)),
  pin_col = c(
    "Barrington_PIN10", "ElkGrove_PIN10", "Evanston_PIN10",
    "Hanover_PIN10", "PARID", "Maine_PIN10", "NewTrier_PIN10",
    "Niles_PIN10", "Northfield_PIN10", "NorwoodPark_PIN10",
    "PalatineTwp_PIN10", "Schaumburg_PIN10", "Wheeling_PIN10",
    "Pin10"
  ),
  pin14_col = c(
    "PARID", "PARID", "PARID", "PARID", "PARID", "PARID", "PARID",
    "PARID", "PARID", "PIN14", "PARID", "PARID", "PARID", "PIN14"
  ),
  zone_col = c(
    "Barrington_MunZone", "ElkGrove_MunZone", "Evanston_MunZone",
    "Hanover_MunZone", "Leyden_MunZone", "Maine_MunZone",
    "NewTrier_MunZone", "Niles_MunZone", "Northfield_MunZone", "MunZone",
    "PalatineTwp_MunZone", "Schaumburg_MunZone", "Wheeling_MunZone",
    "zone_class"
  ),
  special_case = c(
    TRUE, FALSE, FALSE, FALSE,
    TRUE, FALSE, FALSE, FALSE,
    FALSE, FALSE, FALSE, FALSE,
    FALSE, FALSE
  )
)

# === Reader function for Excel and CSV files ===
read_and_standardize <- function(file_path,
                                 file_name,
                                 folder,
                                 pin_col,
                                 pin14_col,
                                 zone_col,
                                 special_case) {
  # Read based on file extension
  df <- if (tolower(file_ext(file_path)) == "csv") {
    read_csv(file_path, col_types = cols(.default = "c"))
  } else {
    read_excel(file_path)
  }

  # Apply custom logic
  df <- if (file_name == "Leyden.xlsx") {
    df %>%
      mutate(
        pin10 = str_sub(!!sym(pin_col), 1, 10),
        pin = !!sym(pin14_col)
      ) %>%
      select(pin10, pin, zoning_code = !!sym(zone_col))
  } else if (special_case) {
    df %>%
      rename(pin10 = !!sym(pin_col)) %>%
      mutate(pin = !!sym(pin14_col)) %>%
      select(pin10, pin, zoning_code = !!sym(zone_col))
  } else {
    df %>%
      transmute(
        pin10 = !!sym(pin_col),
        pin = !!sym(pin14_col),
        zoning_code = !!sym(zone_col)
      )
  }

  df %>%
    mutate(
      pin10 = as.character(pin10),
      pin = as.character(pin),
      zoning_code = as.character(zoning_code)
    ) %>%
    filter(!is.na(pin10), !is.na(zoning_code))
}

# === Read all township zoning datasets ===
township_data <- pmap(township_specs, read_and_standardize)

# === Combine and write one file ===
zoning <- bind_rows(township_data) %>%
  distinct(pin10, zoning_code, .keep_all = TRUE) %>%
  mutate(year = "2025")

output_path <- file.path(output_bucket, "zoning.parquet")
write_parquet(zoning, output_path)
