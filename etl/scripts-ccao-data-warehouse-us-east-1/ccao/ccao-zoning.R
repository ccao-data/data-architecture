# Prevent scientific notation in console and I/O
options(scipen = 999)

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

# === File paths ===
# Kept as list to ensure that they align with the following function
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
  "O:/CCAODATA/zoning/data/ChicagoTri.xlsx"
)

# === Metadata for each file ===
township_specs <- tibble::tibble(
  file_path = township_paths,
  file_name = basename(township_paths),
  folder = basename(dirname(township_paths)),
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
  )
)

# === Reader function for Excel files only ===
read_and_standardize <- function(file_path,
                                 file_name,
                                 folder,
                                 pin14_col,
                                 zone_col) {
  df <- read_excel(file_path) %>%
    mutate(across(everything(), as.character))

  df %>%
    select(pin = matches(c("PIN14", "PARID")), zoning_code = !!sym(zone_col)) %>%
    # Drop observations without a zoning code
    filter(!is.na(pin), !is.na(zoning_code))
}

# === Read and standardize all zoning files ===
township_data <- pmap(township_specs, read_and_standardize)

# === Combine all data and write one Parquet file ===
zoning <- bind_rows(township_data) %>%
  distinct(pin, zoning_code, .keep_all = TRUE) %>%
  group_by(pin) %>%
  summarise(
    # A small number of observations have two zoning codes
    # due to fuzzy geo-spatial techniques
    zoning_code = paste(unique(zoning_code), collapse = ", "),
    .groups = "drop"
  ) %>%
  # Created year since data is downloaded from different data sources.
  mutate(year = "2025") %>%
  group_by(year)

write_partitions_to_s3(
  df = zoning,
  s3_output_path = output_bucket,
  is_spatial = FALSE,
  overwrite = TRUE
)
