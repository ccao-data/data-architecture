library(readxl)
library(dplyr)
library(aws.s3)
source("utils.R")
library(tools)

# TODO: Standardize and rework filernames
# TODO: Refactor ingest
# TODO: Rework mutates into a function


# Source directory with Excel files, provided by valuations
src_dir <- "O:/CCAODATA/data/sale"

# Output dir
s3_dir <- "s3://ccao-data-raw-us-east-1/sale/flag_override/"

# List Excel files
excel_files <- list.files(
  path = src_dir,
  pattern = "\\.xlsx$",
  full.names = TRUE
)

# Read each Excel file and immediately write to Parquet
for (f in excel_files) {
  base_name <- file_path_sans_ext(basename(f))

  df <- read_excel(f)

  write_parquet(
    df,
    file.path(s3_dir, paste0(base_name, ".parquet"))
  )
}
