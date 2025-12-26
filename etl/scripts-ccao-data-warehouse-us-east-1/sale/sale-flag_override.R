library(readxl)
library(janitor)
library(dplyr)
library(arrow)
library(aws.s3)
source("utils.R")

df <-
  read_excel(
    "O:/CCAODATA/data/sale/res_characteristic_potentials_reviewed.xlsx" # nolint
  ) %>%
  janitor::clean_names() %>%
  rename(
    sale_is_arms_length = sale_is_arm_s_length,
    doc_no = sale_doc_no
  )

df <- df %>%
  mutate(
    exclude_sale =
      coalesce(
        sale_is_arms_length == "NO", FALSE
      ) |
        coalesce(flips == "YES", FALSE) | # nolint
        coalesce(grepl("YES", class_change, ignore.case = TRUE), FALSE) | # nolint
        coalesce(grepl("YES", field_check, ignore.case = TRUE), FALSE) # nolint
  )

out_uri <- "s3://ccao-data-warehouse-dev-us-east-1/z_dev_miwagne_sale/flag_override/res_characteristic_potentials_reviewed.parquet" # nolint

tmp_file <- tempfile(fileext = ".parquet")
arrow::write_parquet(
  df %>% select(doc_no, exclude_sale),
  tmp_file,
  compression = "snappy"
)

save_local_to_s3(
  s3_uri = out_uri,
  path = tmp_file,
  overwrite = FALSE
)

unlink(tmp_file)
