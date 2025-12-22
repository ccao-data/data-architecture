library(readxl)
library(janitor)
library(dplyr)
library(arrow)
library(aws.s3)

df <-
  read_excel(
    here::here(
      "scripts-ccao-data-warehouse-us-east-1/sale/res_characteristic_potentials_reviewed.xlsx" # nonlint
    )
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
      sale_is_arms_length == "NO", FALSE) |
      coalesce(flips == "YES", FALSE) |
      coalesce(grepl("YES", class_change, ignore.case = TRUE), FALSE) |
      coalesce(grepl("YES", field_check, ignore.case = TRUE), FALSE)
  )

write_parquet(
  df %>% select(doc_no, exclude_sale),
  "s3://ccao-data-warehouse-dev-us-east-1/z_dev_miwagne_sale/flag_override/res_characteristic_potentials_reviewed.parquet" # nolint
)
