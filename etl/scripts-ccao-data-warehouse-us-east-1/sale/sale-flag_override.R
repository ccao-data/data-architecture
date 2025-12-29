library(readxl)
library(janitor)
library(dplyr)
library(arrow)
library(aws.s3)
source("utils.R")

# TODO: Standardize and rework filernames
# TODO: Refactor ingest
# TODO: Rework mutates into a function


# These files are filled out by valuations. We then place them in the O drive
# for ingest
res_characteristic_potentials_reviewed <-
  read_excel(
    "O:/CCAODATA/data/sale/res_characteristic_potentials_reviewed.xlsx" # nolint
  ) %>%
  janitor::clean_names() %>%
  rename(
    sale_is_arms_length = sale_is_arm_s_length,
    doc_no = sale_doc_no
  )

res_characteristic_potentials_round_2_reviewed <-
  read_excel(
    "O:/CCAODATA/data/sale/res_characteristic_potentials_round_2_reviewed.xlsx" # nolint
  ) %>%
  janitor::clean_names() %>%
  rename(
    sale_is_arms_length = sale_is_arm_s_length,
    doc_no = sale_doc_no
  )

res_characteristic_potentials_reviewed <-
  res_characteristic_potentials_reviewed %>%
  mutate(
    is_valid_for_modeling =
      !(
        coalesce(sale_is_arms_length == "NO", FALSE) |
          coalesce(flips == "YES", FALSE) |
          coalesce(grepl("YES", class_change, ignore.case = TRUE), FALSE) |
          coalesce(grepl("YES", field_check, ignore.case = TRUE), FALSE)
      )
  )

res_characteristic_potentials_round_2_reviewed <-
  res_characteristic_potentials_round_2_reviewed |>
  mutate(
    # TODO: Decide if we want to continue to use field_check, might make sense to just
    # let char change do the heavy lifting
    is_valid_for_modeling =
      !(
        coalesce(sale_is_arms_length == "NO", FALSE) |
          coalesce(flip == "YES", FALSE) |
          coalesce(grepl("YES", class_change, ignore.case = TRUE), FALSE) |
          # Regex statement explained:
          #   YES - string match
          #   [- ]? - matches a space, a hyphen, and doesn't disqualift omission (YES-MAJOR, YES MAJOR, YESMAJOR)
          #   MAJ - string match
          #   [OA] - matches O or A
          #   [REOT] - matches R, E, O, or T (we see data like 'YES-MAJOE' and 'YES-MAJOE')
          coalesce(
            grepl("YES[- ]?MAJ[OA][REOT]", characteristic_change, ignore.case = TRUE),
            FALSE
          )
      )
  )

data_to_write <- rbind(
  res_characteristic_potentials_reviewed %>% select(doc_no, is_valid_for_modeling, flagged_as_outlier_sale),
  res_characteristic_potentials_round_2_reviewed %>% select(doc_no, is_valid_for_modeling, flagged_as_outlier_sale)
)

out_uri <- "s3://ccao-data-warehouse-dev-us-east-1/z_dev_miwagne_sale/flag_override/res_characteristic_potentials_reviewed.parquet" # nolint

tmp_file <- tempfile(fileext = ".parquet")
arrow::write_parquet(
  data_to_write,
  tmp_file,
  compression = "snappy"
)

save_local_to_s3(
  s3_uri = out_uri,
  path = tmp_file,
  overwrite = FALSE
)

unlink(tmp_file)
