library(arrow)
library(ccao)
library(DBI)
library(data.table)
library(noctua)
library(dplyr)
library(openxlsx)
library(readr)
library(tidyverse)
library(stringr)
library(varhandle)

# Define known character columns
char_cols <- c(
  "keypin",
  "pins",
  "township",
  "class(es)",
  "address",
  "propertyuse",
  "investmentrating",
  "f/r",
  "carwash?",
  "ceilingheight",
  "2023permit/partial/demovalue",
  "file",
  "sheet"
)

# Compile a filtered list of excel workbooks and worksheets to ingest ----
list.files(
  "G:/1st Pass spreadsheets",
  pattern = "[0-9]{4} Valuation Models",
  full.names = TRUE
) %>%
  file.path("PublicVersions") %>%
  list.files(ignore.case = TRUE, full.names = TRUE) %>%
  grep(pattern = "Past|xlsx", invert = TRUE, value = TRUE) %>%
  list.files(pattern = ".xlsx", full.names = TRUE) %>%
  map(~ crossing(sheet = getSheetNames(.x), file = .x), .progress = TRUE) %>%
  bind_rows() %>%
  filter(
    str_detect(file, "Hard|Copy", negate = TRUE),
    str_detect(sheet, "Summary", negate = TRUE)
  ) %>%

  # Ingest the sheets, clean, and bind them ----
  pmap(function(...) {

    data <- tibble(...)

    read.xlsx(data$file, sheet = data$sheet) %>%
      mutate(file = data$file, sheet = data$sheet) %>%
      rename_with(~ tolower(gsub("\\.|", "", .x))) %>%
      rename_with(~ tolower(gsub("class|classes", "class(es)", .x))) %>%
      rename_with(~ gsub("mv", "marketvalue", .x)) %>%
      rename_with(~ gsub("sqft", "sf", .x)) %>%
      rename_with(~ gsub("incm", "incomem", .x)) %>%
      rename_with(~ gsub("iasworld", "", .x)) %>%
      rename_with(~ gsub("finalmarketvalue/sf", "finalmarketvalue$/sf", .x)) %>%
      rename_with(~ gsub("marketvalue/sf", "marketvalue$/sf", .x)) %>%
      select(-starts_with("X")) %>%
      mutate(
        across(.cols = everything(), as.character)
      )

  }, .progress = TRUE) %>%
  bind_rows() %>%

  # Add useful information to output and clean-up columns ----
  mutate(
    year = str_extract(file, "[0-9]{4}"),
    township = str_extract(file, paste(ccao::town_dict$township_name, collapse = "|")),
    across(.cols = everything(), ~ na_if(.x, "N/A")),
    # Ignore known character columns for parse_number
    across(.cols = !char_cols, parse_number),
    # Columns that can be numeric should be
    across(where(~ all(check.numeric(.x))), as.numeric),
    # Don't stack pin numbers when "Thru" is present in PIN list
    across(.cols = c(pins, `class(es)`), ~ case_when(
      grepl("thru", .x, ignore.case = TRUE) ~ str_squish(.x),
      .x == "0" ~ NA,
      TRUE ~ str_replace_all(str_squish(.x), " ", ", ")
    ))
  ) %>%
  select(sort(names(.))) %>%
  relocate(c(keypin, pins, township, year)) %>%
  relocate(c(file, sheet), .after = last_col()) %>%
  write.xlsx("C:/Users/wridgew/Scratch Space/commercial_models.xlsx")