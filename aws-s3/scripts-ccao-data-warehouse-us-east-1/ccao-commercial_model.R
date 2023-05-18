# This script ingests excel workbooks from the IC drive and standardizes them
# in order to compile a single aggregated dataset of commercial valuation data.

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
  "property_type/use",
  "investmentrating",
  "f/r",
  "carwash?",
  "ceilingheight",
  "2023permit/partial/demovalue",
  "file",
  "sheet"
)

# Declare all regex syntax for renaming sheet column names as they're ingested
renames <- c(
  "\\." = "",
  "(^adj)(.*rent.*)" = "adj_rent$/sf",
  "adjsales" = "adjsale",
  "hotelclass" = "hotel",
  "cost\\\\" = "cost",
  "costapproach" = "costapp",
  "costapp" = "costapproach",
  ".*class.*" = "class(es)",
  "(^exc)(.*val.*)" = "excesslandval",
  "^exp%|^%exp" = "exp",
  "approxcommsf|apprxtotalsfcomm|commsf" = "aprx_comm_sf",
  "ceilinght" = "ceilingheight",
  "mv" = "marketvalue",
  "occ%" = "occupancy",
  "sqft" = "sf",
  "incm" = "incomem",
  "(^income)(.*exc.*)" = "income_mv_incl_excessland",
  "iasworld" = "",
  "finalmarketvalue/sf" = "finalmarketvalue$/sf",
  "marketmarket" = "market",
  "marketvalue/sf" = "marketvalue$/sf",
  "(^market)(.*exc.*)" = "marketvalue_incl_excessland",
  "^propertyn.*|^propertyd.*" = "property_name/description",
  "^propertyt.*|^propertyt.*" = "property_type/use",
  "(^total)(.*ap.*)" = "tot_apts",
  "(^total)(.*units.*)" = "tot_units",
  "unit2" = "unit",
  "^v.*|%vac" = "vacancy"
)

# Compile a filtered list of excel workbooks and worksheets to ingest ----
list.files(
  "G:/1st Pass spreadsheets",
  pattern = "[0-9]{4} Valuation",
  full.names = TRUE
) %>%
  file.path("PublicVersions") %>%
  list.files(ignore.case = TRUE, full.names = TRUE) %>%
  # Ignore files that are currently open
  grep(pattern = "Other", invert = TRUE, value = TRUE) %>%
  list.files(pattern = ".xlsx", full.names = TRUE) %>%
  map(function(x) {

    crossing(
      # Unfortunately, people can still be working on these sheets which means
      # this script will error out when a file is open - TryCatch here avoids
      # that. Don't consider data final if an error is thrown.
      sheet = tryCatch(
        {getSheetNames(x)},
        error = function(error_message){return(NULL)}
        ),
      file = x
      )

    }, .progress = TRUE) %>%
  bind_rows() %>%
  filter(str_detect(sheet, "Summary", negate = TRUE), !is.na(sheet)) %>%

  # Ingest the sheets, clean, and bind them ----
  pmap(function(...) {

    data <- tibble(...)

    read.xlsx(data$file, sheet = data$sheet) %>%
      mutate(file = data$file, sheet = data$sheet) %>%
      rename_with(tolower) %>%
      set_names(str_replace_all(names(.), renames)) %>%
      select(-starts_with("X"), -contains("age2")) %>%
      mutate(
        across(.cols = everything(), as.character)
      )

  }, .progress = TRUE) %>%
  bind_rows() %>%
  filter(check.numeric(excesslandval)) %>%
  select(where(~ !all(is.na(.x)))) %>%

  # Add useful information to output and clean-up columns ----
  mutate(
    year = str_extract(file, "[0-9]{4}"),
    township = str_extract(
      file,
      str_remove_all(paste(ccao::town_dict$township_name, collapse = "|"), " ")
      ),
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
  select(all_of(sort(names(.)))) %>%
  relocate(c(keypin, pins, township, year)) %>%
  relocate(c(file, sheet), .after = last_col()) %>%
  write.xlsx("C:/Users/wridgew/Scratch Space/commercial_models.xlsx")
