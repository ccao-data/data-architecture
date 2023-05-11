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

files <- list.files("G:/1st Pass spreadsheets", pattern = "[0-9]{4} Valuation Models", full.names = TRUE) %>%
  map(function(x) {

  list.files(file.path(x, "PublicVersions"), ignore.case = TRUE, full.names = TRUE)

}) %>%
  unlist() %>%
  grep(pattern = "Past|xlsx", invert = TRUE, value = TRUE) %>%
  map(list.files, pattern = ".xlsx", full.names = TRUE) %>%
  unlist() %>%
  map(function(x) {

    expand.grid(sheet_name = getSheetNames(x), file = x, stringsAsFactors = FALSE)

  }) %>%
  bind_rows() %>%
  arrange(sheet_name) %>%
  filter(
    str_detect(file, "Hard|Copy", negate = TRUE),
    str_detect(sheet_name, "Summary", negate = TRUE)
    ) %>%
  mutate(agg_name = trimws(str_remove_all(sheet_name, "T[0-9]{2}-")))

temp <- sapply(unique(files$agg_name), function(x) {

  group <- files %>%
    filter(agg_name == x)
    map2(group$file, group$sheet_name, function(y, z) {

      read.xlsx(y, sheet = z) %>%
        mutate(
          year = str_extract(y, "[0-9]{4}"),
          township = str_extract(y, paste(ccao::town_dict$township_name, collapse = "|")),
          file = y,
          sheet = z
          ) %>%
        rename_with(~ tolower(gsub("\\.|", "", .x))) %>%
        rename_with(~ tolower(gsub("class|classes", "class(es)", .x))) %>%
        rename_with(~ gsub("mv", "marketvalue", .x)) %>%
        rename_with(~ gsub("sqft", "sf", .x)) %>%
        rename_with(~ gsub("incm", "incomem", .x)) %>%
        rename_with(~ gsub("iasworld", "", .x)) %>%
        rename_with(~ gsub("finalmarketvalue/sf", "finalmarketvalue$/sf", .x)) %>%
        rename_with(~ gsub("marketvalue/sf" , "marketvalue$/sf" , .x)) %>%
        select(-starts_with("X")) %>%
        mutate(
          across(.cols= everything(), as.character)
          )

    }, .progress = TRUE) %>%
    bind_rows()

}, simplify = FALSE, USE.NAMES = TRUE) %>%
  bind_rows()

temp %>%
  mutate(
    across(.cols = everything(), ~ na_if(.x, "N/A")),
    across(.cols = !c(
      keypin,
      pins,
      township,
      `class(es)`,
      address,
      propertyuse,
      investmentrating,
      `f/r`,
      `carwash?`,
      ceilingheight,
      `2023permit/partial/demovalue`,
      file,
      sheet
    ), parse_number),
    across(where(~ all(check.numeric(.x)) ), as.numeric),
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