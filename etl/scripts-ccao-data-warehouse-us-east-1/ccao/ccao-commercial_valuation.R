# This script ingests excel workbooks from the IC drive and standardizes them
# in order to compile a single aggregated dataset of commercial valuation data.
# It should be run once a year after determining if Commercial Valuations has
# completed their valuation for the current tri.

library(ccao)
library(dplyr)
library(openxlsx)
library(readr)
library(tidyverse)
library(stringr)
library(varhandle)
source("utils.R")

# Declare output paths
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  "ccao", "other", "commercial_valuation"
)

# Declare known character columns
char_cols <- c(
  "keypin",
  "pins",
  "township",
  "class(es)",
  "address",
  "property_name/description",
  "property_type/use",
  "investmentrating",
  "f/r",
  "carwash",
  "ceilingheight",
  "permit/partial/demovalue",
  "file",
  "sheet",
  "model",
  "subclass2",
  "permit/partial/demovaluereason",
  "townregion",
  "year",
  "taxdist",
  "taxpayer"
)

# Declare known integer columns
int_cols <- c(
  "yearbuilt",
  "1brunits",
  "2brunits",
  "3brunits",
  "4brunits",
  "aprx_comm_sf",
  "bldgsf",
  "gross_building_area",
  "landsf",
  "studiounits",
  "tot_units",
  "totalrevreported",
  "totallandval"
)

# Declare columns to remove
remove_cols <- c(
  "age",
  "boatslips",
  "ccaofinal",
  "comments",
  "cond",
  "locrating",
  "mobilehomepads",
  "tot_apts",
  "usecode",
  "imprname",
  "units/beds"
)

# Declare all regex syntax for renaming sheet column names as they're ingested
renames <- c(
  "\\.|[0-9]{4}|\\$|\\?" = "",
  "additional" = "excess",
  "(^adj)(.*rent.*)" = "adj_rent/sf",
  "adjsales" = "adjsale",
  "hotelclass" = "hotel",
  "^idph.*" = "idphlicense#",
  "cost\\\\" = "cost",
  "^costapp.*" = "costapproach/sf",
  "(.*bed.*)(.*day.*)" = "revenuebed/day",
  "^(?!.*sub).*class.*" = "class(es)",
  "(^exc)(.*val.*)|surpluslandvalue" = "excesslandval",
  "^exp%|^%exp|totalexp%" = "exp",
  "approxcommsf|apprxtotalsfcomm|commsf" = "aprx_comm_sf",
  "ceilinght" = "ceilingheight",
  "grouping" = "s",
  "mv" = "marketvalue",
  "occ%" = "occupancy",
  "sqft" = "sf",
  "(^income)(.*exc.*)|.*incm.*|^incc.*" = "incomemarketvalue",
  "iasworld" = "",
  ".*landsf.*" = "landsf",
  "marketmarket" = "market",
  "netbldgsf" = "netrentablesf",
  "netincome|.*noi.*" = "noi",
  ".*occupancy.*" = "reportedoccupancy",
  "^oiltankvalue.*" = "oiltankvalue/atypicaloby",
  ".*pgi.*|grossinc" = "pgi",
  "^propertyn.*|^propertyd.*" = "property_name/description",
  "^propertyt.*|^propertyu.*" = "property_type/use",
  "(.*sale.*)(.*/sf.*)" = "salecompmarketvalue/sf",
  "(^total)(.*ap.*)" = "tot_apts",
  "(^total)(.*units.*)|^#of.*|units/keys" = "tot_units",
  "unit2" = "unit",
  "^v.*|%vac" = "vacancy",
  "br$" = "brunits",
  "yearblt" = "yearbuilt",
  "landtotalsf" = "landsf",
  "estvacancy%" = "vacancy",
  "landtotalval" = "totallandval",
  "revenue/bed/night" = "revenuebed/day",
  "taxdistrict" = "taxdist",
  "studios" = "studiounits",
  "l:bratio" = "land:bldg",
  "rev/key/night" = "avgdailyrate",
  "partialvalue" = "permit/partial/demovalue",
  "gba" = "gross_building_area"
)

# Compile a filtered list of excel workbooks and worksheets to ingest ----
output <- list.files(
  "G:/1st Pass spreadsheets",
  pattern = "[0-9]{4} Valuation",
  full.names = TRUE
) %>%
  list.files(pattern = "public", full.names = TRUE, ignore.case = TRUE) %>%
  list.files(pattern = ".xlsx", full.names = TRUE, recursive = TRUE) %>%
  grep(
    pattern = "Other|PropertyData|12-09-2025", invert = TRUE, value = TRUE
  ) %>%
  map(function(x) {
    # Unfortunately, people are still working on some of these sheets which
    # means this script will error out when a file is open - `possibly` here
    # avoids that. Don't consider data final if an error is caught.
    safe_gSN <- possibly(.f = getSheetNames, otherwise = NULL)

    crossing(sheet = safe_gSN(x), file = x)
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
  filter(
    check.numeric(excesslandval),
    !is.na(keypin),
    !str_detect(keypin, "[:alpha:]"),
    keypin != "0"
  ) %>%
  select(where(~ !all(is.na(.x)))) %>%
  # Add useful information to output and clean-up columns ----
  mutate(
    keypin = str_trim(keypin),
    keypin = str_pad(keypin, side = "left", width = 14, pad = "0"),
    keypin = map(keypin, \(x) {
      ifelse(nchar(x) == 14, pin_format_pretty(x, full_length = TRUE), x)
    }),
    keypin = str_pad(keypin, side = "right", width = 18, pad = "0"),
    year = str_extract(file, "[0-9]{4}"),
    township = str_replace_all(
      str_extract(
        file,
        str_remove_all(
          paste(ccao::town_dict$township_name, collapse = "|"), " "
        )
      ),
      c(
        "ElkGrove" = "Elk Grove",
        "HydePark" = "Hyde Park",
        "LakeView" = "Lake View",
        "NewTrier" = "New Trier",
        "NorthChicago" = "North Chicago",
        "NorwoodPark" = "Norwood Park",
        "OakPark" = "Oak Park",
        "SouthChicago" = "South Chicago",
        "RiverForest" = "River Forest",
        "RogersPark" = "Rogers Park",
        "WestChicago" = "West Chicago"
      )
    ),
    township = coalesce(township, ccao::town_convert(substr(taxdist, 1, 2))),
    across(.cols = everything(), ~ na_if(.x, "N/A")),
    # Ignore known character columns for parse_number
    across(.cols = !any_of(char_cols), parse_number),
    # Columns that can be numeric should be
    across(where(~ all(check.numeric(.x))), as.numeric),
    yearbuilt = case_when(
      is.na(yearbuilt) & age < 1000 ~ (year - age),
      TRUE ~ yearbuilt
    ),
    tot_units = coalesce(tot_units, tot_apts, `boatslips`, `mobilehomepads`),
    # Don't stack pin numbers when "Thru" is present in PIN list
    across(.cols = c(pins, `class(es)`), ~ case_when(
      grepl("thru", .x, ignore.case = TRUE) ~ str_squish(.x),
      .x == "0" ~ NA,
      TRUE ~ str_replace_all(str_squish(.x), " ", ", ")
    )),
    across(int_cols, as.integer),
    across(char_cols, as.character),
    across(where(is.character), ~ gsub("\\r", "", .x))
  ) %>%
  # Remove empty columns
  select(where(~ !(all(is.na(.)) | all(. == "")))) %>%
  # Remove pre-declared columns
  select(!all_of(remove_cols) & !starts_with("market")) %>%
  select(all_of(sort(names(.)))) %>%
  relocate(c(keypin, pins, township, year)) %>%
  relocate(c(file, sheet), .after = last_col()) %>%
  distinct()

output %>%
  group_by(year) %>%
  write_partitions_to_s3(output_bucket, is_spatial = FALSE, overwrite = TRUE)
