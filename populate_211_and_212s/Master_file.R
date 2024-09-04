library(DBI)
library(noctua)
library(dplyr)
library(xlsx)
library(readr)
library(stringr)

AWS_ATHENA_CONN_NOCTUA <- dbConnect(noctua::athena())

missing_units <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA, read_file("populate_211_and_212s/Missing_units.sql")
) %>%
  distinct(pin)

data_quality <- dbGetQuery(
  conn = AWS_ATHENA_CONN_NOCTUA, read_file("populate_211_and_212s/MyDec_script.sql")
) %>%
  select(line_8_intended_number_of_apartment_units, pin)

usps_data <- read.csv("populate_211_and_212s/usps_output.csv") %>%
  group_by(PIN) %>%
  filter(DPVConfirmation == "Y") %>%
  filter(is.na(Address1) | Address1 != "") %>%
  summarise(
    Address1_combined = paste(unique(Address1), collapse = ", "),
    usps_value = n_distinct(Address1)
  ) %>%
  ungroup() %>%
  filter(!grepl("[ABCDEFG]", Address1_combined, ignore.case = TRUE)) %>%
  rename("pin" = "PIN") %>%
  mutate(pin = as.character(pin))


usps_data <- read.csv("populate_211_and_212s/usps_output.csv") %>%
  group_by(PIN) %>%
  filter(DPVConfirmation == "Y") %>%
  filter(is.na(Address1) | Address1 != "") %>%
  summarise(
    Address1_combined = paste(unique(Address1), collapse = ", "),
    usps_value = n_distinct(Address1)
  ) %>%
  ungroup() %>%
  filter(!grepl("[ABCDEFG]", Address1_combined, ignore.case = TRUE)) %>%
  rename("pin" = "PIN") %>%
  mutate(pin = as.character(pin))

redfin_9_3 <- read_csv("populate_211_and_212s/redfin_9.3.csv",
                       col_types = cols(pin = col_character()))



redfin_data <- redfin_9_3 %>%
  mutate(
    redfin_data = ifelse(
      is.na(char_apts_redfin) | char_apts_redfin == "",
      char_apts_in_building,
      char_apts_redfin
    )
  ) %>%
  mutate(
    remarks_cleaned = str_replace_all(remarks, "[,\\-\\s]", ""),  # Remove commas, hyphens, and spaces
    test = case_when(
      grepl("singlefamily", remarks_cleaned, ignore.case = TRUE) ~ 1,
      grepl("duplex|twounit|2unit|twoap|2ap|twoflat|2flat",
            remarks_cleaned, ignore.case = TRUE) ~ 2,
      grepl("threeunit|3unit|threeap|3ap|threeflat|3flat",
            remarks_cleaned, ignore.case = TRUE) ~ 3,
      grepl("fourunit|4unit|fourap|4ap|fourflat|4flat",
            remarks_cleaned, ignore.case = TRUE) ~ 4,
      grepl("fiveunit|5unit|fiveap|5ap|fiveflat|5flat",
            remarks_cleaned, ignore.case = TRUE) ~ 5,
      grepl("sixunit|6unit|sixap|6ap|sixflat|6flat",
            remarks_cleaned, ignore.case = TRUE) ~ 6,
      TRUE ~ NA_real_
    )
  )


final <- left_join(missing_units, data_quality, by = "pin") %>%
  left_join(usps_data, by = "pin") %>%
  left_join(redfin_data, by = "pin")
