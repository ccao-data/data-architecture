# load necessary packages
library(jsonlite)
library(DBI)
library(tidyverse)
library(glue)
library(odbc)
library(safer)

# create CCAODATA connection object
CCAODATA <- dbConnect(odbc(), .connection_string = Sys.getenv("DB_CONFIG_CCAODATA"))

rpie <- data.table(dbGetQuery(CCAODATA, "

declare @AlLChars varchar(100) = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'

SELECT A.PIN AS RPIE_PIN,

RIGHT(LEFT(@AlLChars,ABS(BINARY_CHECKSUM(NEWID())%35) + 1 ), 1) +
RIGHT(LEFT(@AlLChars,ABS(BINARY_CHECKSUM(NEWID())%35) + 1 ), 1) +
RIGHT(LEFT(@AlLChars,ABS(BINARY_CHECKSUM(NEWID())%35) + 1 ), 1) +

'-' +

RIGHT(LEFT(@AlLChars,ABS(BINARY_CHECKSUM(NEWID())%35) + 1 ), 1) +
RIGHT(LEFT(@AlLChars,ABS(BINARY_CHECKSUM(NEWID())%35) + 1 ), 1) +
RIGHT(LEFT(@AlLChars,ABS(BINARY_CHECKSUM(NEWID())%35) + 1 ), 1) +

'-' +

RIGHT(LEFT(@AlLChars,ABS(BINARY_CHECKSUM(NEWID())%35) + 1 ), 1) +
RIGHT(LEFT(@AlLChars,ABS(BINARY_CHECKSUM(NEWID())%35) + 1 ), 1) +
RIGHT(LEFT(@AlLChars,ABS(BINARY_CHECKSUM(NEWID())%35) + 1 ), 1) +

'-' +

RIGHT(LEFT(@AlLChars,ABS(BINARY_CHECKSUM(NEWID())%35) + 1 ), 1)

AS RPIE_CODE,

triad_name, FTBL_TOWNCODES.township_name,
HD_NAME AS MAILING_NAME, PROPERTY_ADDRESS, PROPERTY_APT_NO, PROPERTY_CITY, PROPERTY_ZIP, MAILING_ADDRESS, MAILING_CITY, MAILING_ZIP,
TAX_YEAR AS CALENDAR_YEAR

FROM

(SELECT PIN,HD_TOWN, HD_NAME, HD_PRI_LND + HD_PRI_BLD as Assessed_Value, TAX_YEAR
FROM AS_HEADT WHERE TAX_YEAR = 2019 AND LEFT(HD_CLASS, 1) NOT IN ('0', '1', '2', '4')) AS A

LEFT JOIN VW_PINGEO AS B ON A.PIN = B.PIN
LEFT JOIN FTBL_TOWNCODES ON LEFT(A.HD_TOWN,2) = township_code

"))

# Define function to geocode missing mailing addresses
census_geocoder <- function(address, type, secondary, state){

  addy <- paste0("street=", gsub(" ", "+", address))

  if (type == "z") {

    wild <- paste0("zip=", gsub(" ", "+", secondary))

  } else {

    wild <- paste0("city=", gsub(" ", "+", secondary))

  }

  state <- paste0("state=", gsub(" ", "+", state))

  string <- paste0("https://geocoding.geo.census.gov/geocoder/geographies/address?",
                   addy, "&", wild,"&", state,
                   "&benchmark=4&vintage=4&format=json")

  json_file <- read_json(string)

  if (length(json_file$result$addressMatches) > 0) {

    print("Success!")

    #Address, lat, lon,tract, block (keep first match)
    return(as.character(json_file$result$addressMatches[[1]]$matchedAddress))

  } else {

    print("Failure!")

    return(NA_character_)

  }

}

rpie_cleaned <- rpie %>%

  as_tibble() %>%
  mutate_if(is.character, function(x) {x %>% str_squish() %>% str_trim()}) %>%

  mutate(MAILING_STATE = MAILING_CITY %>% str_trim() %>% str_sub(-2, -1),
         MAILING_CITY = MAILING_CITY %>% str_trim() %>% str_sub(1, -3) %>% str_trim() %>% str_squish()) %>%

  select(RPIE_PIN:MAILING_CITY, MAILING_STATE, MAILING_ZIP, CALENDAR_YEAR) %>%

  mutate(MAILING_ZIP = ifelse(str_length(MAILING_ZIP) == 4,
                              str_pad(MAILING_ZIP, 5, "left", 0),
                              MAILING_ZIP)) %>%

  mutate(RPIE_MAILING_ADDRESS = ifelse(str_length(MAILING_ADDRESS) == 0, PROPERTY_ADDRESS, MAILING_ADDRESS),
         RPIE_MAILING_CITY = ifelse(str_length(MAILING_CITY) == 0, PROPERTY_CITY, MAILING_CITY),
         RPIE_MAILING_STATE = ifelse(str_length(MAILING_STATE) == 0, "IL", MAILING_STATE),
         RPIE_MAILING_ZIP = ifelse(str_length(MAILING_ADDRESS) == 0, PROPERTY_ZIP, MAILING_ZIP))

rpie_geocode_pass_1 <- rpie_cleaned %>%

  filter(str_length(RPIE_MAILING_ZIP) != 5, str_length(RPIE_MAILING_ZIP) != 9) %>%
  mutate(MAILING_ADDRESS_MATCHED = pmap_chr(., ~ census_geocoder(..10, "c", ..11, ..12)))

rpie_geocode_pass_2 <- rpie_geocode_pass_1 %>%

  separate(MAILING_ADDRESS_MATCHED,
           into = c("RPIE_MAILING_ADDRESS", "RPIE_MAILING_CITY",
                    "RPIE_MAILING_STATE", "RPIE_MAILING_ZIP"),
           sep = ", ") %>%

  mutate(RPIE_MAILING_ADDRESS = ifelse(is.na(RPIE_MAILING_ADDRESS), PROPERTY_ADDRESS, RPIE_MAILING_ADDRESS),
         RPIE_MAILING_CITY = ifelse(is.na(RPIE_MAILING_CITY), PROPERTY_CITY, RPIE_MAILING_CITY),
         RPIE_MAILING_STATE = ifelse(is.na(RPIE_MAILING_STATE), "IL", RPIE_MAILING_STATE),
         RPIE_MAILING_ZIP = ifelse(is.na(RPIE_MAILING_ZIP), PROPERTY_ZIP, RPIE_MAILING_ZIP))

rpie_geocode_pass_3 <- rpie_geocode_pass_2 %>%

  filter(RPIE_MAILING_ZIP == "0") %>%
  mutate(MAILING_ADDRESS_MATCHED = pmap_chr(., ~ census_geocoder(..15, "c", ..16, ..17))) %>%

  separate(MAILING_ADDRESS_MATCHED,
           into = c("RPIE_MAILING_ADDRESS", "RPIE_MAILING_CITY",
                    "RPIE_MAILING_STATE", "RPIE_MAILING_ZIP"),
           sep = ", ")

rpie_done <- rpie_cleaned %>%

  anti_join(rpie_geocode_pass_2 %>% select(RPIE_PIN)) %>%

  bind_rows(rpie_geocode_pass_2 %>% anti_join(rpie_geocode_pass_3 %>% select(RPIE_PIN)),
            rpie_geocode_pass_3)

dbWriteTable(CCAODATA, "DTBL_RPIE_NOTICES", rpie_done, overwrite = TRUE)
