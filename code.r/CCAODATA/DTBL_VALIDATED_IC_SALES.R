# this script exists soley to insert validated IC sales into the SQL server

# connect to SQL server
CCAODATA <- dbConnect(odbc::odbc(), .connection_string = Sys.getenv("DB_CONFIG_CCAODATAW"))

# ingest old formatted validated sales
old_sales <- read_xlsx("O:/CCAODATA/results/ad_hoc/validation_report.xlsx",
                       col_types = c("text", "skip", "text", "text", "skip", "skip")) %>%
  rename(VALID = VALID_INVALID)

# ingest new sales
new_sales <- read_xlsx("O:/CCAODATA/data/raw_data/2020 Sales to Validate for DS 11.14.2020.xlsx",
                       col_types = c("skip", "skip", "skip", "skip", "text", "skip", "text", "text")) %>%

  rename(DEED_NUMBER = DOC_NO)

# combine sales and format for upload
validated_sales <- bind_rows(old_sales, new_sales) %>%

  mutate(DEED_NUMBER = str_pad(DEED_NUMBER, 11, side = "left", pad = "0"),
         VALID = as.logical(case_when(VALID %in% c("yes", "Y") ~ 1,
                                      VALID %in% c("no", "N") ~ 0)),
         COMMENTS = case_when(is.na(COMMENTS) ~ "",
                              TRUE ~ COMMENTS)) %>%

  distinct() %>%
  group_by(DEED_NUMBER, VALID) %>%
  summarise(COMMENTS = paste(COMMENTS, collapse = "; ")) %>%
  ungroup() %>%

  mutate(COMMENTS = case_when(substr(COMMENTS, 1, 2) == "; " ~ substr(COMMENTS, 3, nchar(COMMENTS)),
                              TRUE ~ COMMENTS))

# insert into sql server
dbWriteTable(CCAODATA, "DTBL_VALIDATED_IC_SALES", validated_sales, overwrite = TRUE)

# disconnect after pulls
dbDisconnect(CCAODATA)