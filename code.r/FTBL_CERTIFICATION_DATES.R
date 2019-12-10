user <- Sys.info()[['user']]
source(paste0("C:/Users/",user,"/Documents/ccao_utility/code.r/99_utility_2.r"))
invisible(check.packages(libs))
dirs <- directories("ccao_sf_cama_dev")

# open SQL connection
CCAODATA <- dbConnect(odbc()
                      , driver   = "SQL Server"
                      , server   = odbc.credentials("server")
                      , database = odbc.credentials("database")
                      , uid      = odbc.credentials("uid")
                      , pwd      = odbc.credentials("pwd"))

# retrieve town codes
town_codes <- dbGetQuery(CCAODATA, paste0("
SELECT township_name as TOWNSHIP, township_code AS CODE FROM FTBL_TOWNCODES
"))

# retrieve certification dates
cert_dates <- read.xlsx2("C:/Users/wridgew/Desktop/New Microsoft Excel Worksheet.xlsx", sheetIndex = 1, header = TRUE, stringsAsFactors = FALSE)

# identify columns that need to be formatted for dates
need_chages <- which(colnames(cert_dates) %in% c("ASSESSMENT.NOTICES.MAILED", "LAST.DATE.APPEALS.ACCEPTED", "DATE.A.ROLL.CERTIFIED", "DATE.A.ROLL.PUBLISHED", "BOARD.CLOSE.DATE"))

# format dates
cert_dates[need_chages] <- lapply(cert_dates[need_chages], function(x) as.Date(as.numeric(x), origin = "1899-12-30"))

# format township column
cert_dates$TOWNSHIP <- gsub("\\*", "", cert_dates$TOWNSHIP)

# add township codes
cert_dates <- join(cert_dates, town_codes, by = "TOWNSHIP")

# format column names for SQL
colnames(cert_dates) <- gsub("\\.", "_", colnames(cert_dates))

# reorder columns
cert_dates <- cert_dates[, c(1, 10, 2, 3, 4, 5, 6, 7, 8, 9)]

# insert table into SQL server
dbWriteTable(CCAODATA, "FTBL_CERT_DATES", cert_dates, field.types = c(TAX_YEAR = "float"), overwrite = TRUE)

# disconnect
dbDisconnect(CCAODATA)