# this script appends new pipeline values to the DTBL_MODELVALS SQL table
# run 'ccao_sf_cama_dev/code.r/main.R' before this script to make sure destfile is defined

load(destfile)

CCAODATA <- dbConnect(odbc(),
                      driver   = "SQL Server",
                      server   = odbc.credentials("server"),
                      database = odbc.credentials("database"),
                      uid      = odbc.credentials("uid"),
                      pwd      = odbc.credentials("pwd"))

pipeline_columns <- colnames(dbGetQuery(CCAODATA, paste0("
SELECT TOP 1 * FROM DTBL_MODELVALS
")))

if(!grepl("_v", substr(destfile, 34, nchar(destfile) - 4))){
  
  valuationdata$version <- NA
  
} else {
  
  valuationdata$version <- substr(destfile, nchar(destfile) - 5, nchar(destfile) - 4)
  
}

dbWriteTable(CCAODATA, "DTBL_MODELVALS", subset(valuationdata, select = pipeline_columns), append = TRUE)

# disconnect after pulls
dbDisconnect(CCAODATA)


