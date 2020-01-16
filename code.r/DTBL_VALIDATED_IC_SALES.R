# this script exists soley to insert validated IC sales into the SQL server
# it is a slave to "ccao_ic_cama_dev/code.r/validation_report.R"

CCAODATA <- dbConnect(odbc(),
                      driver   = "SQL Server",
                      server   = odbc.credentials("server"),
                      database = odbc.credentials("database"),
                      uid      = odbc.credentials("uid"),
                      pwd      = odbc.credentials("pwd"))

dbWriteTable(CCAODATA, "DTBL_VALIDATED_IC_SALES", validated_sales, overwrite = TRUE)

# disconnect after pulls
dbDisconnect(CCAODATA)