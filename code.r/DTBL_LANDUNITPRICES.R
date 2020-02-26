# this script aggregates land unit prices for a given year and appends them to DTBL_LANDUNITPRICES
# based on how the files are named and a given tri, debugging may be needed to properly ingest the necessary excel files

user <- Sys.info()[['user']]
source(paste0("C:/Users/",user,"/Documents/ccao_utility/code.r/99_utility_2.r"))
invisible(check.packages(libs))
dirs <- directories("ccao_sf_cama_dev")
options(java.parameters = "-Xmx24g")
database <- 1

# input year and tri LUPs to be uploaded
year <- 2019
tri <- "North"

# connect to retrieve data
CCAODATA <- dbConnect(odbc(),
                      driver   = "SQL Server",
                      server   = odbc.credentials("server"),
                      database = odbc.credentials("database"),
                      uid      = odbc.credentials("uid"),
                      pwd      = odbc.credentials("pwd"))

# acquire township names and codes
land_towns <- dbGetQuery(CCAODATA, paste0("
SELECT CONVERT(varchar, township_code) AS TOWN, UPPER(township_name) as township_name FROM FTBL_TOWNCODES
WHERE triad = 2
"))

# ingest LUPs
land_values <- data.frame()

for (i in land_towns[, 2]) {
  
  x <- read.xlsx2(paste0("O:/", year, " ", tri, " Tri Res Land/", i, " 2-CLASS RES PRICING ", year, ".xlsx"), header = FALSE, sheetIndex = 1, stringsAsFactors = FALSE)
  x[, 4] <- i
  
  land_values <- rbind(land_values, x)
  
}

# clean up
land_values[land_values == ''] <- NA
land_values <- na.omit(land_values)
land_values <- land_values[grep('land|classes', land_values$X3, ignore.case = TRUE, inv = TRUE), ]
land_values <- land_values[, c(1, 2, 4)]
colnames(land_values) <- c("NBHD", "LUP", "township_name")

# add town codes
land_values <- left_join(land_values, land_towns, by = "township_name")

# clean up
land_values <- dplyr::select(land_values, c("TOWN", "NBHD", "LUP"))
land_values$LUP <- as.numeric(land_values$LUP)
land_values$TAX_YEAR <- year

# export main condo strata table
dbWriteTable(CCAODATA, "DTBL_LANDUNITPRICES", land_values, row.names = FALSE, append = TRUE)

