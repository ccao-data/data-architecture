# this script generates a list of condo buildings with associated sales data
# from the 5 years preceding the target year for a given township

# north tri "26, 17, 23, 16, 22, 25, 10, 20, 38, 29, 35, 24, 18"
# south tri "11, 12, 13, 14, 15, 19, 21, 27, 28, 30, 31, 32, 33, 34, 36, 37, 39"
# city "70, 71, 72, 73, 74, 75, 76, 77"
# north tri + proviso, jefferson, and rogers park "26, 17, 23, 16, 22, 25, 10, 20, 38, 29, 35, 24, 18, 31"
towns <- "70, 71, 72, 73, 74, 75, 76, 77"
year <- 2019

# connect
CCAODATA <- dbConnect(odbc()
                      , driver   = "SQL Server"
                      , server   = odbc.credentials("server")
                      , database = odbc.credentials("database")
                      , uid      = odbc.credentials("uid")
                      , pwd      = odbc.credentials("pwd"))

# SQL pull
pull <- dbGetQuery(CCAODATA, paste0("
SELECT H.PIN AS PIN, LEFT(H.PIN, 10) AS PIN10, HD_CLASS AS CLASS, LEFT(HD_TOWN, 2) AS TOWN, HD_NBHD AS NBHD,
			CASE WHEN SALES.SALE_PRICE <= 10000 OR (HD_PRI_BLD + HD_PRI_LND) <= 10 OR DT_CDU = 'GR' OR DT_CDU = 'ST' THEN NULL
      ELSE SALES.SALE_DATE END AS SALE_DATE,
      CASE WHEN SALES.SALE_PRICE <= 10000 OR (HD_PRI_BLD + HD_PRI_LND) <= 10 OR DT_CDU = 'GR' OR DT_CDU = 'ST' THEN NULL
      ELSE SALES.SALE_YEAR END AS SALE_YEAR,
      DOC_NO, PER_ASS, HD_PRI_BLD, HD_PRI_LND, AGE,
			CASE WHEN ((HD_PRI_BLD + HD_PRI_LND) <= 10) THEN 'COMMON AREA'
					WHEN (DT_CDU = 'GR' AND (HD_PRI_BLD + HD_PRI_LND) > 10) THEN 'GR'
					WHEN DT_CDU = 'ST' THEN 'STORAGE'
			ELSE NULL END AS CDU,
      CASE WHEN SALES.SALE_PRICE <= 10000 OR (HD_PRI_BLD + HD_PRI_LND) <= 10 OR DT_CDU = 'GR' OR DT_CDU = 'ST' THEN NULL
      ELSE SALES.SALE_PRICE END AS SALE_PRICE,
	  CASE WHEN PL_HOUSE_NO IS NULL THEN 'ADDRESS MISSING FROM PROPLOCS' ELSE
	  LTRIM(RTRIM(CAST(PL_HOUSE_NO as varchar(10)))) + ' ' + LTRIM(RTRIM(PL_DIR))+ ' ' + LTRIM(RTRIM(PL_STR_NAME))+ 
    ' ' + LTRIM(RTRIM(PL_STR_SUFFIX)) + ' ' + LTRIM(RTRIM(PL_CITY_NAME)) END AS ADDR
    , PUMA
		FROM AS_HEADTB AS H
		LEFT JOIN
			(SELECT DISTINCT PIN, DT_CDU, DT_PER_ASS AS PER_ASS, DT_AGE AS AGE, TAX_YEAR FROM AS_DETAILT
			WHERE TAX_YEAR = ", year, "
			AND DT_CLASS IN (299, 399, 599)
			) AS DETAIL
		ON H.PIN = DETAIL.PIN
		LEFT JOIN
			(SELECT X.PIN, X.RECORDED_DATE AS SALE_DATE, year(X.RECORDED_DATE) AS SALE_YEAR, MAX(X.SALE_PRICE) AS SALE_PRICE FROM
			IDORSALES AS X
			WHERE year(X.RECORDED_DATE) >= 1997
      AND X.MULT_IND = ''
      AND X.DEED_TYPE NOT IN ('Q', 'E', 'B')
			GROUP BY X.PIN, X.RECORDED_DATE
			) AS SALES
		ON H.PIN = SALES.PIN
		LEFT JOIN
		PROPLOCS
		ON H.PIN = PL_PIN
    LEFT JOIN
		(SELECT PIN, RECORDED_DATE AS SALE_DATE, SALE_PRICE, DOC_NO FROM IDORSALES) AS DOX
		ON H.PIN = DOX.PIN AND SALES.SALE_DATE = DOX.SALE_DATE AND SALES.SALE_PRICE = DOX.SALE_PRICE
		LEFT JOIN
		(SELECT Name, PUMA FROM PINLOCATIONS) AS LOX
		ON LEFT(H.PIN, 10) = LEFT(Name, 10)
		WHERE HD_CLASS IN (299, 399, 599)
		AND LEFT(HD_TOWN, 2) NOT IN (70, 71, 72, 73, 74, 75, 76, 77)
		AND H.TAX_YEAR = ", year
))

# only keep buildings that have at least one 299
pull <- pull %>% group_by(PIN10) %>% filter(any(CLASS == 299))

# buildings without age are probably new
pull$AGE[is.na(pull$AGE)] <- 1

# some pins are not in pinlocations and don't have lat/long
pull$PUMA[pull$PIN10 == '1028201034'] <- "03421"
pull$PUMA[pull$PIN10 == '1312224035'] <- "03503"
pull$PUMA[pull$PIN10 == '1312315027'] <- "03504"
pull$PUMA[pull$PIN10 == '1312421022'] <- "03503"
pull$PUMA[pull$PIN10 == '1313207041'] <- "03503"
pull$PUMA[pull$PIN10 == '1315211045'] <- "03504"
pull$PUMA[pull$PIN10 == '1328303041'] <- "03521"
pull$PUMA[pull$PIN10 == '1334208014'] <- "03522"
pull$PUMA[pull$PIN10 == '1319433051'] <- "03520"
pull$PUMA[pull$PIN10 == '1335233036'] <- "03522"
pull$PUMA[pull$PIN10 == '1432425141'] <- "03502"
pull$PUMA[pull$PIN10 == '1512417010'] <- "03407"
pull$PUMA[pull$PIN10 == '1601425054'] <- "03524"
pull$PUMA[pull$PIN10 == '1704201057'] <- '03525'
pull$PUMA[pull$PIN10 == '1704201062'] <- "03525"
pull$PUMA[pull$PIN10 == '1722312027'] <- "03525"
pull$PUMA[pull$PIN10 == '1722314037'] <- "03525"
pull$PUMA[pull$PIN10 == '1731305036'] <- "03526"
pull$PUMA[pull$PIN10 == '2015403037'] <- "03529"
pull$PUMA[pull$PIN10 == '2301409024'] <- "03410"

# adjust prices to Q4 2018
pull$SALE_QUARTER <- lubridate::quarter(pull$SALE_DATE)
pull$PUMA <- as.numeric(pull$PUMA)
pull <- IHS_sale_time_adj(pull, "2018", "4", "SALE_YEAR", "SALE_QUARTER")
pull$PUMA.y <- NULL
pull$SALE_PRICE_OG <- pull$SALE_PRICE
pull$adjustment_factor[pull$SALE_YEAR == 2019] <- 1
pull$SALE_PRICE <- as.numeric(pull$SALE_PRICE) * pull$adjustment_factor

# format PIN10
pull$PIN10 <- paste(substr(pull$PIN10, 1, 2), substr(pull$PIN10, 3, 4), substr(pull$PIN10, 5, 7), substr(pull$PIN10, 8, 10), sep = "-")

# create sales universe
sales_universe<-subset(pull, !is.na(SALE_PRICE))
sales_universe$SALE_PRICE<-as.numeric(sales_universe$SALE_PRICE)
sales_universe$PIN <- paste(substr(sales_universe$PIN, 1, 2), substr(sales_universe$PIN, 3, 4), substr(sales_universe$PIN, 5, 7), substr(sales_universe$PIN, 8, 10), substr(sales_universe$PIN, 11, 14), sep = "-")

number_sales <- nrow(sales_universe)

# list of NBHDS for each building
nbhds <- dplyr::select(pull[!duplicated(pull$PIN10),], c("PIN10", "NBHD", "TOWN"))
colnames(nbhds)[colnames(nbhds) == 'NBHD'] <- 'nbhd'

# aggregating to building level
pull <- pull %>% dplyr::group_by(PIN10) %>% summarise(total_pins = length(PIN[!duplicated(PIN)]),
                                                      age = round(mean(AGE, na.rm=TRUE), 0),
                                                      num_sales = length(SALE_PRICE[!is.na(SALE_PRICE)]),
                                                      mean_sale_price = round(mean(as.numeric(SALE_PRICE), na.rm=TRUE), 0 ),
                                                      adj_mean_sale_price = round(mean(as.numeric(SALE_PRICE) / (PER_ASS * 100), na.rm=TRUE), 0 ),
                                                      min_sale_price = min(as.numeric(SALE_PRICE), na.rm=TRUE),
                                                      p25_sale_price = quantile(as.numeric(SALE_PRICE), c(.25), na.rm=TRUE),
                                                      median_sale_price = median(as.numeric(SALE_PRICE), na.rm=TRUE),
                                                      p75_sale_price = quantile(as.numeric(SALE_PRICE), c(.75), na.rm=TRUE),
                                                      max_sale_price = max(as.numeric(SALE_PRICE), na.rm=TRUE),
                                                      parking_spaces = length(CDU[CDU == 'GR' & !is.na(CDU) & !duplicated(PIN)]),
                                                      common_areas = length(CDU[CDU == 'COMMON AREA' & !is.na(CDU) & !duplicated(PIN)]),
                                                      storage_units = length(CDU[CDU == 'STORAGE' & !is.na(CDU) & !duplicated(PIN)]),
                                                      dwelling_units = length(CDU[is.na(CDU) & !duplicated(PIN)]))

# additional info
pull <- join(pull, nbhds, by = "PIN10")

# assign condo strata
pull$condo_strata_10[!is.na(pull$mean_sale_price)] <- cut(pull$mean_sale_price[!is.na(pull$mean_sale_price)]
                                                          , breaks = quantile(pull$mean_sale_price[!is.na(pull$mean_sale_price)]
                                                                              , probs = seq(0, 1, 0.1))
                                                          , labels = 1:10
                                                          , include.lowest = TRUE
                                                          , na.rm = TRUE)
pull$condo_strata_100[!is.na(pull$mean_sale_price)] <- cut(pull$mean_sale_price[!is.na(pull$mean_sale_price)]
                                                           , breaks = quantile(pull$mean_sale_price[!is.na(pull$mean_sale_price)]
                                                                               , probs = seq(0, 1, 0.01))
                                                           , labels = 1:100
                                                           , include.lowest = TRUE
                                                           , na.rm = TRUE)

# stats by condo strata
pull$strata_10_age <- pull$strata_100_age <- NA
pull$strata_10_medsp <- pull$strata_100_medsp <- NA

for (i in 1:10){
  pull$strata_10_age[pull$condo_strata_10 == i] <- median(pull$age[pull$condo_strata_10 == i], na.rm = TRUE)
  pull$strata_10_minsp[pull$condo_strata_10 == i] <- min(pull$mean_sale_price[pull$condo_strata_10 == i], na.rm = TRUE)
  pull$strata_10_medsp[pull$condo_strata_10 == i] <- median(pull$mean_sale_price[pull$condo_strata_10 == i], na.rm = TRUE)
  pull$strata_10_maxsp[pull$condo_strata_10 == i] <- max(pull$mean_sale_price[pull$condo_strata_10 == i], na.rm = TRUE)
}

for (i in 1:100){
  pull$strata_100_age[pull$condo_strata_100 == i] <- median(pull$age[pull$condo_strata_100 == i], na.rm = TRUE)
  pull$strata_100_minsp[pull$condo_strata_100 == i] <- min(pull$mean_sale_price[pull$condo_strata_100 == i], na.rm = TRUE)
  pull$strata_100_medsp[pull$condo_strata_100 == i] <- median(pull$mean_sale_price[pull$condo_strata_100 == i], na.rm = TRUE)
  pull$strata_100_maxsp[pull$condo_strata_100 == i] <- max(pull$mean_sale_price[pull$condo_strata_100 == i], na.rm = TRUE)
}

# clean up
pull[pull=="NaN"]<-pull[pull=="Inf"]<-pull[pull=="-Inf"]<-NA
pull <- pull[order(pull$mean_sale_price),]

# add some aggregate stats
stats_10 <- subset(dplyr::select(pull, condo_strata_10, strata_10_age, strata_10_minsp, strata_10_medsp, strata_10_maxsp), duplicated(condo_strata_10) == FALSE & !is.na(condo_strata_10))
stats_10$strata_10_medsp <- round(stats_10$strata_10_medsp, 0)
stats_100 <- subset(dplyr::select(pull, condo_strata_100, strata_100_age, strata_100_minsp, strata_100_medsp, strata_100_maxsp), duplicated(condo_strata_100) == FALSE & !is.na(condo_strata_100))
stats_100$strata_100_medsp <- round(stats_100$strata_100_medsp, 0)

stats_10$ASSESSMENT_YEAR <- year
stats_100$ASSESSMENT_YEAR <- year

if (ask.user.yn.question("Do you want to overwrite the strata stat tables?") == TRUE) {

  dbWriteTable(CCAODATA, "DTBL_CONDOSTRATA_STATS10", stats_10, overwrite = TRUE)
  dbWriteTable(CCAODATA, "DTBL_CONDOSTRATA_STATS100", stats_100, overwrite = TRUE)

}

# export unassigned strata for review
if (file.exists(paste0("O:/CCAODATA/results/ad_hoc/missing_strata.xlsx"))) {
  print("Cannot overwrite an existing review sheet.")
}else{
  print("Review sheet created")
  write.xlsx2(subset(pull[order(pull$TOWN, pull$nbhd),], is.na(condo_strata_10)), file=paste0("O:/CCAODATA/results/ad_hoc/missing_strata.xlsx"), showNA=FALSE, sheetName = "No Sales", row.names = FALSE)
  write.xlsx2(stats_10,  file=paste0("O:/CCAODATA/results/ad_hoc/missing_strata.xlsx"), showNA=FALSE, sheetName = "Strata Stats, 10", append = TRUE, row.names = FALSE)
  write.xlsx2(stats_100, file=paste0("O:/CCAODATA/results/ad_hoc/missing_strata.xlsx"), showNA=FALSE, sheetName = "Strata Stats, 100", append = TRUE, row.names = FALSE)
}

# drop buildings with missing strata and combine with manually assigned strata
pull <- rbind(subset(pull, !is.na(condo_strata_10)), subset(read.xlsx2(file=paste0("O:/CCAODATA/results/ad_hoc/missing_strata.xlsx"), sheetName = "No Sales", colClasses = lapply(pull, class)), !is.na(condo_strata_10)))

# only keep the columns we care about for upload, formatting
pull <- dplyr::select(pull, c("PIN10", "condo_strata_10", "condo_strata_100"))
pull$PIN10 <- gsub("-", "", pull$PIN10)
pull$ASSESSMENT_YEAR <- year
pull$TAX_YEAR <- NULL

# add on condos that are just missing from data and need to be added manually
if(file.exists(paste0("O:/CCAODATA/data/bad_data/missing_condos_", year, ".txt"))) {
  
  temp <- read_delim(paste0("O:/CCAODATA/data/bad_data/missing_condos_", year, ".txt"), delim = "|")
  temp <- subset(temp, temp$PIN10 %ni% pull$PIN10)
  
  pull <- rbind(pull, temp)
  
}

# make sure not to overwrite any old data
check <- dbGetQuery(CCAODATA, paste0("
SELECT * FROM DTBL_CONDOSTRATA
"))

check <- subset(check, ASSESSMENT_YEAR != year | (ASSESSMENT_YEAR == year & check$PIN10 %ni% pull$PIN10))
pull <- rbind(check, pull)
  
# export main condo strata table
dbWriteTable(CCAODATA, "DTBL_CONDOSTRATA", pull, row.names = FALSE, overwrite = TRUE)

# disconnect after pulls
dbDisconnect(CCAODATA)