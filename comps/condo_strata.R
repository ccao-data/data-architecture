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

# export sales universe
sales_universe <- merge(sales_universe, subset(dplyr::select(pull, c("PIN10", "condo_strata_10")), duplicated(PIN10) == FALSE & !is.na(condo_strata_10)), by = "PIN10")
sales_universe <- sales_universe[order(sales_universe$condo_strata_10, sales_universe$TOWN, sales_universe$NBHD, sales_universe$PIN10, -as.numeric(sales_universe$SALE_DATE)),]

#write.xlsx2(sales_universe, file=paste0("O:/CCAODATA/results/ad_hoc/condo_strata.xlsx"), showNA=FALSE, sheetName = "Sales Universe", row.names = FALSE)

# add buildings with strata determined by analyst and export assigned strata for analyst use
if(file.exists(paste0("O:/CCAODATA/results/ad_hoc/missing_strata.xlsx"))){
temp <- rbind(subset(pull, !is.na(condo_strata_10)), subset(read.xlsx2(file=paste0("O:/CCAODATA/results/ad_hoc/missing_strata.xlsx"), sheetName = "No Sales", colClasses = lapply(pull, class)), !is.na(condo_strata_10)))
buildings_number <- nrow(subset(temp, !is.na(condo_strata_10)))
}else{
  temp <- subset(pull, !is.na(condo_strata_10))
  buildings_number <- nrow(subset(temp, !is.na(condo_strata_10)))
}

#write.xlsx2(temp, file=paste0("O:/CCAODATA/results/ad_hoc/condo_strata.xlsx"), showNA=FALSE, sheetName = "Mean Sale Price", append = TRUE, row.names = FALSE)
rm(temp)

# add some aggregate stats
stats_10 <- subset(dplyr::select(pull, condo_strata_10, strata_10_age, strata_10_minsp, strata_10_medsp, strata_10_maxsp), duplicated(condo_strata_10) == FALSE & !is.na(condo_strata_10))
stats_10$strata_10_medsp <- round(stats_10$strata_10_medsp, 0)
stats_100 <- subset(dplyr::select(pull, condo_strata_100, strata_100_age, strata_100_minsp, strata_100_medsp, strata_100_maxsp), duplicated(condo_strata_100) == FALSE & !is.na(condo_strata_100))
stats_100$strata_100_medsp <- round(stats_100$strata_100_medsp, 0)

# write.xlsx2(stats_10, file=paste0("O:/CCAODATA/results/ad_hoc/condo_strata.xlsx"), showNA=FALSE, sheetName = "Strata Stats", append = TRUE, row.names = FALSE)

# create interface and camera sheets
interface <- data.frame(t(c('Input Pin:', NA, ' ', 'Buildings in same strata', 'Unit with most recent sale', 'Sale price', 'Sale date', 'Doc No', 'Unit Address', 'Town', 'Neighborhood')))
interface <- rbind.fill(interface, data.frame(t(c(NA, NA, NA
                                , paste0('=IFERROR(OFFSET(INDEX(\'Mean Sale Price\'!$A$2:$A$',buildings_number+1,',AGGREGATE(15,6,ROW(\'Mean Sale Price\'!$R$2:$R$',buildings_number+1,')/(\'Mean Sale Price\'!$R$2:$R$',buildings_number+1,'=Camera!$B$2),ROW(\'Mean Sale Price\'!1:1))), -1, 0),"INPUT NOT FOUND")')
                                , paste0('=IFERROR(RIGHT(OFFSET(INDEX(\'Sales Universe\'!$B$2:$B$',number_sales + 1,',AGGREGATE(15,6,ROW(\'Sales Universe\'!$A$2:$A$',number_sales + 1,')/(\'Sales Universe\'!$A$2:$A$',number_sales + 1,'=D2),ROW($1:$1))), -1, 0),4),"INPUT NOT FOUND")')
                                , paste0('=IFERROR(DOLLAR(OFFSET(INDEX(\'Sales Universe\'!$N$2:$N$',number_sales + 1,',AGGREGATE(15,6,ROW(\'Sales Universe\'!$A$2:$A$',number_sales + 1,')/(\'Sales Universe\'!$A$2:$A$',number_sales + 1,'=D2),ROW($1:$1))), -1, 0),0),"INPUT NOT FOUND")')
                                , paste0('=IFERROR(TEXT(OFFSET(INDEX(\'Sales Universe\'!$F$2:$F$',number_sales + 1,',AGGREGATE(15,6,ROW(\'Sales Universe\'!$A$2:$A$',number_sales + 1,')/(\'Sales Universe\'!$A$2:$A$',number_sales + 1,'=D2),ROW($1:$1))), -1, 0),"mm/dd/yyyy"),"INPUT NOT FOUND")')
                                , paste0('=IFERROR(OFFSET(INDEX(\'Sales Universe\'!$H$2:$H$',number_sales + 1,',AGGREGATE(15,6,ROW(\'Sales Universe\'!$A$2:$A$',number_sales + 1,')/(\'Sales Universe\'!$A$2:$A$',number_sales + 1,'=D2),ROW($1:$1))), -1, 0),"INPUT NOT FOUND")')
                                , paste0('=IFERROR(OFFSET(INDEX(\'Sales Universe\'!$O$2:$O$',number_sales + 1,',AGGREGATE(15,6,ROW(\'Sales Universe\'!$A$2:$A$',number_sales + 1,')/(\'Sales Universe\'!$A$2:$A$',number_sales + 1,'=D2),ROW($1:$1))), -1, 0),"INPUT NOT FOUND")')
                                , paste0('=IFERROR(OFFSET(INDEX(\'Sales Universe\'!$D$2:$D$',number_sales + 1,',AGGREGATE(15,6,ROW(\'Sales Universe\'!$A$2:$A$',number_sales + 1,')/(\'Sales Universe\'!$A$2:$A$',number_sales + 1,'=D2),ROW($1:$1))), -1, 0),"INPUT NOT FOUND")')
                                , paste0('=IFERROR(OFFSET(INDEX(\'Sales Universe\'!$E$2:$E$',number_sales + 1,',AGGREGATE(15,6,ROW(\'Sales Universe\'!$A$2:$A$',number_sales + 1,')/(\'Sales Universe\'!$A$2:$A$',number_sales + 1,'=D2),ROW($1:$1))), -1, 0),"INPUT NOT FOUND")')))))

colnames(interface) <- LETTERS[1:11]

#write.xlsx2(interface, file=paste0("O:/CCAODATA/results/ad_hoc/condo_strata.xlsx"), showNA=FALSE, sheetName = "Interface", append = TRUE, row.names = FALSE, col.names=FALSE)

camera <- data.frame(t(c('Pin information', NA)))
camera <- rbind.fill(camera, data.frame(t(c('pin strata:', paste0('=IFERROR(VLOOKUP(LEFT(Interface!B1, 13),\'Mean Sale Price\'!A2:R',buildings_number+1,',18,FALSE), "PIN NOT FOUND")')))))
camera <- rbind.fill(camera, data.frame(t(c('pin town:', paste0('=IFERROR(VLOOKUP(LEFT(Interface!B1, 13),\'Mean Sale Price\'!A2:R',buildings_number+1,',17,FALSE), "PIN NOT FOUND")')))))
camera <- rbind.fill(camera, data.frame(t(c('pin neighborhood:', paste0('=IFERROR(VLOOKUP(LEFT(Interface!B1, 13),\'Mean Sale Price\'!A2:R',buildings_number+1,',16,FALSE), "PIN NOT FOUND")')))))
camera <- rbind.fill(camera, data.frame(t(c('percent assessed:', paste0('=IFERROR(TEXT(VLOOKUP(Interface!B1,\'Sales Universe\'!B2:I36391,8,FALSE), "0.00%"), "UNIT PIN NOT FOUND")')))))
camera <- rbind.fill(camera, data.frame(t(c(NA, NA))))
camera <- rbind.fill(camera, data.frame(t(c('Strata Stats', NA))))
camera <- rbind.fill(camera, data.frame(t(c('min sale price:', '=IFERROR(DOLLAR(VLOOKUP(B2,\'Strata Stats\'!A2:E11,3,FALSE),0), "PIN NOT FOUND")'))))
camera <- rbind.fill(camera, data.frame(t(c('median sale price:', '=IFERROR(DOLLAR(VLOOKUP(B2,\'Strata Stats\'!A2:E11, 4,FALSE),0), "PIN NOT FOUND")'))))
camera <- rbind.fill(camera, data.frame(t(c('max sale price:', '=IFERROR(DOLLAR(VLOOKUP(B2,\'Strata Stats\'!A2:E11, 5,FALSE),0), "PIN NOT FOUND")'))))
camera <- rbind.fill(camera, data.frame(t(c('number of sales:', paste0('=IFERROR(COUNTIF(\'Sales Universe\'!P2:P',number_sales + 1,',B2), "PIN NOT FOUND")')))))
camera <- rbind.fill(camera, data.frame(t(c(NA, NA))))
camera <- rbind.fill(camera, data.frame(t(c('= "Unit sales (last 5 | " & IFERROR(COUNTIF(\'Sales Universe\'!B2:B36391,Interface!B1), "-") & " total)"', NA))))
camera <- rbind.fill(camera, data.frame(t(c(paste0('=Interface!$B$1')
                                                , paste0('=IFERROR(TEXT(OFFSET(INDEX(\'Sales Universe\'!$F$2:$F$',number_sales + 1,',AGGREGATE(15,6,ROW(\'Sales Universe\'!$B$2:$B$',number_sales + 1,')/(\'Sales Universe\'!$B$2:$B$',number_sales + 1,'=Interface!$B$1),ROW(\'Sales Universe\'!1:1))), -1, 0),"mm/dd/yyyy"),"---")')
                                                , paste0('=IFERROR(DOLLAR(OFFSET(INDEX(\'Sales Universe\'!$N$2:$N$',number_sales + 1,',AGGREGATE(15,6,ROW(\'Sales Universe\'!$B$2:$B$',number_sales + 1,')/(\'Sales Universe\'!$B$2:$B$',number_sales + 1,'=Interface!$B$1),ROW(\'Sales Universe\'!1:1))), -1, 0),0),"---")')))))
for (i in 1:5) {camera <- rbind.fill(camera, data.frame(t(c(NA, NA))))}
camera <- rbind.fill(camera, data.frame(t(c('= "Sales in building (last 10 | " & IFERROR(COUNTIF(\'Sales Universe\'!A2:A36391,LEFT(Interface!B1, 13)), "-") & " total)"', NA))))
camera <- rbind.fill(camera, data.frame(t(c(paste0('=IFERROR(OFFSET(INDEX(\'Sales Universe\'!$B$2:$B$',number_sales + 1,',AGGREGATE(15,6,ROW(\'Sales Universe\'!$A$2:$A$',number_sales + 1,')/(\'Sales Universe\'!$A$2:$A$',number_sales + 1,'=LEFT(Interface!$B$1, 13)),ROW(\'Sales Universe\'!1:1))), -1, 0),"---")')
                                                 ,paste0('=IFERROR(TEXT(OFFSET(INDEX(\'Sales Universe\'!$F$2:$F$',number_sales + 1,',AGGREGATE(15,6,ROW(\'Sales Universe\'!$A$2:$A$',number_sales + 1,')/(\'Sales Universe\'!$A$2:$A$',number_sales + 1,'=LEFT(Interface!$B$1, 13)),ROW(\'Sales Universe\'!1:1))), -1, 0),"mm/dd/yyyy"),"---")')
                                                 ,paste0('=IFERROR(DOLLAR(OFFSET(INDEX(\'Sales Universe\'!$N$2:$N$',number_sales + 1,',AGGREGATE(15,6,ROW(\'Sales Universe\'!$A$2:$A$',number_sales + 1,')/(\'Sales Universe\'!$A$2:$A$',number_sales + 1,'=LEFT(Interface!$B$1, 13)),ROW(\'Sales Universe\'!1:1))), -1, 0),0),"---")')))))
for (i in 1:9) {camera <- rbind.fill(camera, data.frame(t(c(NA, NA))))}
camera <- rbind.fill(camera, data.frame(t(c('5 year median:', NA, paste0('=IFERROR(DOLLAR(VLOOKUP(LEFT(Interface!B1,13),\'Mean Sale Price\'!A2:I',buildings_number+1,',9,FALSE),0),"PIN NOT FOUND")')))))
camera <- rbind.fill(camera, data.frame(t(c(NA, NA))))
camera <- rbind.fill(camera, data.frame(t(c('Condo Strata Comp Finder version 1.0', NA))))
camera <- rbind.fill(camera, data.frame(t(c('enterprise-intelligence commit 9a1ef44', NA))))

colnames(camera) <- LETTERS[1:3]

camera <- dplyr::select(camera, c('A', 'B', 'C'))

#write.xlsx2(camera, file=paste0("O:/CCAODATA/results/ad_hoc/condo_strata.xlsx"), showNA=FALSE, sheetName = "Camera", append = TRUE, row.names = FALSE, col.names=FALSE)

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

# export for upload
# write.table(pull, paste0("O:/CCAODATA/data/files_for_server_upload/CONDO_STRATA_", year, ".txt"), sep = "|", na = "", quote = FALSE, row.names = FALSE)
