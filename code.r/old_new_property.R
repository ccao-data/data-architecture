data <- dbGetQuery(CCAODATA, paste0("
with cur_sfchars as (select DISTINCT PIN as pin1, AGE as cur_age1, CLASS as cur_class1 FROM CCAOSFCHARS WHERE TAX_YEAR = 2018), 
old_sfchars as (SELECT DISTINCT PIN as pin2, AGE as old_age1, CLASS as old_class1 FROM CCAOSFCHARS WHERE TAX_YEAR = 2017),
cur_detail as (select DISTINCT DT_PIN as pin3, DT_AGE as cur_age2, DT_CLASS as cur_class2 FROM DETAIL WHERE TAX_YEAR = 2018 AND DT_AGE > 1 AND DT_ASS_VAL_DET > 0), 
old_detail as (SELECT DISTINCT DT_PIN as pin4, DT_AGE as old_age2, DT_CLASS as old_class2 FROM DETAIL WHERE TAX_YEAR = 2017 and DT_AGE > 1 AND DT_ASS_VAL_DET > 0), age_class_tbl as
(select DISTINCT HD_PIN,
CASE WHEN cur_age1 is not null then cur_age1
WHEN cur_age1 is null and cur_age2 is not null then cur_age2
ELSE NULL END as current_age,
CASE WHEN old_age1 is not null then old_age1
WHEN old_age1 is null and old_age2 is not null then old_age2
ELSE NULL END as old_age,
CASE WHEN cur_class1 is not null then cur_class1
WHEN cur_class1 is null and cur_class2 is not null then cur_class2
ELSE NULL END as cur_class,
CASE WHEN old_class1 is not null then old_class1
WHEN old_class1 is null and old_class2 is not null then old_class2
ELSE NULL END as old_class
FROM HEAD
left join cur_sfchars on HEAD.HD_PIN = cur_sfchars.pin1
left join old_sfchars on HEAD.HD_PIN = old_sfchars.pin2
left join cur_detail on HEAD.HD_PIN = cur_detail.pin3
left join old_detail on HEAD.HD_PIN = old_detail.pin4
WHERE TAX_YEAR = 2018),
full_pop as (select HD_PIN as main_pin, LEFT(HD_TOWN, 2) AS TOWN, HD_CLASS, (HD_ASS_LND + HD_ASS_BLD) AS HD_CURRENT_AV, (HD_PRI_LND + HD_PRI_BLD) AS HD_PAST_AV, TAX_YEAR
from HEAD WHERE TAX_YEAR = 2018 AND LEFT(HD_CLASS, 1) = 2),
all_data_joined as (select * from full_pop left join age_class_tbl on full_pop.main_pin = age_class_tbl.HD_PIN 
WHERE HD_CLASS NOT IN (200, 201, 241, 288, 290, 297))
select main_pin, TOWN, HD_CLASS, HD_CURRENT_AV, HD_PAST_AV, TAX_YEAR, old_age, current_age, old_class, cur_class,
CASE WHEN HD_PAST_AV = 0 THEN 'NEW'
WHEN old_age > 10 AND current_age < 10 THEN 'NEW'
WHEN cur_class != old_class THEN 'NEW'
ELSE 'OLD' END AS property_status
FROM all_data_joined"
))

result <- data.frame(statistic = character(), prior_year = numeric(), current_year = numeric(), n_pins = numeric())
for (i in seq(1:3)){
  if(i == 1){
    cur_data <- data
    stat_type <- "All Property"
  } else if (i == 2){
    cur_data <- data %>% filter(property_status == "NEW")
    stat_type <- "New Property"
  } else if (i == 3){
    cur_data <- data %>% filter(property_status == "OLD")
    stat_type <- "Old Property"
  }
  total_val <- cur_data %>% summarise(prior_year = sum(HD_PAST_AV), current_year = sum(HD_CURRENT_AV), n_pins = n())
  sf_val <- cur_data %>% filter(HD_CLASS %in% c(202, 203, 204, 205, 206, 207, 208, 209, 210, 234, 278, 295)) %>% summarise(prior_year = sum(HD_PAST_AV), current_year = sum(HD_CURRENT_AV), n_pins = n())
  mf_val <- cur_data %>% filter(HD_CLASS %in% c(211, 212)) %>% summarise(prior_year = sum(HD_PAST_AV), current_year = sum(HD_CURRENT_AV), n_pins = n())
  condo_val <- cur_data %>% filter(HD_CLASS %in% c(299)) %>% summarise(prior_year = sum(HD_PAST_AV), current_year = sum(HD_CURRENT_AV), n_pins = n())
  total_val$statistic <- paste("Total 200 class value", stat_type)
  sf_val$statistic <- paste("Single-Family class value", stat_type)
  mf_val$statistic <- paste("Multi-Family class value", stat_type)
  condo_val$statistic <- paste("Condo class value", stat_type)
  result <- rbind(result, total_val, sf_val, mf_val, condo_val)
  }
