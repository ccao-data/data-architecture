library(ggplot2)
library(tidyverse)
library(dplyr)


format_data <- function(v, monthly_rent_per_uni){
  
  if (percent == FALSE){  
    
    paste0(v[1] , " - ",  v[2])
    
  } else {
    
    paste0(v[1]*100, "%" , " - ",  v[2]*100, "%")
    
    }
    
}


# LOAD DATA
load(file = "reporting_data.rda")
names(gpi) 


## TOTAL NUMBER OF PROPERTIES --
total_property_number <- length(unique(gpi$key_pin))+ length(unique(modeling_data$buildingid))


# UNITS PER PROPERTIES (RANGE) -- 

unit_building <- modeling_data %>% 
  group_by(buildingid) %>%
  summarize(sum(units)) %>% 
  rename("Property Identifier" = "buildingid"  )

unit_Pin <- gpi %>%  
  group_by(key_pin) %>%
  summarize(sum(units))  %>%  
  rename("Property Identifier" = "key_pin"  )

unit_per_building_calculation <- rbind(unit_building, unit_Pin)
unit_per_building <- format_data(c(min(unit_per_building_calculation$`sum(units)`, na.rm =T), max(unit_per_building_calculation$`sum(units)`, na.rm =T)))


# SQUARE FEET PER UNIT(RANGE) --

gpi_sq_feet <- gpi %>% 
  group_by(key_pin) %>%
  summarize(sum(mean_square_feet/units)) %>%
  rename("Property Identifier" = "key_pin", "sq_feet" ="sum(mean_square_feet/units)"  )

modeling_sq_feet <- modeling_data %>%
  group_by(buildingid) %>%
  summarize(sum(square_feet/units)) %>%
  rename("Property Identifier" = "buildingid", "sq_feet" ="sum(square_feet/units)"  )

square_feet_per_unit_calculation <- rbind(modeling_sq_feet, gpi_sq_feet)

square_feet_per_unit <- format_data(c(min(square_feet_per_unit_calculation$sq_feet, na.rm =T), max(square_feet_per_unit_calculation$sq_feet, na.rm =T)))


# MONTHLY RENT PER UNITS (RANGE)
monthly_rent_gpi <- gpi %>%
  group_by(key_pin) %>%
  summarize(sum(mean_monthly_rent/units)) %>%
  # summarize(sum(mean_predicted_market_rent/units)) %>%
  rename("Property Identifier" = "key_pin" , 'monthly_rent' = 'sum(mean_monthly_rent/units)' )

monthly_rent_modeling <- modeling_data %>%
  group_by(buildingid) %>%
  summarize(sum(monthly_rent/units)) %>%
  rename("Property Identifier" = "buildingid" , 'monthly_rent' = 'sum(monthly_rent/units)' ) 
  

monthly_rent_per_unit_calculation <- rbind(monthly_rent_gpi, monthly_rent_modeling)

monthly_rent_per_unit <- format_data(c(min(monthly_rent_per_unit_calculation$monthly_rent, na.rm =T), max(monthly_rent_per_unit_calculation$monthly_rent, na.rm =T)), percent = FALSE)
monthly_rent_per_unit

# VACANCY RATE (RANGE)  --- Validate Vacancy rate need to be % and range from 0 -1 ?
vacancy_rate_gpi <- gpi %>% 
  filter(vacancy_rate >= 0) %>%
  group_by(key_pin) %>% 
  summarize(round(sum(vacancy_rate), 2)) %>%
  rename("Property Identifier" = "key_pin" , 'vacancy_rate' = 'round(sum(vacancy_rate), 2)' ) %>%
  filter(vacancy_rate >0 & vacancy_rate <= 1)


vacancy_rate_modeling <- modeling_data %>%  
  group_by(buildingid) %>%
  summarise(round(sum(months_vacant/12), 2)) %>%
  rename("Property Identifier" = "buildingid" , 'vacancy_rate' = 'round(sum(months_vacant/12), 2)' ) %>%
  filter(vacancy_rate >0 & vacancy_rate <= 1)


vacancy_rate_calculated <- rbind(vacancy_rate_gpi, vacancy_rate_modeling)
vacancy_rate <- format_data(c(min(vacancy_rate_calculated$vacancy_rate, na.rm =T), max(vacancy_rate_calculated$vacancy_rate, na.rm =T)), percent = TRUE  )

vacancy_rate

# EXPENSE RATIO (RANGE) 
expense_ratio <- format_data(round(c(min(gpi$expense_ratio, na.rm =T), max(gpi$expense_ratio, na.rm =T)), 2), percent = TRUE )

# CAP RATE(RANGE) 
cap_rate <- format_data(round(c(min(gpi$mean_cap_rate, na.rm =T), max(gpi$mean_cap_rate, na.rm =T)), 2), percent = TRUE)


# ESTIMATED VALUES/UNIT (RANGE) ???
estimated_val_unit_gpi <- gpi %>%
  group_by(key_pin) %>%
  summarise(round(sum( (gpi * (1-vacancy_rate) * (1-predicted_non_payment_rate) * (1-expense_ratio) * (1/mean_cap_rate)) /units), 2)) %>%
  rename("Property Identifier" = "key_pin" , 'estimated_val_unit' = 'round(...)' )

# How to use modeling_data to calculate estimated value?
names(modeling_data)

Estimated_val_per_unit_calculation <- rbind(estimated_val_unit_gpi)

Estimated_val_per_unit <- format_data(round(c(min(Estimated_val_per_unit_calculation$estimated_val_unit, na.rm =T), max(Estimated_val_per_unit_calculation$estimated_val_unit, na.rm =T)), 2) )

Estimated_val_per_unit

# TABLE 3 --- create a data frame with the reported value

tbl3 <- data.frame("Num of Properties" = total_property_number, 
                    "Sqr feet Per Unit(Range)"=square_feet_per_unit,
                   "Units per building(Range)" = unit_per_building,
                   "Monthly Rent/unit(Range)" = monthly_rent_per_unit,
                  "Vacancy Rate(Range)" = vacancy_rate,
                  "Expense Ratio(Range)" = expense_ratio,
                  "Cap Rate(Range)" = cap_rate,
                  "Estimated Value/unit(Range)" = Estimated_val_per_unit
                   )
tbl3


# KABLE library to create table visualizations --
library(kableExtra)

kbl(tbl3, booktabs = T)

kable(tbl3)
add_footnote(x, c("footnote 1", "footnote 2"), notation = "symbol")






























