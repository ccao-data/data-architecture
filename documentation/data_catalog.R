# Load necessary libraries
library(dplyr)
library(jsonlite)
library(openxlsx)
library(tidyr)

# Data catalog path
catalog_path <- "xxx"

# Load workbook template
wb <- loadWorkbook(catalog_path)

# VIEWS ----

# Ingest views sheet
views <- read.xlsx(catalog_path, sheet = "Views") %>%
  select(-c('open_data_portal_table', 'open_data_portal_url', 'open_data_portal_update_frequency', 'open_data_portal_data_last_updated','open_data_portal_metadata_last_updated'
  ))

# Gather list of open data URNs
urns <- views %>%
  filter(!is.na(open_data_portal_urn)) %>%
  pull(open_data_portal_urn)

writeData(
  wb,
  "Views",

  views %>% left_join(

    lapply(urns, function(x) {

      jsonlite::read_json(
        paste0('https://datacatalog.cookcountyil.gov/api/views/metadata/v1/', x)
      ) %>%
        unlist(.)

    }) %>%
      bind_rows() %>%
      mutate(
        across(ends_with("UpdatedAt"), ~ substr(.x, 1, 10))
      ) %>%
      select(
        'open_data_portal_table' = 'name',
        'open_data_portal_url' = 'webUri',
        'open_data_portal_update_frequency' = 'customFields.Publishing Details.Publishing frequency',
        'open_data_portal_data_last_updated' = 'dataUpdatedAt',
        'open_data_portal_metadata_last_updated' = 'metadataUpdatedAt'
      ) %>%
      cbind('open_data_portal_urn' = urns)

  )

  )

saveWorkbook(wb, "xxx", overwrite = TRUE)

