# Run all scripts 
lapply(
  list.files(here("scripts-ccao-landing-us-east-1"), full.names = TRUE),
  source
)
