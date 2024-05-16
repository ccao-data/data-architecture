# CCAO Data Infrastructure

This repository stores the code for the CCAO Data Department's ETL
pipelines and data lakehouse. This infrastructure supports the Data Team's
modeling, reporting, and data integrity work.

## Quick Links

- [:file_folder: dbt Data Catalog](https://ccao-data.github.io/data-architecture/#!/overview) -
  Documentation for all CCAO data lakehouse tables and views
- [:nut_and_bolt: dbt README](/dbt/README.md) - How to develop CCAO data
  infrastructure using dbt
- [:ballot_box_with_check: dbt Test Documentation](/dbt/tests/generic/README.md) -
  Definitions for CCAO generic dbt data tests

## Repository Structure

- [./dbt](./dbt) contains the models and tests that build our Athena data lakehouse
- [./docs](./docs) contains design documents and other supplemental documentation
- [./etl](./etl) contains small ETL scripts used to load data in the lakehouse as
  dbt sources
- [./socrata](./socrata) contains column transformations for the CCAO's
  [Open Data Portal](https://datacatalog.cookcountyil.gov/) assets
