# CCAO Data Infrastructure

This repository stores the code for the CCAO Data Department's ETL
pipelines and data lakehouse. This infrastructure supports the Data Team's
modeling, reporting, and data integrity work.

## Quick Links

- [:file_folder: dbt Data Catalog](https://ccao-data.github.io/data-architecture/#!/overview) -
  Documentation for all CCAO data lakehouse tables and views
- [:nut_and_bolt: dbt README](/dbt/README.md) - How to develop CCAO data
  infrastructure using dbt
- [:test_tube: dbt Tests and QC Reports](dbt/README.md#-how-to-add-and-run-tests-and-qc-reports) -
  How to add and run data tests, unit tests, and QC reports using dbt
- [:pencil: dbt Generic Test Documentation](/dbt/tests/generic/README.md) -
  Definitions for CCAO generic dbt tests, which are functions that we use to define our QC tests

## Repository Structure

- [./dbt](./dbt) contains the models and tests that build our Athena data lakehouse;
  dbt mainly acts as a transformation and documentation layer on top of our raw data
- [./docs](./docs) contains design documents and other supplemental documentation
- [./etl](./etl) contains ETL scripts used to load raw and slightly cleaned up
  data into the lakehouse as dbt sources
- [./socrata](./socrata) contains column transformations for the CCAO's
  [Open Data Portal](https://datacatalog.cookcountyil.gov/) assets
