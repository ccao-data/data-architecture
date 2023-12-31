---
name: Data Onboarding
about: Filled out when the ingest of a new data source is scripted.
title: ''
labels: ''
assignees: ''

---

# Data Onboarding

After filling out this issue template: onboard the data, close the issue, and add the necessary information to the department's [data catalog](https://gitlab.com/groups/ccao-data-science---modeling/-/wikis/Data/data_catalog.xlsx).

### Description

(What is this data for? Briefly describe the intended purpose of this data and why it's being added to the department's data architecture.)

### Origin

- [ ] Internal
- [ ] External

(Detail where the data is from - provide a URL and organization name if possible, or a contact email address if provided by a collaborator. If it's internal data, provide the system of origin, filepath, or department from which it originates.)

### Documentation

(If the data has documentation, provide its source.)

### Date Retrieved

(The date the data was initially received or retrieved from the source noted above. MM-DD-YYYY.)

### Reproducibility 

- [ ] Reproducible (How was the data retrieved? API? R Package? Please note the method.)
- [ ] Bespoke

### Data Type

- [ ] Spatial (.GeoJSON)
- [ ] Document (.doc, .pdf)
- [ ] Columnar (.JSON, .parquet)
- [ ] Image (.png, .jpeg)

### Date Lifecycle

- [ ] Static
- [ ] Updated regularly/irregularly
    - [ ] Daily
    - [ ] Weekly
    - [ ] Monthly
    - [ ] Yearly
    - [ ] Irregularly

### Ingestion and Cleaning Scripts

If the data is ingested and/or cleaned programatically note the repository URL of the scripts responsible for those tasks below:

 - Ingestion script location:
 - Cleaning script location:

### Intended Storage Location

If the data will live in the department's S3 buckets, provide S3 URLs below:

- Warehouse S3 URL:
- Data Lake S3 URL:

If will live elsewhere, provide a filepath and a reason it won't be on S3:

- Non-S3 filepath:
- Reason for non-S3 location:
