# municipality_crosswalk

{% docs seed_municipality_crosswalk %}
A list of semantic discrepancies between `cook_municipality_name` and `tax_municipality_name`

**Primary Key**: `cook_municipality_name', 'tax_municipality_name`
{% enddocs %}

# neighborhood_group

{% docs seed_neighborhood_group %}
A mapping of neighborhood codes to groups that are used for sales
validation.

These mappings are defined by the Valuations team and then delivered to the
Data team in the form of an Excel workbook. The Data team then manually
reformats the workbook and saves it as a CSV so that it can populate the data
warehouse as a dbt seed. See the `Nuance` section below for detailed
instructions on how to make changes to the CSV file.

### Nuance

- The mapping table contains two rows that are used for versioning: `version` and
  `updated_at`. The combination of these two columns allows us to reproduce the
  state of the neighborhood groups at a specific moment in time. To do so, query
  for all records where `updated_at` is below a certain date, group by `nbhd`,
  and select the record with the highest value for `version` within the group.
- Instructions for how to update the seed that generates this table:
  - When **adding new neighborhoods**, append the rows to the CSV file with a
    `version` of 1
  - When **updating neighborhoods that already have a group**, append the rows
    to the CSV file and increment the `version` by 1
  - In the case of **all new rows**, set the `updated_at` column to the current
    date

**Primary Key**: `nbhd`, `version`
{% enddocs %}