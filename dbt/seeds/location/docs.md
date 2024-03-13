# neighborhood_groups

{% docs seed_neighborhood_groups %}
A mapping of neighborhood codes to groups that are used for sales
validation.

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
