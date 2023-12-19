# acs1

{% docs table_acs1 %}
American Community Survey 1-year estimates for variables of interest, at many
levels of geography (tract, block group, etc.).

**Primary Key**: `geoid`, `year`, `geography`
{% enddocs %}

# acs5

{% docs table_acs5 %}
American Community Survey 5-year estimates for variables of interest, at many
levels of geography (tract, block group, etc.).

Does not include all ACS variables, only ones the Data Department would be
useful for modeling and reporting e.g. median income, age breakdowns, etc.

**Primary Key**: `geoid`, `year`, `geography`
{% enddocs %}

# decennial

{% docs table_decennial %}
Decennial Census estimates for variables of interest, at many
levels of geography (tract, block group, etc.).

**Primary Key**: `geoid`, `year`, `geography`
{% enddocs %}

# table_dict

{% docs table_table_dict %}
Decennial and ACS table name dictionary.

For example, given the Census variable `B01001_001E`, one can look up the
table code `B01001` and find that the title of the table `B01001`
is `Sex by Age`.

**Primary Key**: `variable_table_code`
{% enddocs %}

# variable_dict

{% docs table_variable_dict %}
Individual variable labels for all ACS and decennial Census variables.

For example, looking up the variable name `B01001_002` will yield the label
`Estimate!!Total!!Male` i.e. the total male population for a given Census
geography.

**Primary Key**: `variable_name`, `survey`
{% enddocs %}

# vw_acs5_stat

{% docs view_vw_acs5_stat %}
View to make raw Census data into useable features for modeling and reporting.

Contains things such as median income, household size, race, sex, educational
attainment, and more. All broken out by different levels of geography. Uses
ACS5 data as a base. See individual features for Census variables used.

**Primary Key**: `year`, `geoid`, `geography`
{% enddocs %}
