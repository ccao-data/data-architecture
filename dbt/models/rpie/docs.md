# pin_codes

{% docs table_pin_codes %}
Generated RPIE codes for each PIN.

Used for identifying and filing RPIE forms.

**Primary Key**: `pin`, `year`
{% enddocs %}

# pin_codes_dummy

{% docs table_pin_codes_dummy %}
Table of test RPIE codes to open test/stub filings.

**Primary Key**: `pin`, `year`
{% enddocs %}

# vw_code_retrieval

{% docs view_vw_code_retrieval %}
View for RPIE code retrieval application. Joins mailing address to PIN.

**Primary Key**: `pin`, `year`
{% enddocs %}

# vw_pin_flatfile

{% docs view_vw_pin_flatfile %}
View to extract RPIE codes for entry into the RPIE database.

**Primary Key**: `pin`, `rpie_year`
{% enddocs %}

# vw_pin_mailers

{% docs view_vw_pin_mailers %}
View to support mailing RPIE codes to taxpayers. Used for vendor mail merge.

**Primary Key**: `pin`, `rpie_year`
{% enddocs %}
