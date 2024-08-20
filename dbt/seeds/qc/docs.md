# iasworld_pardat_existing_spot_not_null

{% docs seed_iasworld_pardat_existing_spot_not_null %}
Table containing parids for parcels in `iasworld.pardat` that have a not-null
value for the `spot` field in the 2024 data.

We use this table as a reference when alerting for new rows that have been added
with a not-null `spot` value.

**Primary key**: `parid`
{% enddocs %}
