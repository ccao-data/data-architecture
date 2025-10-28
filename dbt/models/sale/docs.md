# flag

{% docs flag %}
PIN-level sales validation flags created by
[model-sales-val](https://github.com/ccao-data/model-sales-val).

This is the primary sales validation output table. Flags within this table
should be possible to reconstruct using the other sales validation tables:
`sale.group_mean`, `sale.parameter`, and `sale.metadata`.

**Primary Key**: `meta_sale_document_number`, `run_id`, `version`
{% enddocs %}

# foreclosure

{% docs foreclosure %}
Foreclosure data ingested from Illinois Public Records (RIS).

**Primary Key**: `pin`, `document_number`
{% enddocs %}

# group_mean

{% docs group_mean %}
Information about groups used to calculate statistical deviations
for sales validation.

**Primary Key**: `run_id`, `group`
{% enddocs %}

# metadata

{% docs metadata %}
Information about the code used for a sales validation run, as well as
the start time and type of run.

**Primary Key**: `run_id`
{% enddocs %}

# mydec

{% docs mydec %}
MyDec data from the Illinois Department of Revenue (IDOR). Includes property
transfer declarations (sales) used to fill in missing data in `iasworld.sales`
and as an input to sales validation flagging.

**Primary Key**: `document_number`, `year_of_sale`
{% enddocs %}

# parameter

{% docs parameter %}
Parameters used for each run of
[model-sales-val](https://github.com/ccao-data/model-sales-val),
including the statistical bounds, groupings, window sizes, etc.

**Primary Key**: `run_id`
{% enddocs %}

# vw_flag_group

{% docs vw_flag_group %}

**Primary Key**: `run_id`, `meta_sale_document_num`
{% enddocs %}

# vw_ias_salesval_upload

{% docs vw_ias_salesval_upload %}
View for sales validation outputs to create an upload format compatible
with iasWorld.

**Primary Key**: `salekey`, `run_id`
{% enddocs %}
