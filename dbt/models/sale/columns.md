## group_id
{% docs column_group_id %}
Group string used as a unique identifier.

Typically a combination of a:
- geographic component (township/nbhd groupings)
- characteristic component (class, sqft, age)
- temporal range (e.g., previous year)
{% enddocs %}

## group_size
{% docs column_group_size %}
Count of sales/properties in the group.
This is the population within which price statistics (e.g., standard deviation) are calculated.
{% enddocs %}

## min_group_thresh
{% docs column_min_group_thresh %}
Minimum number of sales required for statistical flagging.

If a group’s `group_size` is below this threshold, the sale is not flagged (set to “Not outlier”).  
**Caveat:** Sales with certain PTAX-203 line-10 items marked may still be classified as outliers.
{% enddocs %}

## rolling_window
{% docs column_rolling_window %}
Rolling window period used to compute grouping statistics for sale flagging.

Defined as *N* months prior, **inclusive of the month of the sale**.  
As of Sep 2024, this is approximately 12 months (sale month + prior 11 months).
{% enddocs %}
