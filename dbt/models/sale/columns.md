## earliest_data_ingest

{% docs column_earliest_data_ingest %}
Date of earliest sale used in validation.

This range is inclusive of the rolling window period used for calculating
statistical groups. In other words, if the earliest sale to-be-flagged is
2013-12-01 and the rolling window period is 9 months, then the earliest sale
*used* (i.e. the `earliest_data_ingest`) would be 2013-03-01.
{% enddocs %}

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

This is the number of observations in a population within which price statistics
(e.g., standard deviation) are calculated.
{% enddocs %}

## housing_market_class_codes

{% docs column_housing_market_class_codes %}
List of class codes that define each housing market type
{% enddocs %}

## iso_forest_cols

{% docs column_iso_forest_cols %}
Columns used as features in the isolation forest model
{% enddocs %}

## latest_data_ingest

{% docs column_latest_data_ingest %}
Date of the latest sale pulled by the ingest query
{% enddocs %}

## min_group_thresh

{% docs column_min_group_thresh %}
Minimum number of sales required for statistical flagging.

If a group’s `group_size` is below this threshold, the sale is not flagged
(set to “Not outlier”).

**Caveat:** Sales with certain PTAX-203 line-10 items marked may still be
classified as outliers.
{% enddocs %}

## raw_price_threshold

{% docs column_raw_price_threshold %}
Upper bound price beyond which all sales are flagged as outliers, independent
of any standard deviation or group size thresholds
{% enddocs %}

## requires_field_check

{% docs column_requires_field_check %}
Analyst determination on whether we need to send someone from the office out
to collect further information.

True in this column suggests that the analyst believes there has been completed
work on the home.
{% enddocs %}

## rolling_window

{% docs column_rolling_window %}
Rolling window period used to compute grouping statistics for sale flagging.

Defined as *N* months prior, **inclusive of the month of the sale**.  
As of Sep 2024, this is approximately 12 months (sale month + prior 11 months).
{% enddocs %}

## run_filter

{% docs column_run_filter %}
Filters that defined the sales that this run considered for flagging, e.g.
housing market type and/or triad
{% enddocs %}

## sales_flagged

{% docs column_sales_flagged %}
Total number of sales flagged.

Inclusive of both sales flagged as outliers *and* sales flagged as non-outliers.
{% enddocs %}

## sales_to_write_filter

{% docs column_sales_to_write_filter %}
Optional filter that determines which sales to write.

Any sales that match `run_filter` but not this filter will still get flagged,
but those flags will not get written to the database.
{% enddocs %}

## short_term_owner_threshold

{% docs column_short_term_owner_threshold %}
Threshold that determines the number of days since a property's previous sale at
which the seller is considered to be a "short-term owner".

Sales under this threshold receive the "Short-term owner" characteristic reason
in one of the `sv_outlier_reason` fields.
{% enddocs %}

## standard_deviation_bounds

{% docs column_standard_deviation_bounds %}
Boundaries for standard deviation flagging.

Sales with prices beyond these boundaries are flagged.

There are two types of bounds:

- `standard_bounds`: The default bounds
- `ptax_bounds`: Bounds used specifically for sales that have a PTAX-203 flag,
  typically a narrower range than `standard_bounds` because these sales are
  more likely to be outliers
{% enddocs %}

## stat_groups

{% docs column_stat_groups %}
Groups used to calculate flagging statistics (std. dev.), keyed by triad name
and housing market type (res or condo)
{% enddocs %}

## time_frame

{% docs column_time_frame %}
Start and end dates for the window of sales to flag.

In contrast to `earliest_data_ingest`, which includes sales that are not up for
flagging but are necessary to construct the rolling window, this date range
only includes sales that are up for flagging.
{% enddocs %}
