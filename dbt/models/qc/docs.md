# vw_change_in_high_low_value_sales

{% docs view_vw_change_in_high_low_value_sales %}
Test that the number of low and high value sales from `iasworld.sales` and
`sale.mydec` do not change dramatically YoY.

{% enddocs %}

# vw_class_mismatch

{% docs view_vw_class_mismatch %}
Test that classes match between tables.

Pulled from the following Inquire queries:

- `FP Checklist--Bldg and parcel class mismatch` (#2161) - Will C.
{% enddocs %}

# vw_incorrect_asmt_value

{% docs view_vw_incorrect_asmt_value %}
Test that no `ASMT` critical column values are wrong.

Pulled from the following Inquire queries:

- `FP Checklist - Non-EX, RR parcels with 0 land value` (#1578) - Will C.
- `FP Checklist - Non-EX, RR PINs with 0 value` (#1570) - Will C.
- `FP Checklist - Vacant Class, bldg value` (#1577) - Will C.
- `FP Checklist - Improved class, no bldg value` (#1047) - Will C.
- `FP Checklist - Class does not equal LUC` (#2046) - Will C.
- `FP Checklist - 500k increase, 1m decrease` (#1673) - Will C.
{% enddocs %}

# vw_incorrect_val_method

{% docs view_vw_incorrect_val_method %}
Test that no residential parcels are using a cost approach.

Pulled from the following Inquire queries:

- `FP Checklist - Res parcels not set to Cost Approach` (#1578) - Will C.
{% enddocs %}

# vw_neg_asmt_value

{% docs view_vw_neg_asmt_value %}
Test that all `ASMT.val*` columns are not negative.

Pulled from the following Inquire queries:

- `FP Checklist - Negative ASMT Values` (#1569) - Will C.
{% enddocs %}

# vw_iasworld_sales_null_values

{% docs view_vw_iasworld_sales_null_values %}
Test that deed, buyer, seller, and price `iasworld.sales` columns are not null.
{% enddocs %}

# vw_iasworld_sales_rowcount_matches_sale_mydec

{% docs view_vw_iasworld_sales_rowcount_matches_sale_mydec %}
Test that `iasworld.sales` and `sale.mydec` have similar row counts.
{% enddocs %}

# vw_iasworld_sales_unmatched_joins_sale_mydec

{% docs view_vw_iasworld_sales_unmatched_joins_sale_mydec %}
Identify years with large numbers of unmatched sales between `sale.mydec` and
`iasworld.sales`.

{% enddocs %}

# vw_iasworld_sales_day_of_month

{% docs view_vw_iasworld_sales_day_of_month %}
Test if sale dates are concentrated to particular days of the month in
`iasworld.sales`.

{% enddocs %}

# vw_iasworld_sales_high_value_by_class

{% docs view_vw_iasworld_sales_high_value_by_class %}
Test if selected classes have sales prices greater than $20,000,000 in
`iasworld.sales`.

{% enddocs %}

# vw_iasworld_sales_price_diff_sale_mydec

{% docs view_vw_iasworld_sales_price_diff_sale_mydec %}
Test if prices in `iasworld.sales` and `sale.mydec` for matched sales are
different.

{% enddocs %}

# vw_pardat_nbhd_town_mismatch

{% docs view_vw_pardat_nbhd_town_mismatch %}
Test if neighborhood codes in `iasworld.pardat` have a township prefix
that matches the township code for the parcel in `iasworld.legdat`.

{% enddocs %}

# vw_sale_mydec_null_values

{% docs view_vw_sale_mydec_null_values %}
Test that deed, buyer, seller, address and price `sale.mydec` columns are not
null.

{% enddocs %}
