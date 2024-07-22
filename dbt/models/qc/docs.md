# vw_change_in_ahsap_values

{% docs view_vw_change_in_ahsap_values %}
Test whether AHSAP properties have had large increases in AV between two
stages. Only applies to most recent year of assessment roll.

For an explanation of AHSAP and insight into why it involves so many different
iasWorld tables, see: https://www.cookcountyassessor.com/affordable-housing

{% enddocs %}

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

# vw_incorrect_val_method

{% docs view_vw_incorrect_val_method %}
Test that no residential parcels are using a cost approach.

Pulled from the following Inquire queries:

- `FP Checklist - Res parcels not set to Cost Approach` (#1578) - Will C.
{% enddocs %}

# vw_neg_asmt_value

{% docs view_vw_neg_asmt_value %}
Pull `ASMT.val*` columns to support tests confirming they are not negative.

Pulled from the following Inquire queries:

- `FP Checklist - Negative ASMT Values` (#1569) - Will C.

In contrast to `qc.vw_report_neg_asmt_value`, this view does not perform
any filtering for negative values. That filtering is performed in tests
defined on the model.
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

# vw_nonlivable_condos_with_chars

{% docs view_vw_nonlivable_condos_with_chars %}
Collects nonlivable condo units with associated characteristics. Nonlivable
units should not have characteristics, so these discrepencies should be
investigated.

{% enddocs %}

# vw_sale_mydec_null_values

{% docs view_vw_sale_mydec_null_values %}
Test that deed, buyer, seller, address and price `sale.mydec` columns are not
null.

{% enddocs %}

# vw_iasworld_asmt_all_joined_to_legdat

{% docs view_vw_iasworld_asmt_all_joined_to_legdat %}
View that joins `iasworld.asmt_all` to `iasworld.legdat` to augment `asmt_all`
with parcel legal descriptions and addresses.

Both views are already filtered for current active records.
{% enddocs %}


# vw_report_town_close_neg_asmt_value

{% docs view_vw_report_town_close_neg_asmt_value %}
Check for `ASMT.val*` columns that are negative.

This view is exported as part of the QC report to check values prior to town
closings.

Pulled from the following Inquire queries:

- `FP Checklist - Negative ASMT Values` (#1569) - Will C.

In contrast to `qc.vw_neg_asmt_value`, this view directly performs filtering
for negative values.
{% enddocs %}

# vw_report_town_close_0_land_value

{% docs view_vw_report_town_close_0_land_value %}
Check for parcels that have 0 land value in the `ASMT` table.

This view is exported as part of the QC report to check values prior to town
closings.

Pulled from the following Inquire queries:

- `FP Checklist - Non-EX, RR parcels with 0 land value` (#1578) - Will C.
{% enddocs %}

# vw_report_town_close_0_value

{% docs view_vw_report_town_close_0_value %}
Check for parcels that have 0 total value in the `ASMT` table.

This view is exported as part of the QC report to check values prior to town
closings.

Pulled from the following Inquire queries:

- `FP Checklist - Non-EX, RR parcels with 0 value` (#1570) - Will C.
{% enddocs %}

# vw_report_town_close_vacant_class_with_bldg_value

{% docs view_vw_report_town_close_vacant_class_with_bldg_value %}
Check for parcels that have a vacant class in the `PARDAT` table, but a
building value in the `ASMT` table.

This view is exported as part of the QC report to check values prior to town
closings.

Pulled from the following Inquire queries:

- `FP Checklist - Vacant Class, bldg value` (#1577) - Will C.
{% enddocs %}

# vw_report_town_close_improved_class_without_bldg_value

{% docs view_vw_report_town_close_improved_class_without_bldg_value %}
Check for parcels that have a non-vacant class in the `PARDAT` table, but no
building value in the `ASMT` table.

This view is exported as part of the QC report to check values prior to town
closings.

Pulled from the following Inquire queries:

- `FP Checklist - Improved Class, no bldg value` (#1047) - Will C.
{% enddocs %}

# vw_report_town_close_class_does_not_equal_luc

{% docs view_vw_report_town_close_class_does_not_equal_luc %}
Check for parcels where the class does not match the land use code in
the `PARDAT` table.

This view is exported as part of the QC report to check values prior to town
closings.

Pulled from the following Inquire queries:

- `FP Checklist - Class does not equal LUC` (#2046) - Will C.
{% enddocs %}

# vw_report_town_close_res_parcels_not_set_to_cost_approach

{% docs view_vw_report_town_close_res_parcels_not_set_to_cost_approach %}
Check for residential class parcels where the `revcode` column in the
`APRVAL` table is not 1.

This view is exported as part of the QC report to check values prior to town
closings.

Pulled from the following Inquire queries:

- `FP Checklist - Res parcels not set to Cost Approach` (#2114) - Will C.
{% enddocs %}

# vw_report_town_close_ovrrcnlds_to_review

{% docs view_vw_report_town_close_ovrrcnlds_to_review %}
Check for rows in the `DWELDAT`, `COMDAT`, and `OBY` tables
where the calculated net market value does not match the override net market
value.

This view is exported as part of the QC report to check values prior to town
closings.

Pulled from the following Inquire queries:

- `FP Checklist - OVRRCNLDs to review` (#2474) - Will C.
{% enddocs %}

# vw_report_town_close_bldg_parcel_class_mismatch

{% docs view_vw_report_town_close_bldg_parcel_class_mismatch %}
Check for rows in the `DWELDAT`, `COMDAT`, and `OBY` tables
where the class does not match the parcel's class in the `PARDAT` table.

This view is exported as part of the QC report to check values prior to town
closings.

Pulled from the following Inquire queries:

- `FP Checklist - Bldg and parcel class mismatch` (#2161) - Will C.
{% enddocs %}

# vw_report_town_close_289s

{% docs view_vw_report_town_close_289s %}
Check for rows in the  `OBY` table where the class is 289.

This view is exported as part of the QC report to check values prior to town
closings.

Pulled from the following Inquire queries:

- `FP Checklist - 289s` (#1963) - Will C.
{% enddocs %}

# vw_report_town_close_500k_increase_1m_decrease

{% docs view_vw_report_town_close_500k_increase_1m_decrease %}
Check for rows in the  `ASMT` table where an assessed value has increased
by more than $500k or decreased by more than $1m in the past year.

This view is exported as part of the QC report to check values prior to town
closings.

Pulled from the following Inquire queries:

- `FP Checklist - 500k increase, 1m decrease` (#1673) - Will C.
- `FP Checklist - Card Code Info` (#2160) - Will C.
{% enddocs %}

# vw_report_town_close_res_multicodes

{% docs view_vw_report_town_close_res_multicodes %}
Check market and assessed values for parcels with multiple cards.

This view is exported as part of the QC report to check values prior to town
closings.

Pulled from the following Inquire queries:

- `FP Checklist - Res multicode PIN list` (#1591) - Will C.
- `FP Checklist - Res multicode report with sales` (#1659) - Will C.
{% enddocs %}

# vw_report_town_close_card_code_5s_not_at_100_comdat

{% docs view_vw_report_town_close_card_code_5s_not_at_100_comdat %}
Check market and assessed values for parcels that had a card code 5 in the
prior year and whose occupancy is not at 100% in the `COMDAT` table.

This view is exported as part of the QC report to check values prior to town
closings.

Pulled from the following Inquire queries:

- `FP Checklist - Prior Yr Card Code 5s COMDAT` (#1585) - Will C.
{% enddocs %}
