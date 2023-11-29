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

# view_vw_iasworld_sales_null_values

{% docs view_vw_iasworld_sales_null_values %}
Test that deed, buyer, seller, and price `ias.sale*` columns are not null.

{% enddocs %}

# view_vw_sale_mydec_null_values

{% docs view_vw_sale_mydec_null_values %}
Test that deed, buyer, seller, address and price `sale.mydec*` columns are not null.

{% enddocs %}