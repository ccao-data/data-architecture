# Possible unit tests

## Big-picture Qs

* Row count tests?
* Unique combination of column tests?
  * These seem reasonable and are generally not failing right now
* When and how to run data tests?
  * Proposal: Reduce/eliminate existing errors, then start testing every day
    (or every week?)

## Test run results

### Failing/warning

* `default_vw_pin_address_no_extra_whitespace`: 40338
  * This seems like a good test, but also like we're not actually stripping all
    of the whitespace we expect
* `default_vw_pin_appeal_change_matches_appeal_outcome`: 32
  * Seems like this is a problem in the underlying data, right? Perhaps we
    should either move it there, and/or make a unit test
* `default_vw_pin_sale_reasonable_number_of_sales_per_year`: 3997
  * I think we should probably move these to a QC report and ask Ray about it
* `default_vw_pin_universe_class_count_is_consistent_by_year`: 25
  * Seems clear that this is not true... do we want a wider range
    of allowed counts (e.g. 100-200) to catch obvious errors?

Some we should move to iasWorld data tests:

* `default_vw_pin_value_board_class_not_null`: 1260
* `default_vw_pin_value_board_tot_mv_not_null`: 1260
* `default_vw_pin_value_certified_tot_mv_not_null`: 15
* `default_vw_pin_value_certified_tot_not_null`: 15
* `default_vw_pin_value_board_tot_not_null`: 1260
* `default_vw_pin_value_certified_class_not_null`: 15
* `default_vw_pin_value_mailed_tot_mv_not_null`: 310
* `default_vw_pin_value_mailed_class_not_null`: 289

## `default.vw_pin_address`

### Definite

* `default_vw_pin_address_no_extra_whitespace`
* `default_vw_pin_address_numeric_pin`

## `default.vw_pin_appeal`

### Possible

* `default_vw_pin_appeal_change_matches_appeal_outcome`
  * Should we test source table?

## `default.vw_pin_condo_char`

### Possible

* `default_vw_pin_condo_char_unique_by_14_digit_pin_and_year`
  * Why does this have 70k errors?
* `default_vw_pin_condo_char_card_not_null`
  * Seems like we just pull `card` directly from source data, do we need to test
    it?

## `default.vw_pin_exempt`

### Definite

* `default_vw_pin_exempt_class_no_hyphens`
* `default_vw_pin_exempt_numeric_pin`

## `default.vw_pin_history`

## `default.vw_pin_sale`

### Definite

* `default_vw_pin_sale_class_no_hyphens`
* `default_vw_pin_sale_sale_filter_deed_type`
* `default_vw_pin_sale_sale_filter_less_than_10k`

## `default.vw_pin_universe`

### Definite

* `default_vw_pin_universe_numeric_pin`
* `default_vw_pin_universe_class_no_hyphens`

## `default.vw_pin_value`

### Definite

* `default_vw_pin_value_mailed_class_no_hyphens`

### Possible

* All not-null tests
  * We could maybe move these to `iasworld.asmt_all`?

## `reporting`

### Definite

* All "no hyphens" tests
