# class_dict

{% docs seed_class_dict %}
Table containing a translation for property class codes to human-readable class
descriptions. Also describes which classes are included in residential
regressions and reporting classes.

**Primary Key**: `class_code`
{% enddocs %}

# pin_test

{% docs seed_pin_test %}
Table containing a set of PINs used for various pipeline and valuation tests.
Mainly includes PINs with unusual situations, such as those with multiple cards
(dwellings) _and_ an associated tieback (proration). The Data Department uses
this as a repository of PINs for testing views, models, and applications.

The possible values for `test_type` include:

- `class_change`
- `incorrect_char`
- `just_weird`
- `multi_card`
- `multi_card_prorated`
- `omitted_assessment`
- `prorated`
- `split_class`

**Primary Key**: `year`, `pin`
{% enddocs %}
