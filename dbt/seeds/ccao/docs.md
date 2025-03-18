# adjective

{% docs seed_adjective %}
Table containing adjectives. These are combined with the person names in
`ccao.person` to generate unique IDs for models and jobs. Adjectives originally
sourced from Docker.

**Primary Key**: `adjective`
{% enddocs %}

# aprval_reascd

{% docs seed_aprval_reascd %}
Table containing descriptions for reason codes from `iasworld.aprval.reascd`.
Reason codes pertain to changes in AV.

**Primary Key**: `reascd`
{% enddocs %}

# cdu

{% docs seed_cdu %}
Table containing CDU (Condition, Desirability, and Utility) codes and
descriptions. CDUs come from the `user16` columns in `comdat`, `dweldat`, and `oby`
tables in `iasworld`.

**Primary Key**: `cdu_code`
{% enddocs %}

# class_dict

{% docs seed_class_dict %}
Table containing a translation for property class codes to human-readable class
descriptions. Also describes which classes are included in residential
regressions and reporting classes.

Derived from the 2023
[PDF](https://prodassets.cookcountyassessor.com/s3fs-public/form_documents/Definitions%20for%20Classifications_2023.pdf).

To find the level of assessment (LoA) for each class, see the `ccao.loa` table.

**Primary Key**: `class_code`
{% enddocs %}

# commercial_major_subclass

{% docs seed_commercial_major_subclass %}
Major subclass for commercial buildings, e.g. `Hotels`, `Retail`, etc.

**Primary Key**: `code`
{% enddocs %}

# commercial_minor_subclass

{% docs seed_commercial_minor_subclass %}
Minor subclass for commercial buildings, e.g. `HOTELS-CONVENTION CENTER`,
`RETAIL-BANKS`, etc.

**Primary Key**: `code`
{% enddocs %}

# htpar_reascd

{% docs seed_htpar_reascd %}
Table containing descriptions for appeal decision reason codes from
`iasworld.htpar`. These codes are sourced directly from the iasWorld interface.
Many (but not all) are documented on the [Assessor's website](https://www.cookcountyassessor.com/form-document/assessor-reason-codes).

**Primary Key**: `reascd`
{% enddocs %}

# infl_reascd

{% docs seed_infl_reascd %}
Table containing descriptions for reason codes from `iasworld.land.infl1`
and `iasworld.land.infl2`. Reason codes pertain to influence factors.

**Primary Key**: `reascd`
{% enddocs %}

# loa

{% docs seed_loa %}
Table containing the Level of Assessment (LoA) for each minor property class
for each year. LoAs change over time due to legislation, changes to Assessor
class codes, etc.

Do *not* use this table as a definitive list of existent class codes per year.
Please use `ccao.class_dict` instead.

**Primary Key**: `year`, `class_code`
{% enddocs %}

# person

{% docs seed_person %}
Table containing the names of current and former CCAO Data Department interns,
employees, and fellows. Used for unique ID generators for models and jobs.

**Primary Key**: `person`
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
