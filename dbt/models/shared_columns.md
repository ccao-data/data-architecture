# Cook County

## pin

{% docs column_pin %}
Full Property Index Number.

The index number is a brief legal description of a particular parcel by
numerical reference to parcels on assessment maps. It is also the primary unit
of taxable value in Cook County.

All PINs are 14 digits: 2 digits for area + 2 digits for sub area + 2 digits
for block + 2 digits for parcel + 4 digits for the condominium unit/leasehold.
{% enddocs %}

## pin10

{% docs column_pin10 %}
First 10 digits of a PIN.

Useful for identifying individual condominium buildings since the last 4
digits of a PIN identifies individual units.
{% enddocs %}

## card

{% docs column_card %}
Sub-unit of a PIN.

For residential properties, cards usually identify each *building*, For
commercial properties, they can identify spaces within the same building.
Cards also serve as the unit of observation for the residential model.

Equivalent to legacy `MLT_CD` (multicode) value.
{% enddocs %}

## cdu

{% docs column_cdu %}
Condition - Desirability - Utility code.

Code representing a any number of seemingly unrelated characteristics
associated with a PIN, ranging from condition to types of subsidies, to
whether or not a PIN is a garage. The full list of CDU codes can be found on
the Assessor's website.
{% enddocs %}

## year

{% docs column_year %}
Tax year.

Tax years are the "working" or current year for which assessments and levies
are calculated. Tax bills are paid in arrears, so an assessment from TY2023
will be paid in calendar year 2024.

{% enddocs %}

## class

{% docs column_class %}
Property type and/or use.

Designates the property type, such as vacant, residential, multi-family,
agricultural, commercial or industrial. The classification determines the
percentage of fair cash value at which a property is assessed for taxing
purposes. See `ccao.class_dict` for more information.
{% enddocs %}

## triad_name

{% docs column_triad_name %}
Cook County triad name.

Triads are the "unit of yearly work" for the Cook County property system:
one triad is reassessed every 3 years on a rotating cycle. Each triad is made
up of townships. The possible triad names are: `North`, `South`,
or `City` (Chicago).
{% enddocs %}

## triad_code

{% docs column_triad_code %}
Cook County triad code.

Triads are the "unit of yearly work" for the Cook County property system:
one triad is reassessed every 3 years on a rotating cycle. Each triad is made
up of townships. The possible triad codes are:

- `1` (City)
- `2` (North)
- `3` (South)
{% enddocs %}

## township_name

{% docs column_township_name %}
Cook County township name.

The county is divided into 38 geographic townships. These townships act as
units of work and analysis for the Assessor's Office. Township boundaries are
coincident with triads (triads are made up of townships). Note that townships
can also be units of local government, with their own boards and taxing
authority. Townships in the City of Chicago are effectively vestigial.
{% enddocs %}

## township_code

{% docs column_township_code %}
Cook County township code.

See `township_name` for more information. Note that township codes that start
with 7 are City triad townships.
{% enddocs %}

## nbhd_code

{% docs column_nbhd_code %}
Assessor neighborhood code.

First 2 digits are township code, last 3 digits are neighborhood code.
Neighborhood boundaries are coincident with townships.

Geographic neighborhoods intended to represent relatively homogeneous
housing sub-markets. They were created a long time ago for internal use by the
various property tax offices. The Assessor now uses them as units of work and
analysis. For example, land rates are usually delimited by neighborhood.
{% enddocs %}

## tax_code

{% docs column_tax_code %}
Property tax code.

Identifies the unique combination of taxing districts which impose a levy
on any given property.
{% enddocs %}

# Sales

## sale_date

## document_number

# Spatial

- x
- y
- lon
- lat
- geometry
- geometry_3435

# iasWorld
