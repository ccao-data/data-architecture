# Characteristics

## char_age

{% docs shared_column_char_age %}
Deprecated.

Use year built instead
{% enddocs %}

## char_apts

{% docs shared_column_char_apts %}
Number of apartments for class 211 and 212 properties.

CAUTION: Note the numerically encoded values DO NOT correspond to the number of apartments i.e. code 1 means 2 apartments, code 6 means 0 apartments
{% enddocs %}

## char_beds

{% docs shared_column_char_beds %}
Number of bedrooms in the building.
{% enddocs %}

## char_bldg_sf

{% docs shared_column_char_bldg_sf %}
Square footage of the building, as measured from the exterior.
{% enddocs %}

## char_cnst_qlty

{% docs shared_column_char_cnst_qlty %}
Construction quality.

In general, this field is not used consistently and is therefore not useful for analytical purposes
{% enddocs %}

## char_fbath

{% docs shared_column_char_fbath %}
Number of full bathrooms.

Defined as bathrooms with a bath or shower. If this value is missing, the default value is set to 1
{% enddocs %}

## char_frpl

{% docs shared_column_char_frpl %}
Number of fireplaces.

Counted as the number of flues one can see from the outside of the building
{% enddocs %}

## char_gar1_area

{% docs shared_column_char_gar1_area %}
Indicator for garage area inclusion.

Is Garage 1 physically included within the building area? If yes, the garage area is subtracted from the building square feet calculation by the field agent
{% enddocs %}

## char_hbath

{% docs shared_column_char_hbath %}
Number of half baths.

Defined as bathrooms without a shower or bathtub
{% enddocs %}

## char_land_sf

{% docs shared_column_char_land_sf %}
Square footage of the land (not just the building) of the property.

A single PIN can have multiple "land lines," meaning it can be associated with more than one 200-class land lot
{% enddocs %}

## char_ncu

{% docs shared_column_char_ncu %}
Number of commercial units.

The vast majority are for properties with class 212
{% enddocs %}

## char_recent_renovation

{% docs shared_column_char_recent_renovation %}
Indicates whether or not a property was renovated within the last 3 years.

Renovation is indicated by the `char_renovation` characteristic flipping from "NO" to "YES"
{% enddocs %}

## char_repair_cnd

{% docs shared_column_char_repair_cnd %}
Deprecated.

This field is subjective and contains little variation. As such, it is not used for modeling or other purposes
{% enddocs %}

## char_rooms

{% docs shared_column_char_rooms %}
Number of total rooms in the building (excluding baths).

Not to be confused with bedrooms
{% enddocs %}

## char_site

{% docs shared_column_char_site %}
Deprecated.

This field lacks sufficient variation to be useful for modeling
{% enddocs %}

## char_tp_dsgn

{% docs shared_column_char_tp_dsgn %}
Deprecated.

Field has not been updated recently enough to be useful for modeling
{% enddocs %}

# Cook County

## card

{% docs shared_column_card %}
Sub-unit of a PIN.

For residential properties, cards usually identify each *building*, For
commercial properties, they can identify spaces within the same building.
Cards also serve as the unit of observation for the residential model.

Equivalent to legacy `MLT_CD` (multicode) value
{% enddocs %}

## card_proration_rate

{% docs shared_column_card_proration_rate %}
Proration rate for a card on a given PIN.

Prorated cards split their total value across multiple PINs. An example is
something like a building that crosses multiple PINs
{% enddocs %}

## cdu

{% docs shared_column_cdu %}
Condition/Desirability/Utility code.

Code representing a any number of seemingly unrelated characteristics
associated with a PIN, ranging from condition to types of subsidies, to
whether or not a PIN is a garage. The full list of CDU codes can be found on
the Assessor's website
{% enddocs %}

## class

{% docs shared_column_class %}
Property type and/or use.

Designates the property type, such as vacant, residential, multi-family,
agricultural, commercial or industrial. The classification determines the
percentage of fair cash value at which a property is assessed for taxing
purposes. See `ccao.class_dict` for more information
{% enddocs %}

## nbhd_code

{% docs shared_column_nbhd_code %}
Assessor neighborhood code.

First 2 digits are township code, last 3 digits are neighborhood code.
Neighborhood boundaries are coincident with townships.

Geographic neighborhoods intended to represent relatively homogeneous
housing sub-markets. They were created a long time ago for internal use by the
various property tax offices. The Assessor now uses them as units of work and
analysis. For example, land rates are usually delimited by neighborhood
{% enddocs %}

## pin

{% docs shared_column_pin %}
Full Property Index Number.

The index number is a brief legal description of a particular parcel by
numerical reference to parcels on assessment maps. It is also the primary unit
of taxable value in Cook County.

All PINs are 14 digits: 2 digits for area + 2 digits for sub area + 2 digits
for block + 2 digits for parcel + 4 digits for the condominium unit/leasehold
{% enddocs %}

## pin_is_multiland

{% docs shared_column_pin_is_multiland %}
Indicates the PIN has more than one landline.
{% enddocs %}

## pin_is_multicard

{% docs shared_column_pin_is_multicard %}
Indicates a PIN with more than one building (ADU, coach house, etc.).
{% enddocs %}

## pin10

{% docs shared_column_pin10 %}
First 10 digits of a PIN.

Useful for identifying individual condominium buildings since the last 4
digits of a PIN identifies individual units
{% enddocs %}

## tax_code

{% docs shared_column_tax_code %}
Property tax code.

Identifies the unique combination of taxing districts which impose a levy
on any given property
{% enddocs %}

## township_code

{% docs shared_column_township_code %}
Cook County township code.

See `township_name` for more information. Note that township codes that start
with 7 are City triad townships
{% enddocs %}

## township_name

{% docs shared_column_township_name %}
Cook County township name.

The county is divided into 38 geographic townships. These townships act as
units of work and analysis for the Assessor's Office. Township boundaries are
coincident with triads (triads are made up of townships). Note that townships
can also be units of local government, with their own boards and taxing
authority. Townships in the City of Chicago are effectively vestigial
{% enddocs %}

## triad_code

{% docs shared_column_triad_code %}
Cook County triad code.

Triads are the "unit of yearly work" for the Cook County property system:
one triad is reassessed every 3 years on a rotating cycle. Each triad is made
up of townships. The possible triad codes are:

- `1` (City)
- `2` (North)
- `3` (South)
{% enddocs %}

## triad_name

{% docs shared_column_triad_name %}
Cook County triad name.

Triads are the "unit of yearly work" for the Cook County property system:
one triad is reassessed every 3 years on a rotating cycle. Each triad is made
up of townships. The possible triad names are: `North`, `South`,
or `City` (Chicago)
{% enddocs %}

## year

{% docs shared_column_year %}
Tax year.

Tax years are the "working" or current year for which assessments and levies
are calculated. Tax bills are paid in arrears, so an assessment from TY2023
will be paid in calendar year 2024
{% enddocs %}

# Sales

## document_number

## sale_date

# Spatial

## geometry

{% docs shared_column_geometry %}
Well-Known Binary (WKB) geometry (EPSG 4326).

Represents a point, polygon, or linestring associated with this observation
{% enddocs %}

## geometry_3435

{% docs shared_column_geometry_3435 %}
Well-Known Binary (WKB) geometry (EPSG 3435).

Represents a point, polygon, or linestring associated with this observation
{% enddocs %}

## latitude

{% docs shared_column_latitude %}
Y coordinate in degrees (global latitude).

Point location derived from the centroid of the largest polygon associated
with the geometry. Units are degrees, taken from the WGS84 projection
(EPSG 4326)
{% enddocs %}

## longitude

{% docs shared_column_longitude %}
X coordinate in degrees (global longitude).

Point location derived from the centroid of the largest polygon associated
with the geometry. Units are degrees, taken from the WGS84 projection
(EPSG 4326)
{% enddocs %}

## x_3435

{% docs shared_column_x_3435 %}
X coordinate in feet.

Point location derived from the centroid of the largest polygon associated
with the geometry. Units are feet, taken from the NAD83 / Illinois East
projection (EPSG 3435)
{% enddocs %}

## y_3435

{% docs shared_column_y_3435 %}
Y coordinate in feet.

Point location derived from the centroid of the largest polygon associated
with the geometry. Units are feet, taken from the NAD83 / Illinois East
projection (EPSG 3435)
{% enddocs %}
