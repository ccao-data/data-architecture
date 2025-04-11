# agency

{% docs table_agency %}
Cleaned up version of the Cook County Clerk's
[Agency Tax Rate reports](https://www.cookcountyclerkil.gov/property-taxes/tax-extension-and-rates).

Contains the total extension, levy, and base EAV for each taxing district in
Cook County by year.

Used within the [PTAXSIM](https://github.com/ccao-data/ptaxsim) package and database.

**Primary Key**: `year`, `agency_num`
{% enddocs %}

# agency_fund

{% docs table_agency_fund %}
Table of funds and line-items that contribute for each taxing
district’s extension.

Pulled from the second sheet of the Cook County Clerk's
[Agency Tax Rate reports](https://www.cookcountyclerkil.gov/property-taxes/tax-extension-and-rates).

Used within the [PTAXSIM](https://github.com/ccao-data/ptaxsim) package and database.

**Primary Key**: `year`, `agency_num`, `fund_num`
{% enddocs %}

# agency_fund_info

{% docs table_agency_fund_info %}
Taxing district fund names and whether the fund is statutorily capped.

Used within the [PTAXSIM](https://github.com/ccao-data/ptaxsim) package and database.

**Primary Key**: `fund_num`
{% enddocs %}

# agency_info

{% docs table_agency_info %}
Taxing district name, type, and subtype, roughly as seen on tax bills.

Used within the [PTAXSIM](https://github.com/ccao-data/ptaxsim) package and database.

**Primary Key**: `agency_num`
{% enddocs %}

# cpi

{% docs table_cpi %}
Consumer Price Index (CPI-U) used for PTELL limit calculations.

Collected from the Illinois Department of Revenue.

Used within the [PTAXSIM](https://github.com/ccao-data/ptaxsim) package and database.

**Primary Key**: `levy_year`
{% enddocs %}

# eq_factor

{% docs table_eq_factor %}
Equalization factor from IDOR, applied to AV to get EAV.

Collected from the Illinois Department of Revenue.

Used within the [PTAXSIM](https://github.com/ccao-data/ptaxsim) package and database.

**Primary Key**: `year`
{% enddocs %}

# pin

{% docs table_pin %}
PIN-level tax code, assessed value, and exemptions.

Used within the [PTAXSIM](https://github.com/ccao-data/ptaxsim) package and database.

**Primary Key**: `pin`, `year`
{% enddocs %}

# pin_geometry_raw

{% docs table_pin_geometry_raw %}
PIN geometries (boundaries) for all PINs within PTAXSIM.

Saved using only the geometry deltas (saved only when PINs change) to reduce
table size. Use the corresponding view in PTAXSIM `pin_geometry` to expand
this table to be per year, per PIN.

Used within the [PTAXSIM](https://github.com/ccao-data/ptaxsim) package and database.

**Primary Key**: `pin`, `year`
{% enddocs %}

# tax_code

{% docs table_tax_code %}
Crosswalk of tax codes by taxing agency/district.

Used within the [PTAXSIM](https://github.com/ccao-data/ptaxsim) package and database.

**Primary Key**: `year`, `agency_num`, `tax_code_num`
{% enddocs %}

# tif

{% docs table_tif %}
TIF revenue, start year, and cancellation year.

Used within the [PTAXSIM](https://github.com/ccao-data/ptaxsim) package and database.

**Primary Key**: `year`, `agency_num`
{% enddocs %}

# tif_crosswalk

{% docs table_tif_crosswalk %}
Hotfix table to crosswalk incorrect or deprecated TIF agency numbers to
corrected variants.

Used within the [PTAXSIM](https://github.com/ccao-data/ptaxsim) package and database.

**Primary Key**: `year`, `agency_num_dist`
{% enddocs %}

# tif_distribution

{% docs table_tif_distribution %}
TIF EAV, frozen EAV, and distribution percentage by tax code.

Used within the [PTAXSIM](https://github.com/ccao-data/ptaxsim) package and database.

**Primary Key**: `year`, `agency_num`, `tax_code_num`
{% enddocs %}
