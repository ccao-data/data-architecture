# cc_dli_senfrr

{% docs table_cc_dli_senfrr %}
Legacy senior freeze exemption data pulled by BoT. Provided via MV 08/18/2024.

**Primary Key**: `pin`, `year`
{% enddocs %}

# cc_pifdb_piexemptre_dise

{% docs table_cc_pifdb_piexemptre_dise %}
Legacy homestead (senior) exemption data pulled by BoT. Provided via
MV 08/18/2024.

**Primary Key**: `pin`, `year`
{% enddocs %}

# cc_pifdb_piexemptre_ownr

{% docs table_cc_pifdb_piexemptre_ownr %}
Legacy homestead exemption data pulled by BoT. Provided via MV 10/01/2024.

**Primary Key**: `pin`, `year`
{% enddocs %}

# cc_pifdb_piexemptre_sted

{% docs table_cc_pifdb_piexemptre_sted %}
Legacy disabled persons and veterans exemption data pulled by BoT. Provided via
MV 08/18/2024.

**Primary Key**: `pin`, `year`
{% enddocs %}

# commercial_valuation

{% docs table_commercial_valuation %}
CCAO commercial valuation data, aggregated from the commercial team spreadsheets
[available on the Assessor's site](https://www.cookcountyassessor.com/valuation-reports).

**Primary Key**: `keypin`, `year`
{% enddocs %}

# hie

{% docs table_hie %}
Legacy Home Improvement Exemption (HIE) data pulled from the AS/400.

This table exists in order to apply HIE characteristics to sales for modeling,
but it will naturally become deprecated as the remaining legacy HOEs expire.

**Primary Key**: `pin`, `year`
{% enddocs %}

# land_nbhd_rate

{% docs table_land_nbhd_rate %}
Neighborhood-level land rates provided by Valuations.

These rates are applied during the modeling process in order to disaggregate
the value of land from the total PIN value. They are provided yearly prior
to modeling.

**Primary Key**: `town_nbhd`, `year`
{% enddocs %}

# land_site_rate

{% docs table_land_site_rate %}
PIN-level land value provided by Valuations.

These PIN-level flat land values were used in the 2021 reassessment year.
They applied specifically to townhome complexes. They are deprecated as of 2022.

**Primary Key**: `pin`, `year`
{% enddocs %}

# pin_condo_char

{% docs table_pin_condo_char %}
Condominium characteristic data collected by Valuations / Data Integrity at
the unit level (14-digit PIN).

Collected yearly prior to each triennial modeling/reassessment cycle. Sourced
from spreadsheets provided by Valuations for each township.

**Primary Key**: `pin`, `year`
{% enddocs %}

# pin_nonlivable

{% docs table_pin_nonlivable %}
Flags for non-livable condominium units such as common areas, parking spaces,
and storage areas.

Collected yearly from Valuations via spreadsheets.

**Primary Key**: `pin`, `year`
{% enddocs %}

# vw_time_util

{% docs view_vw_time_util %}
View that supplies dynamic datetime values in a way that we can mock for unit
tests.

See: <https://discourse.getdbt.com/t/dynamic-dates-in-unit-tests/16883/2?>
{% enddocs %}
