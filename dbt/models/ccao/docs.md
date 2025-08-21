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

### Nuance

- The table is _not_ unique by its intended primary keys and should not be
aggregated or used for analyses, only provided in raw.

**Primary Key**: `keypin`, `year`, `class(es)`, `sheet`
{% enddocs %}

# dtbl_modelvals

{% docs table_dtbl_modelvals %}
Archived model values produced by the [legacy Data Department AVM](https://gitlab.com/ccao-data-science---modeling/ccao_sf_cama_dev). Archived documentation can be found [here](https://gitlab.com/ccao-data-science---modeling/ccao_sf_cama_dev/-/blob/master/data_dictionary_constituents/DTBL_MODELVALS.csv).

These model values were produced and recorded using a model and architecture
that are incompatible with the department's current AVM and `model` database.

### Nuance

- The table is _not_ unique by its intended primary keys and should not be
aggregated or used for analyses, only provided in raw.

**Primary Key**: `pin`, `tax_year`, `version`
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

**Primary Key**: `town_nbhd`, `year`, `class`
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

# zoning

{% docs table_zoning %}
CCAO GIS compiles zoning on the pin level. All years are coded as
2025. This is because data is pulled asynchronously from different sources. At the moment,
the measn that we do not include this feature in modeling or default views.

There are known issues where pins have multiple zoning codes due to fuzzy geo-spatial techniques.
These are concatenated them with ','.

GIS is currently updating data for the South Tri. These townships will be
uploaded upon completion.

Raw data is stored in 
`O:\CCAODATA\zoning\data`

**Primary Key**: `pin`
{% enddocs %}
